//! A sqlite based block store for content-addressed data that tries to do as much as possible
//! in the database.
//!
//! This module is for all interactions with the database, so all SQL statements go in here.
//!
//! Tables:
//! cids: mapping from cid (blob < 64 bytes) to id (u64)
//! refs: m:n mapping from block ids to their children
//! blocks: the actual data for blocks, keyed by block id
//!    cids can exist in the system without having data associated with them!
//! alias: table that contains named pins for roots of graphs that should not be deleted by gc
//!    you can alias incomplete or in fact non-existing data. It is not necessary for a pinned dag
//!    to be complete.
use libipld::{Cid, DefaultParams};
use rusqlite::{
    config::DbConfig, params, types::FromSql, Connection, OptionalExtension, ToSql, Transaction,
};
use std::{collections::BTreeSet, convert::TryFrom, time::Duration, time::Instant};
use tracing::*;

use crate::{
    cache::{BlockInfo, CacheTracker},
    cidbytes::CidBytes,
    SizeTargets, StoreStats, Synchronous,
};

const PRAGMAS: &str = r#"
-- this must be done before changing the database via the CLI!
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
-- I tried different even larger values for this. Did not make a difference.
PRAGMA page_size = 4096;
"#;

const INIT: &str = r#"
PRAGMA user_version = 1;

CREATE TABLE IF NOT EXISTS cids (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cid BLOB UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS refs (
    parent_id INTEGER NOT NULL,
    child_id INTEGER NOT NULL,
    PRIMARY KEY(parent_id,child_id)
    CONSTRAINT fk_parent_id
      FOREIGN KEY (parent_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
    CONSTRAINT fk_child_id
      FOREIGN KEY (child_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_refs_child_id
ON refs (child_id);

CREATE TABLE IF NOT EXISTS blocks (
    block_id INTEGER PRIMARY KEY,
    block BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS aliases (
    name blob NOT NULL PRIMARY KEY,
    block_id INTEGER NOT NULL,
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aliases_block_id
ON aliases (block_id);

CREATE TABLE IF NOT EXISTS temp_pins (
    id INTEGER NOT NULL,
    block_id INTEGER NOT NULL,
    PRIMARY KEY(id,block_id)
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_temp_pins_block_id
ON temp_pins (block_id);

-- delete temp aliases that were not dropped because of crash
DELETE FROM temp_pins;

-- stats table to keep track of total number and size of blocks
CREATE TABLE IF NOT EXISTS stats (
    count INTEGER NOT NULL,
    size INTEGER NOT NULL
);

-- initialize stats from the real values at startup
DELETE FROM stats;
INSERT INTO stats (count, size) VALUES (
    (SELECT COUNT(id) FROM cids, blocks WHERE id = block_id),
    (SELECT COALESCE(SUM(LENGTH(block)), 0) FROM cids, blocks WHERE id = block_id)
);
"#;

fn user_version(txn: &Transaction) -> rusqlite::Result<u32> {
    Ok(txn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .optional()?
        .unwrap_or_default())
}

fn table_exists(txn: &Transaction, table: &str) -> rusqlite::Result<bool> {
    let num: u32 = txn
        .prepare_cached("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1;")?
        .query_row([table], |row| row.get(0))?;
    Ok(num > 0)
}

fn migrate_v0_v1(txn: &Transaction) -> anyhow::Result<()> {
    info!("executing migration from v0 to v1");
    txn.execute_batch("ALTER TABLE blocks RENAME TO blocks_v0")?;
    // drop the old refs table, since the content can be extracted from blocks_v0
    txn.execute_batch("DROP TABLE IF EXISTS refs;")?;
    txn.execute_batch(INIT)?;
    let num_blocks: i64 = txn.query_row("SELECT COUNT(*) FROM blocks_v0", [], |r| r.get(0))?;
    let mut stmt = txn.prepare("SELECT * FROM blocks_v0")?;
    let block_iter = stmt.query_map([], |row| {
        Ok((row.get::<_, Vec<u8>>(2)?, row.get::<_, Vec<u8>>(3)?))
    })?;
    for (i, block) in block_iter.enumerate() {
        if num_blocks != 0 && i % 1000 == 0 {
            info!(
                "converting to new blocks, block {} of {} ({}%)",
                i,
                num_blocks,
                100 * i / (num_blocks as usize)
            );
        }
        let (cid, data) = block?;
        let cid = Cid::try_from(cid)?;
        let block = libipld::Block::<DefaultParams>::new(cid, data)?;
        let mut set = BTreeSet::new();
        block.references(&mut set)?;
        let mut pin = None;
        put_block(
            &txn,
            &block.cid().to_bytes(),
            block.data(),
            set.into_iter()
                .map(|cid| cid.to_bytes())
                .collect::<Vec<_>>(),
            &mut pin,
        )?;
    }
    info!("dropping table blocks_v0");
    txn.execute_batch("DROP TABLE blocks_v0")?;
    drop(stmt);
    info!("migration from v0 to v1 done!");
    Ok(())
}

fn get_id(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<Option<i64>> {
    txn.prepare_cached("SELECT id FROM cids WHERE cid=?")?
        .query_row([cid], |row| row.get(0))
        .optional()
}

/// returns the number and size of blocks, excluding orphaned blocks, computed from scratch
pub(crate) fn compute_store_stats(txn: &Transaction) -> crate::Result<StoreStats> {
    let (count, size): (i64, i64) = txn.prepare(
"SELECT COUNT(id), COALESCE(SUM(LENGTH(block)), 0) FROM cids JOIN blocks ON id = block_id"
    )?
    .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(StoreStats {
        count: u64::try_from(count)?,
        size: u64::try_from(size)?,
    })
}

/// returns the number and size of blocks, excluding orphaned blocks, from the stats table
pub(crate) fn get_store_stats(txn: &Transaction) -> crate::Result<StoreStats> {
    let (count, size): (i64, i64) = txn
        .prepare_cached("SELECT count, size FROM stats LIMIT 1")?
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let result = StoreStats {
        count: u64::try_from(count)?,
        size: u64::try_from(size)?,
    };
    debug_assert_eq!(result, compute_store_stats(txn)?);
    Ok(result)
}

fn get_or_create_id(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<i64> {
    let id = get_id(&txn, cid.to_sql()?)?;
    Ok(if let Some(id) = id {
        id
    } else {
        txn.prepare_cached("INSERT INTO cids (cid) VALUES (?)")?
            .execute([cid])?;
        txn.last_insert_rowid()
    })
}

pub(crate) fn incremental_gc(
    txn: &Transaction,
    min_blocks: usize,
    max_duration: Duration,
    size_targets: SizeTargets,
    cache_tracker: &impl CacheTracker,
) -> crate::Result<(Vec<BlockInfo>, bool)> {
    // get the store stats from the stats table
    let mut stats = get_store_stats(txn)?;
    // if we don't exceed any of the size targets, there is nothing to do
    if !size_targets.exceeded(&stats) {
        return Ok((Vec::new(), true));
    }
    // find all ids that have neither a parent nor are aliased
    let mut id_query = txn.prepare_cached(
        r#"
WITH RECURSIVE
    descendant_of(id) AS
    (
        SELECT block_id FROM aliases UNION SELECT block_id FROM temp_pins
        UNION
        SELECT DISTINCT child_id FROM refs JOIN descendant_of ON descendant_of.id=refs.parent_id
    )
SELECT id FROM
    cids
WHERE
    id NOT IN descendant_of;
        "#,
    )?;
    // measure the time from the start.
    // min_blocks will ensure that we get some work done even if the id query takes too long
    let t0 = Instant::now();
    // log execution time of the non-interruptible query that computes the set of ids to delete
    let mut ids = log_execution_time("gc_id_query", Duration::from_secs(1), || {
        id_query
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<i64>>>()
    })?;
    // give the cache tracker the opportunity to sort the non-pinned ids by value
    cache_tracker.sort_ids(&mut ids);
    let mut block_size_stmt = txn.prepare_cached(
        "SELECT LENGTH(block), cid FROM cids INNER JOIN blocks ON id = block_id AND id = ?;",
    )?;
    let mut update_stats_stmt =
        txn.prepare_cached("UPDATE stats SET count = count - 1, size = size - ?")?;
    let mut delete_stmt = txn.prepare_cached("DELETE FROM cids WHERE id = ?")?;
    let mut n = 0;
    let mut deleted = Vec::new();
    for id in ids.iter() {
        if n >= min_blocks && t0.elapsed() > max_duration {
            break;
        }
        if !size_targets.exceeded(&stats) {
            break;
        }
        trace!("deleting id {}", id);
        let block_size: Option<(i64, CidBytes)> = block_size_stmt
            .query_row(&[id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;
        if let Some((block_size, cid)) = block_size {
            let cid = Cid::try_from(&cid)?;
            let len: usize = usize::try_from(block_size)?;
            update_stats_stmt.execute([block_size])?;
            stats.count -= 1;
            stats.size -= block_size as u64;
            deleted.push(BlockInfo::new(*id, &cid, len));
        }
        delete_stmt.execute(&[id])?;
        n += 1;
    }
    let complete = n == ids.len() || !size_targets.exceeded(&stats);
    Ok((deleted, complete))
}

/// deletes the orphaned blocks.
///
/// orphaned blocks are blocks from the blocks table that do not have a corresponding id in the
/// cid table and in the other metadata table. They are unreachable.
///
/// The reason for deleting them incrementally is that deleting blocks can be an expensive operation
/// in sqlite, and we want to minimize gc related interruptions.
///
/// note that the execution time limit is not entirely accurate, because in many cases the cost of
/// deleting blocks will only be fully felt when doing the commit of the transaction.
pub(crate) fn incremental_delete_orphaned(
    txn: &Transaction,
    min_blocks: usize,
    max_duration: Duration,
) -> rusqlite::Result<bool> {
    let t0 = Instant::now();
    let ids: Vec<i64> = log_execution_time("determine_orphaned", Duration::from_secs(1), || {
        txn.prepare_cached(
            "SELECT block_id FROM blocks WHERE block_id NOT IN (SELECT id FROM cids)",
        )?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()
    })?;
    let mut delete_stmt = txn.prepare_cached("DELETE FROM blocks WHERE block_id = ?")?;
    let mut n = 0;
    for id in ids.iter() {
        let dt = t0.elapsed();
        if n >= min_blocks && dt > max_duration {
            info!(
                "stopped incremental delete after {}us and {} blocks",
                dt.as_micros(),
                n
            );
            break;
        }
        trace!("deleting block for id {}", id);
        delete_stmt.execute([id])?;
        n += 1;
    }
    Ok(n == ids.len())
}

pub(crate) fn delete_temp_pin(txn: &Transaction, pin: i64) -> rusqlite::Result<()> {
    txn.prepare_cached("DELETE FROM temp_pins WHERE id = ?")?
        .execute([pin])?;
    Ok(())
}

pub(crate) fn extend_temp_pin(
    txn: &Transaction,
    mut pin: i64,
    links: Vec<impl ToSql>,
) -> crate::Result<i64> {
    // we can reuse the id
    for link in links {
        let id = get_or_create_id(txn, link)?;
        add_temp_pin(txn, id, &mut pin)?;
    }
    Ok(pin)
}

pub(crate) struct PutBlockResult {
    /// id for the cid
    pub(crate) id: i64,
    /// true if the block already existed
    pub(crate) block_exists: bool,
}

pub(crate) fn add_temp_pin(txn: &Transaction, id: i64, pin_id: &mut i64) -> crate::Result<()> {
    if *pin_id > 0 {
        txn.prepare_cached("INSERT OR IGNORE INTO temp_pins (id, block_id) VALUES (?, ?)")?
            .execute([*pin_id, id])?;
    } else {
        // since we are not using an autoincrement column, this will reuse ids.
        // I think this is safe, but is it really? deserves some thought.
        *pin_id = txn
            .prepare_cached("SELECT COALESCE(MAX(id), 1) + 1 FROM temp_pins")?
            .query_row([], |row| row.get(0))?;
        txn.prepare_cached("INSERT INTO temp_pins (id, block_id) VALUES (?, ?)")?
            .execute([*pin_id, id])?;
    }
    Ok(())
}

pub(crate) fn put_block<C: ToSql>(
    txn: &Transaction,
    key: &C,
    data: &[u8],
    links: impl IntoIterator<Item = C>,
    pin: &mut Option<i64>,
) -> crate::Result<PutBlockResult> {
    let id = get_or_create_id(&txn, &key)?;
    let block_exists = txn
        .prepare_cached("SELECT 1 FROM blocks WHERE block_id = ?")?
        .query_row([id], |_| Ok(()))
        .optional()?
        .is_some();
    // create a temporary alias for the block, even if it already exists
    if let Some(pin) = pin {
        add_temp_pin(txn, id, pin)?;
    }
    if !block_exists {
        // add the block itself
        txn.prepare_cached("INSERT INTO blocks (block_id, block) VALUES (?, ?)")?
            .execute(params![id, &data])?;

        // update the stats
        txn.prepare_cached("UPDATE stats SET count = count + 1, size = size + ?")?
            .execute([data.len() as i64])?;

        // insert the links
        let mut insert_ref =
            txn.prepare_cached("INSERT INTO refs (parent_id, child_id) VALUES (?,?)")?;
        for link in links {
            let child_id: i64 = get_or_create_id(&txn, link)?;
            insert_ref.execute([id, child_id])?;
        }
    }
    Ok(PutBlockResult { id, block_exists })
}

/// Get a block
pub(crate) fn get_block(
    txn: &Transaction,
    cid: impl ToSql,
) -> crate::Result<Option<(i64, Vec<u8>)>> {
    let id = get_id(&txn, cid)?;
    Ok(if let Some(id) = id {
        txn.prepare_cached("SELECT block FROM blocks WHERE block_id = ?")?
            .query_row([id], |row| row.get(0))
            .optional()?
            .map(|b| (id, b))
    } else {
        None
    })
}

/// Check if we have a block
pub(crate) fn has_block(txn: &Transaction, cid: impl ToSql) -> crate::Result<bool> {
    Ok(txn
        .prepare_cached(
            "SELECT 1 FROM blocks, cids WHERE blocks.block_id = cids.id AND cids.cid = ?",
        )?
        .query_row([cid], |_| Ok(()))
        .optional()?
        .is_some())
}

/// Check if we have a cid
pub(crate) fn has_cid(txn: &Transaction, cid: impl ToSql) -> crate::Result<bool> {
    Ok(txn
        .prepare_cached("SELECT 1 FROM cids WHERE cids.cid = ?")?
        .query_row([cid], |_| Ok(()))
        .optional()?
        .is_some())
}

/// get the descendants of a cid.
/// This just uses the refs table, so it does not ensure that we actually have data for each cid.
/// The value itself is included.
pub(crate) fn get_descendants<C: ToSql + FromSql>(
    txn: &Transaction,
    cid: C,
) -> crate::Result<Vec<C>> {
    let res = txn
        .prepare_cached(
            r#"
WITH RECURSIVE
    descendant_of(id) AS
    (
        SELECT id FROM cids WHERE cid = ?
        UNION ALL
        SELECT DISTINCT child_id FROM refs JOIN descendant_of ON descendant_of.id=refs.parent_id
    ),
    descendant_ids as (
        SELECT DISTINCT id FROM descendant_of
    )
    -- retrieve corresponding cids - this is a set because of select distinct
    SELECT cid from cids JOIN descendant_ids ON cids.id = descendant_ids.id;
"#,
        )?
        .query_map([cid], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<C>>>()?;
    Ok(res)
}

/// get the set of descendants of an id for which we do not have the data yet.
/// The value itself is included.
/// It is safe to call this method for a cid we don't have yet.
pub(crate) fn get_missing_blocks<C: ToSql + FromSql>(
    txn: &Transaction,
    cid: C,
) -> crate::Result<Vec<C>> {
    let id = get_or_create_id(&txn, cid)?;
    let res = txn.prepare_cached(
        r#"
WITH RECURSIVE
    -- find descendants of cid, including the id of the cid itself
    descendant_of(id) AS (
        SELECT ?
        UNION ALL
        SELECT DISTINCT child_id FROM refs JOIN descendant_of ON descendant_of.id=refs.parent_id
    ),
    -- find orphaned ids
    orphaned_ids as (
      SELECT DISTINCT id FROM descendant_of LEFT JOIN blocks ON descendant_of.id = blocks.block_id WHERE blocks.block_id IS NULL
    )
    -- retrieve corresponding cids - this is a set because of select distinct
SELECT cid from cids JOIN orphaned_ids ON cids.id = orphaned_ids.id
"#,
    )?
        .query_map([id], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<C>>>()?;
    Ok(res)
}

pub(crate) fn alias<C: ToSql>(
    txn: &Transaction,
    name: &[u8],
    key: Option<&C>,
) -> crate::Result<()> {
    if let Some(key) = key {
        let id = get_or_create_id(txn, key)?;
        txn.prepare_cached("REPLACE INTO aliases (name, block_id) VALUES (?, ?)")?
            .execute(params![name, id])?;
    } else {
        txn.prepare_cached("DELETE FROM aliases WHERE name = ?")?
            .execute([name])?;
    }
    Ok(())
}

pub(crate) fn resolve<C: FromSql>(txn: &Transaction, name: &[u8]) -> crate::Result<Option<C>> {
    Ok(txn
        .prepare_cached(
            r#"
SELECT cids.cid FROM aliases JOIN cids ON aliases.block_id = cids.id AND aliases.name = ?
"#,
        )?
        .query_row([name], |row| row.get(0))
        .optional()?)
}

pub(crate) fn reverse_alias(
    txn: &Transaction,
    cid: impl ToSql,
) -> crate::Result<Option<Vec<Vec<u8>>>> {
    if let Some(id) = get_id(txn, cid)? {
        Ok(Some(
            txn.prepare_cached(
                r#"
WITH RECURSIVE
    ancestor_of(id) AS
    (
        SELECT ?
        UNION ALL
        SELECT DISTINCT parent_id FROM refs JOIN ancestor_of ON ancestor_of.id=refs.child_id
    )
SELECT DISTINCT name FROM ancestor_of LEFT JOIN aliases ON ancestor_of.id = block_id;
"#,
            )?
            .query_map([id], |row| row.get(0))?
            .filter_map(|a| a.transpose())
            .collect::<rusqlite::Result<Vec<Vec<u8>>>>()?,
        ))
    } else {
        Ok(None)
    }
}

/// get all ids corresponding to cids that we have a block for
pub(crate) fn get_ids(txn: &Transaction) -> crate::Result<Vec<i64>> {
    Ok(txn
        .prepare_cached(r#"SELECT id FROM cids JOIN blocks ON id = block_id"#)?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<i64>>>()?)
}

/// get all cids of blocks in the store
pub(crate) fn get_block_cids<C: FromSql>(txn: &Transaction) -> crate::Result<Vec<C>> {
    Ok(txn
        .prepare_cached(r#"SELECT cid FROM cids JOIN blocks ON id = block_id"#)?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<C>>>()?)
}

/// get all cids that we know about, even ones that we don't have a block for
pub(crate) fn get_known_cids<C: FromSql>(txn: &Transaction) -> crate::Result<Vec<C>> {
    Ok(txn
        .prepare_cached(r#"SELECT cid FROM cids"#)?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<C>>>()?)
}

pub(crate) fn aliases<C: FromSql>(txn: &Transaction) -> crate::Result<Vec<(Vec<u8>, C)>> {
    Ok(txn
        .prepare_cached(r#"SELECT name, cid FROM aliases JOIN cids ON id = block_id"#)?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<Vec<(Vec<u8>, C)>>>()?)
}

pub(crate) fn vacuum(conn: &Connection) -> crate::Result<()> {
    conn.execute("VACUUM;", [])?;
    Ok(())
}

pub(crate) fn init_db(
    conn: &mut Connection,
    is_memory: bool,
    cache_pages: i64,
    synchronous: Synchronous,
) -> anyhow::Result<()> {
    conn.execute_batch(PRAGMAS)?;
    conn.pragma_update(None, "synchronous", &synchronous.to_string())?;
    conn.pragma_update(None, "cache_pages", &cache_pages)?;
    let foreign_keys: i64 = conn.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    let journal_mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    let expected_journal_mode = if is_memory { "memory" } else { "wal" };
    assert_eq!(foreign_keys, 1);
    assert_eq!(journal_mode, expected_journal_mode.to_owned());
    // use in_txn so we get the logging
    in_txn(conn, |txn| {
        if user_version(&txn)? == 0 && table_exists(&txn, "blocks")? {
            Ok(migrate_v0_v1(&txn)?)
        } else {
            Ok(txn.execute_batch(INIT)?)
        }
    })?;
    assert!(conn.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?);
    Ok(())
}

pub(crate) fn integrity_check(conn: &Connection) -> crate::Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT integrity_check FROM pragma_integrity_check")?;
    let result = stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(result)
}

/// helper to log execution time of a block of code that returns a result
///
/// will log at info level if `expected_duration` is exceeded,
/// at warn level if the result is a failure, and
/// just at debug level if the operation is quick and successful.
///
/// this is an attempt to avoid spamming the log with lots of irrelevant info.
pub(crate) fn log_execution_time<T, E>(
    msg: &str,
    expected_duration: Duration,
    f: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<T, E> {
    let t0 = Instant::now();
    let result = (f)();
    let dt = t0.elapsed();
    if result.is_err() {
        warn!("{} took {}us and failed", msg, dt.as_micros());
    } else if dt > expected_duration {
        info!("{} took {}us", msg, dt.as_micros());
    } else {
        debug!("{} took {}us", msg, dt.as_micros());
    };
    result
}

/// execute a statement in a write transaction
pub(crate) fn in_txn<T>(
    conn: &mut Connection,
    f: impl FnOnce(&Transaction) -> crate::Result<T>,
) -> crate::Result<T> {
    let txn = conn.transaction()?;
    let result = f(&txn);
    match result {
        Ok(value) => {
            trace!("committing transaction!");
            if let Err(cause) = txn.commit() {
                error!("unable to commit transaction! {}", cause);
                return Err(cause.into());
            }
            Ok(value)
        }
        Err(cause) => {
            error!("rolling back transaction! {}", cause);
            Err(cause)
        }
    }
}

/// execute a statement in a readonly transaction
/// nested transactions are not allowed here.
pub(crate) fn in_ro_txn<T>(
    conn: &Connection,
    f: impl FnOnce(&Transaction) -> crate::Result<T>,
) -> crate::Result<T> {
    let txn = conn.unchecked_transaction()?;
    f(&txn)
}

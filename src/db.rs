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
    config::DbConfig,
    params, params_from_iter,
    types::FromSql,
    Connection,
    Error::{QueryReturnedNoRows, SqliteFailure},
    ErrorCode::DatabaseBusy,
    OptionalExtension, ToSql, Transaction,
};
use std::{
    collections::{BTreeSet, HashSet},
    convert::TryFrom,
    time::Duration,
    time::Instant,
};

use crate::{
    cache::{BlockInfo, CacheTracker},
    cidbytes::CidBytes,
    error::Context,
    BlockStoreError, SizeTargets, StoreStats, Synchronous, TempPin,
};
use anyhow::Context as _;
use itertools::Itertools;

const PRAGMAS: &str = r#"
-- this must be done before creating the first table, otherwise it has no effect
PRAGMA auto_vacuum = 2;
-- this must be done before changing the database via the CLI!
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA wal_checkpoint(TRUNCATE);
-- I tried different even larger values for this. Did not make a difference.
PRAGMA page_size = 4096;
"#;

const TABLES: &[(&str, &str)] = &[
    (
        "cids",
        "CREATE TABLE cids ( \
            id INTEGER PRIMARY KEY AUTOINCREMENT, \
            cid BLOB UNIQUE NOT NULL \
        )",
    ),
    (
        "refs",
        "CREATE TABLE refs ( \
            parent_id INTEGER NOT NULL, \
            child_id INTEGER NOT NULL, \
            PRIMARY KEY(parent_id,child_id) \
            CONSTRAINT fk_parent_block \
              FOREIGN KEY (parent_id) \
              REFERENCES blocks(block_id) \
              ON DELETE CASCADE \
            CONSTRAINT fk_child_id \
              FOREIGN KEY (child_id) \
              REFERENCES cids(id) \
              ON DELETE RESTRICT \
        )",
    ),
    (
        "blocks",
        "CREATE TABLE blocks ( \
            block_id INTEGER PRIMARY KEY, \
            block BLOB NOT NULL, \
            CONSTRAINT fk_block_cid \
              FOREIGN KEY (block_id) \
              REFERENCES cids(id) \
              ON DELETE CASCADE \
        )",
    ),
    (
        "aliases",
        "CREATE TABLE aliases ( \
            name blob NOT NULL PRIMARY KEY, \
            block_id INTEGER NOT NULL, \
            CONSTRAINT fk_block_id \
              FOREIGN KEY (block_id) \
              REFERENCES cids(id) \
              ON DELETE CASCADE \
        )",
    ),
    (
        "temp_pins",
        "CREATE TABLE temp_pins ( \
            id INTEGER NOT NULL, \
            block_id INTEGER NOT NULL, \
            PRIMARY KEY(id,block_id) \
            CONSTRAINT fk_block_id \
              FOREIGN KEY (block_id) \
              REFERENCES cids(id) \
              ON DELETE RESTRICT \
        )",
    ),
    (
        "stats",
        "CREATE TABLE stats ( \
            count INTEGER NOT NULL, \
            size INTEGER NOT NULL \
        )",
    ),
];

const INIT: &str = r#"
PRAGMA user_version = 2;

CREATE INDEX IF NOT EXISTS idx_refs_child_id
ON refs (child_id);

CREATE INDEX IF NOT EXISTS idx_aliases_block_id
ON aliases (block_id);

CREATE INDEX IF NOT EXISTS idx_temp_pins_block_id
ON temp_pins (block_id);
"#;

const CLEANUP_TEMP_PINS: &str = r#"
-- delete temp aliases that were not dropped because of crash
DELETE FROM temp_pins;
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

macro_rules! c {
    (DEBUG $t:literal => $e:expr) => {{
        tracing::debug!($t);
        $e.ctx(concat!($t, " (line ", line!(), ")"))?
    }};
    ($t:literal => $e:expr) => {
        $e.ctx(concat!($t, " (line ", line!(), ")"))?
    };
}

fn get_id(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<Option<i64>> {
    txn.prepare_cached("SELECT id FROM cids WHERE cid=?")?
        .query_row([cid], |row| row.get(0))
        .optional()
}

/// returns the number and size of blocks, excluding orphaned blocks, computed from scratch
pub(crate) fn compute_store_stats(txn: &Transaction) -> crate::Result<StoreStats> {
    let (count, size): (i64, i64) = txn
        .prepare(
            "SELECT COUNT(id), COALESCE(SUM(LENGTH(block)), 0) \
                FROM cids, blocks ON id = block_id",
        )
        .ctx("computing store stats (prep)")?
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
        .ctx("computing store stats")?;
    Ok(StoreStats {
        count: u64::try_from(count).ctx("computed count")?,
        size: u64::try_from(size).ctx("computed size")?,
    })
}

/// recomputes the store stats (should be done at startup to prevent unbounded drift)
pub(crate) fn recompute_store_stats(conn: &mut Connection) -> crate::Result<()> {
    let _span = tracing::debug_span!("check stats").entered();
    // first a read-only transaction to determine the true base
    let (stats, truth) = in_txn(conn, None, |txn| {
        let stats = get_store_stats(txn)?;
        let truth = compute_store_stats(txn)?;
        Ok((stats, truth))
    })?;

    tracing::debug!("applying findings");
    // now compute the correction based on what the above snapshot has calculated
    in_txn(conn, None, |txn| {
        let stats2 = get_store_stats(txn)?;
        let new_stats = StoreStats {
            count: stats2.count - stats.count + truth.count,
            size: stats2.size - stats.size + truth.size,
        };
        if new_stats != stats2 {
            tracing::info!(
                "correcting usage stats from {:?} to {:?}",
                stats2,
                new_stats
            );
            txn.prepare_cached("UPDATE stats SET count = ?, size = ?")
                .ctx("updating stats (prep)")?
                .execute([new_stats.count, new_stats.size])
                .ctx("updating stats")?;
        } else {
            tracing::debug!("usage stats were correct");
        }
        Ok(())
    })?;

    Ok(())
}

/// returns the number and size of blocks, excluding orphaned blocks, from the stats table
pub(crate) fn get_store_stats(txn: &Transaction) -> crate::Result<StoreStats> {
    let (count, size): (i64, i64) = txn
        .prepare_cached("SELECT count, size FROM stats LIMIT 1")
        .ctx("getting store stats (prep)")?
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
        .ctx("getting store stats")?;
    let result = StoreStats {
        count: u64::try_from(count).ctx("getting count")?,
        size: u64::try_from(size).ctx("getting size")?,
    };
    Ok(result)
}

fn get_or_create_id(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<i64> {
    txn.prepare_cached(
        "INSERT INTO cids (cid) VALUES (?) ON CONFLICT DO UPDATE SET cid=cid RETURNING id",
    )?
    .query_row([cid], |row| row.get(0))
}

// This is the plan:
//
// First figure out in a read transaction which blocks are not referenced; ideally get an iterator
// for these and stop iterating when enough sufficiently low-prio blocks have been found as seen by
// the CacheTracker. In a second step delete from least important upwards, block by block, in a
// write transaction that first checks whether that particular block is still unreferenced. Then
// at the end perform an incremental or full vacuum, depending on config or fragmentation state.
pub(crate) fn incremental_gc(
    conn: &mut Connection,
    min_blocks: usize,
    max_duration: Duration,
    size_targets: SizeTargets,
    cache_tracker: &impl CacheTracker,
) -> crate::Result<bool> {
    let _span = tracing::debug_span!("GC", %min_blocks, ?max_duration).entered();

    // get the store stats from the stats table:
    // if we don't exceed any of the size targets, there is nothing to do
    let mut stats = in_txn(conn, None, get_store_stats)?;
    if !size_targets.exceeded(&stats) {
        tracing::info!(
            blocks = display(stats.count),
            size = display(stats.size),
            "nothing to do"
        );
        return Ok(true);
    }

    let t0 = Instant::now();

    let mut ids = in_txn(
        conn,
        Some(("getting unreferenced CIDs", Duration::from_secs(3))),
        |txn| {
            // find all ids that are not pinned (directly or indirectly)
            let mut id_query = txn
                .prepare_cached(
                    r#"
                    WITH RECURSIVE
                        descendant_of(id) AS
                        (
                            SELECT block_id FROM aliases UNION SELECT block_id FROM temp_pins
                            UNION
                            SELECT child_id FROM refs, descendant_of ON id = parent_id
                        )
                    SELECT id FROM cids
                    WHERE id NOT IN descendant_of;
                    "#,
                )
                .ctx("finding GC blocks (prep)")?;
            // log execution time of the non-interruptible query that computes the set of ids to delete
            let ret = id_query
                .query_map([], |row| row.get(0))
                .ctx("finding GC blocks")?
                .collect::<rusqlite::Result<Vec<i64>>>()
                .ctx("reading GC block ID")?;
            Ok(ret)
        },
    )?;

    // give the cache tracker the opportunity to sort the non-pinned ids by value
    let span = tracing::debug_span!("sorting CIDs").entered();
    cache_tracker.sort_ids(&mut ids);
    drop(span);

    let mut n = 0;
    let mut ret_val = true;
    for id in ids.iter() {
        if n >= min_blocks && t0.elapsed() > max_duration {
            tracing::info!(removed = n, "stopping due to time constraint");
            ret_val = false;
            break;
        }
        if !size_targets.exceeded(&stats) {
            tracing::info!(removed = n, "finished, target reached");
            break;
        }
        let res = in_txn(conn, Some(("", Duration::from_millis(100))), |txn| {
            // get block size and check whether now referenced
            let mut block_size_stmt = c!("getting GC block (prep)" => txn.prepare_cached(
                r#"
                WITH RECURSIVE
                    ancestor(id) AS (
                        SELECT ?
                        UNION -- must not use UNION ALL in case of pathologically linked dags
                        SELECT parent_id FROM refs, ancestor ON id = child_id
                    ),
                    names AS (SELECT name FROM ancestor, aliases ON id = block_id)
                SELECT LENGTH(block), cid, (SELECT count(*) FROM names)
                    FROM cids, blocks ON id = block_id WHERE id = ?;
                "#,
            ));
            let mut update_stats_stmt = c!("updating GC stats (prep)" =>
                txn.prepare_cached("UPDATE stats SET count = count - 1, size = size - ?"));
            let mut delete_stmt = c!("deleting GC block (prep)" => txn.prepare_cached("DELETE FROM blocks WHERE block_id = ?"));

            tracing::trace!("deleting id {}", id);

            let block_size: Option<(i64, CidBytes, i64)> = block_size_stmt
                .query_row([id, id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .optional()
                .ctx("getting GC block")?;
            tracing::trace!(block_size = ?&block_size);
            if let Some((block_size, cid, names)) = block_size {
                if names != 0 {
                    // block is referenced again
                    return Ok(None);
                }
                let cid = Cid::try_from(&cid)?;
                let len = c!("getting GC block size" => usize::try_from(block_size));
                c!("updating GC stats" => update_stats_stmt.execute([block_size]));
                tracing::trace!("stats updated");
                c!("deleting GC block" => delete_stmt.execute(&[id]));
                Ok(Some((block_size, cid, len)))
            } else {
                Ok(None)
            }
        })?;
        if let Some((size, cid, len)) = res {
            stats.count -= 1;
            stats.size -= size as u64;
            cache_tracker.blocks_deleted(vec![BlockInfo::new(*id, &cid, len)]);
            n += 1;
        }
    }

    if n > 0 {
        // the above only removed the blocks, now we need to clean up those cids that we don’t
        // need anymore

        // doing this in one transaction may block the DB for too long, so get the IDs first and then
        // remove them in batches
        let ids = in_txn(
            conn,
            Some(("getting IDs to clean up", Duration::from_secs(5))),
            |txn| {
                let mut stmt = c!("getting IDs (prep)" => txn.prepare_cached(
                        // refs.parent_id is not a blocker because if we delete this it means that
                        // the block is gone
                        "SELECT id FROM cids WHERE \
                        id NOT IN (SELECT block_id FROM blocks) AND \
                        id NOT IN (SELECT block_id FROM aliases) AND \
                        id NOT IN (SELECT child_id FROM refs) AND \
                        id NOT IN (SELECT block_id FROM temp_pins)",
                ));
                let ids = c!("getting IDs" => stmt.query_map([], |row| row.get(0)));
                ids.collect::<Result<Vec<i64>, _>>().ctx("ids")
            },
        )?;

        tracing::debug!("cleaning up {} IDs", ids.len());

        // this number is linked to the prepared query below!
        const BATCH_SIZE: usize = 10;
        let mut v = Vec::with_capacity(BATCH_SIZE);
        for ids in &ids.into_iter().chunks(BATCH_SIZE) {
            v.extend(ids);
            if v.len() == BATCH_SIZE {
                in_txn(
                    conn,
                    Some(("cleaning up CIDs", Duration::from_millis(100))),
                    |txn| {
                        let mut del_cid = c!("deleting CIDs (prep)" => txn.prepare_cached(
                            "DELETE FROM cids WHERE \
                                id in (VALUES (?), (?), (?), (?), (?), (?), (?), (?), (?), (?)) AND \
                                id NOT IN (SELECT block_id FROM blocks) AND \
                                id NOT IN (SELECT block_id FROM aliases) AND \
                                id NOT IN (SELECT child_id FROM refs) AND \
                                id NOT IN (SELECT block_id FROM temp_pins)"
                        ));
                        c!("deleting CIDs" => del_cid.execute(params_from_iter(v.iter())));
                        Ok(())
                    },
                )?;
            } else {
                in_txn(conn, None, |txn| {
                    let mut stmt = c!("deleting CIDs (prep)" => txn.prepare_cached(
                        "DELETE FROM cids WHERE \
                            id = ? AND \
                            id NOT IN (SELECT block_id FROM blocks) AND \
                            id NOT IN (SELECT block_id FROM aliases) AND \
                            id NOT IN (SELECT child_id FROM refs) AND \
                            id NOT IN (SELECT block_id FROM temp_pins)"
                    ));
                    for id in v.iter() {
                        c!("deleting CIDs" => stmt.execute([id]));
                    }
                    Ok(())
                })?;
            }
            v.clear();
        }
    }

    Ok(ret_val)
}

pub(crate) fn delete_temp_pin(txn: &Transaction, pin: i64) -> crate::Result<()> {
    let mut stmt =
        c!("deleting temp_pin (prep)" => txn.prepare_cached("DELETE FROM temp_pins WHERE id = ?"));
    c!("deleting temp_pin" => stmt.execute([pin]));
    Ok(())
}

pub(crate) fn extend_temp_pin(
    txn: &Transaction,
    pin: &mut TempPin,
    links: Vec<impl ToSql>,
) -> crate::Result<()> {
    for link in links {
        let block_id = c!("getting ID for temp pinning" => get_or_create_id(txn, link));
        // it is important that the above is a write action, because otherwise a rollback may
        // invalidate the id stored in the TempPin in the below
        add_temp_pin(txn, block_id, pin).context("extending temp_pin")?;
    }
    Ok(())
}

fn add_temp_pin(txn: &Transaction, block_id: i64, pin: &mut TempPin) -> crate::Result<()> {
    if pin.id > 0 {
        txn.prepare_cached("INSERT OR IGNORE INTO temp_pins (id, block_id) VALUES (?, ?)")
            .ctx("extending existing temp_pin (prep)")?
            .execute([pin.id, block_id])
            .ctx("extending existing temp_pin")?;
    } else {
        // we must not reuse IDs, but sqlite takes care of transactionality here
        pin.id = txn
            .prepare_cached(
                "INSERT INTO temp_pins (id, block_id) VALUES \
                ((SELECT coalesce(max(id), 0) FROM temp_pins) + 1, ?) RETURNING id",
            )
            .ctx("creating new temp_pin (prep)")?
            .query_row([block_id], |row| row.get(0))
            .ctx("creating new temp_pin")?;
    }
    Ok(())
}

pub(crate) struct PutBlockResult {
    /// id for the cid
    pub(crate) id: i64,
    /// true if the block already existed
    pub(crate) block_exists: bool,
}

pub(crate) fn put_block<C: ToSql>(
    txn: &Transaction,
    key: &C,
    data: &[u8],
    links: impl IntoIterator<Item = C>,
    pin: Option<&mut TempPin>,
) -> crate::Result<PutBlockResult> {
    // this is important: we need write lock on the table so that add_temp_pin is never rolled back
    let block_id = c!("getting put_block ID" => get_or_create_id(txn, key));
    let block_exists = txn
        .prepare_cached("SELECT COUNT(*) FROM blocks WHERE block_id = ?")
        .ctx("checking put_block (prep)")?
        .query_row([block_id], |row| Ok(row.get::<_, i64>(0)? == 1))
        .ctx("checking put_block")?;
    if !block_exists {
        // add the block itself
        txn.prepare_cached("INSERT INTO blocks (block_id, block) VALUES (?, ?)")
            .ctx("adding put_block (prep)")?
            .execute(params![block_id, &data])
            .ctx("adding put_block")?;

        // update the stats
        txn.prepare_cached("UPDATE stats SET count = count + 1, size = size + ?")
            .ctx("updating put_block stats (prep)")?
            .execute([data.len() as i64])
            .ctx("updating put_block stats")?;

        // insert the links
        let mut insert_ref = txn
            .prepare_cached("INSERT INTO refs (parent_id, child_id) VALUES (?,?)")
            .ctx("adding put_block link (prep)")?;
        for link in links {
            let child_id: i64 = c!("getting put_block link ID" => get_or_create_id(txn, link));
            insert_ref
                .execute([block_id, child_id])
                .ctx("adding put_block link")?;
        }
    }
    if let Some(pin) = pin {
        // create a temporary alias for the block, even if it already exists
        // this is only safe because get_or_create_id ensured that we have write lock on the table
        add_temp_pin(txn, block_id, pin).context("adding put_block temp_pin")?;
    }
    Ok(PutBlockResult {
        id: block_id,
        block_exists,
    })
}

/// Get a block
pub(crate) fn get_block(
    txn: &Transaction,
    cid: impl ToSql,
) -> crate::Result<Option<(i64, Vec<u8>)>> {
    let id = c!("getting get_block ID" => get_id(txn, cid));
    Ok(if let Some(id) = id {
        txn.prepare_cached("SELECT block FROM blocks WHERE block_id = ?")
            .ctx("getting get_block (prep)")?
            .query_row([id], |row| row.get(0))
            .optional()
            .ctx("getting get_block")?
            .map(|b| (id, b))
    } else {
        None
    })
}

/// Check if we have a block
pub(crate) fn has_block(txn: &Transaction, cid: impl ToSql) -> crate::Result<bool> {
    Ok(txn
        .prepare_cached("SELECT 1 FROM blocks, cids ON block_id = id WHERE cid = ?")
        .ctx("getting has_block (prep)")?
        .query_row([cid], |_| Ok(()))
        .optional()
        .ctx("getting has_block")?
        .is_some())
}

/// Check if we have a cid
pub(crate) fn has_cid(txn: &Transaction, cid: impl ToSql) -> crate::Result<bool> {
    Ok(txn
        .prepare_cached("SELECT 1 FROM cids WHERE cids.cid = ?")
        .ctx("getting has_cid (prep)")?
        .query_row([cid], |_| Ok(()))
        .optional()
        .ctx("getting has_cid")?
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
                    UNION
                    SELECT child_id FROM refs, descendant_of ON id = parent_id
                )
                -- retrieve corresponding cids - this is a set because of select distinct
                SELECT cid from cids, descendant_of USING (id);
            "#,
        )
        .ctx("getting descendants (prep)")?
        .query_map([cid], |row| row.get(0))
        .ctx("getting descendants")?
        .collect::<rusqlite::Result<Vec<C>>>()
        .ctx("parsing descendants")?;
    Ok(res)
}

/// get the set of descendants of an id for which we do not have the data yet.
/// The value itself is included.
/// It is safe to call this method for a cid we don't have yet.
pub(crate) fn get_missing_blocks<C: ToSql + FromSql>(
    txn: &Transaction,
    cid: C,
) -> crate::Result<Vec<C>> {
    let id = c!("getting missing_blocks ID" => get_or_create_id(txn, cid));
    let res = txn
        .prepare_cached(
            r#"
                WITH RECURSIVE
                    -- find descendants of cid, including the id of the cid itself
                    desc(id) AS (
                        SELECT ?
                        UNION
                        SELECT child_id FROM refs, desc ON id = parent_id
                    ),
                    -- find orphaned ids
                    orphaned_ids as (
                      SELECT id FROM desc LEFT JOIN blocks ON id = block_id WHERE block_id IS NULL
                    )
                    -- retrieve corresponding cids - this is a set because of select distinct
                SELECT cid FROM cids, orphaned_ids USING (id)
                "#,
        )
        .ctx("finding missing_blocks (prep)")?
        .query_map([id], |row| row.get(0))
        .ctx("finding missing_blocks")?
        .collect::<rusqlite::Result<Vec<C>>>()
        .ctx("parsing missing_blocks")?;
    Ok(res)
}

pub(crate) fn alias<C: ToSql>(
    txn: &Transaction,
    name: &[u8],
    key: Option<&C>,
) -> crate::Result<()> {
    if let Some(key) = key {
        let id = c!("getting alias ID" => get_or_create_id(txn, key));
        txn.prepare_cached("REPLACE INTO aliases (name, block_id) VALUES (?, ?)")
            .ctx("setting alias (prep)")?
            .execute(params![name, id])
            .ctx("setting alias")?;
    } else {
        txn.prepare_cached("DELETE FROM aliases WHERE name = ?")
            .ctx("removing alias (prep)")?
            .execute([name])
            .ctx("removing alias")?;
    }
    Ok(())
}

pub(crate) fn resolve<C: FromSql>(txn: &Transaction, name: &[u8]) -> crate::Result<Option<C>> {
    txn.prepare_cached("SELECT cid FROM aliases, cids ON block_id = id WHERE name = ?")
        .ctx("resolving alias (prep)")?
        .query_row([name], |row| row.get(0))
        .optional()
        .ctx("resolving alias")
}

pub(crate) fn reverse_alias(
    txn: &Transaction,
    cid: impl ToSql,
) -> crate::Result<Option<HashSet<Vec<u8>>>> {
    if let Some(id) = c!("getting reverse_alias ID" => get_id(txn, cid)) {
        Ok(Some(
            txn.prepare_cached(
                r#"
                WITH RECURSIVE
                    ancestor_of(id) AS
                    (
                        SELECT ?
                        UNION
                        SELECT parent_id FROM refs, ancestor_of ON id = child_id
                    )
                SELECT name FROM ancestor_of, aliases ON id = block_id;
                "#,
            )
            .ctx("getting reverse_alias (prep)")?
            .query_map([id], |row| row.get::<_, Vec<u8>>(0))
            .ctx("getting reverse_alias")?
            .collect::<rusqlite::Result<_>>()
            .ctx("parsing reverse_alias")?,
        ))
    } else {
        Ok(None)
    }
}

/// get all ids corresponding to cids that we have a block for
pub(crate) fn get_ids(txn: &Transaction) -> crate::Result<Vec<i64>> {
    txn.prepare_cached("SELECT id FROM cids JOIN blocks ON id = block_id")
        .ctx("getting IDs (prep)")?
        .query_map([], |row| row.get(0))
        .ctx("getting IDs")?
        .collect::<rusqlite::Result<Vec<i64>>>()
        .ctx("parsing IDs")
}

/// get all cids of blocks in the store
pub(crate) fn get_block_cids<C: FromSql>(txn: &Transaction) -> crate::Result<Vec<C>> {
    txn.prepare_cached("SELECT cid FROM cids JOIN blocks ON id = block_id")
        .ctx("getting all CIDs (prep)")?
        .query_map([], |row| row.get(0))
        .ctx("getting all CIDs")?
        .collect::<rusqlite::Result<Vec<C>>>()
        .ctx("parsing all CIDs")
}

/// get all cids that we know about, even ones that we don't have a block for
pub(crate) fn get_known_cids<C: FromSql>(txn: &Transaction) -> crate::Result<Vec<C>> {
    txn.prepare_cached("SELECT cid FROM cids")
        .ctx("getting known CIDs (prep)")?
        .query_map([], |row| row.get(0))
        .ctx("getting known CIDs")?
        .collect::<rusqlite::Result<Vec<C>>>()
        .ctx("parsing known CIDs")
}

pub(crate) fn aliases<C: FromSql>(txn: &Transaction) -> crate::Result<Vec<(Vec<u8>, C)>> {
    txn.prepare_cached("SELECT name, cid FROM aliases JOIN cids ON id = block_id")
        .ctx("getting aliases (prep)")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .ctx("getting aliases")?
        .collect::<rusqlite::Result<Vec<(Vec<u8>, C)>>>()
        .ctx("parsing aliases")
}

pub(crate) fn vacuum(conn: &mut Connection) -> crate::Result<()> {
    let _span = tracing::debug_span!("vacuuming the db").entered();
    conn.execute("VACUUM;", []).ctx("running VACUUM")?;
    Ok(())
}

pub(crate) fn init_pragmas(
    conn: &mut Connection,
    is_memory: bool,
    cache_pages: i64,
) -> crate::Result<()> {
    c!("running pragmas" => conn.execute_batch(PRAGMAS));
    c!("setting cache_pages" => conn.pragma_update(None, "cache_pages", &cache_pages));

    let foreign_keys: i64 = c!("getting foreign_keys" => conn.pragma_query_value(None, "foreign_keys", |row| row.get(0)));
    let journal_mode: String = c!("getting journal_mode" => conn.pragma_query_value(None, "journal_mode", |row| row.get(0)));
    let expected_journal_mode = if is_memory { "memory" } else { "wal" };
    assert_eq!(foreign_keys, 1);
    assert_eq!(journal_mode, expected_journal_mode);

    conn.set_prepared_statement_cache_capacity(100);

    if !c!("checking foreign keys" => conn.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)) {
        Err(BlockStoreError::Other(anyhow::anyhow!(
            "foreign keys not enabled in SQLITE library"
        )))
    } else {
        Ok(())
    }
}

fn ws(s: impl AsRef<str>) -> String {
    let mut r = String::new();
    for (i, t) in s.as_ref().split_whitespace().enumerate() {
        if i > 0 {
            r.push(' ');
        }
        r.push_str(t);
    }
    r.to_lowercase()
        .replace("primary_key", "primary key")
        // adding AUTOINCREMENT doesn’t really work, but it doesn’t hurt here either
        // (only CREATE TABLE switched on AUTOINCREMENT special behaviour, but our CIDs
        // don’t need monotonically incrementing IDs, unique is enough)
        .replace("primary key autoincrement", "primary key")
        .replace("unique", "primary key")
}

fn ensure_table(txn: &Transaction, name: &str, sql: &str) -> crate::Result<bool> {
    let mut in_db = c!("getting table (prep)" => txn
        .prepare("SELECT sql FROM sqlite_master WHERE type = 'table' and name = ?"));
    let in_db = c!("getting table" => in_db
        .query_row([name], |row| row.get::<_, String>(0))
        .optional());

    if let Some(existing) = in_db {
        let ex_ws = ws(existing);
        let sql_ws = ws(sql);
        if ex_ws == sql_ws {
            // all good, it has the right definition already
            tracing::debug!("table {} is up-to-date", name);
            return Ok(false);
        }
        if let Some(prefix) = ex_ws.find("constraint") {
            // definitions must be equal up to the first constraint
            if ex_ws[..prefix] != sql_ws[..prefix] {
                return Err(BlockStoreError::Other(anyhow::anyhow!(
                    "cannot update table `{}` due to incompatible data content",
                    name
                )));
            }
        } else {
            // it is only okay to add constraints before the closing paren
            let ex_trim = ex_ws.trim_end_matches(|c| " )".contains(c));
            if sql_ws[..ex_trim.len()] != *ex_trim
                || !sql_ws[ex_trim.len()..]
                    .trim_start_matches(|c| ", ".contains(c))
                    .starts_with("constraint")
            {
                return Err(BlockStoreError::Other(anyhow::anyhow!(
                    "cannot update table `{}` due to incompatible data content",
                    name
                )));
            }
        }
        // okay, let’s try the update (knock wood)
        tracing::debug!("updating table {}", name);
        c!("change table" => txn.execute(
            "UPDATE sqlite_master SET sql = ? WHERE type = 'table' and name = ?",
            [sql, name]
        ));
        Ok(true)
    } else {
        tracing::debug!("creating table {}", name);
        c!("creating table" => txn.execute_batch(sql));
        Ok(false)
    }
}

fn ensure_tables(txn: &Transaction, tables: &[(&str, &str)]) -> crate::Result<()> {
    let version = c!("schema version" =>
            txn.pragma_query_value(None, "schema_version", |r| r.get::<_, i64>(0)));
    let mut changed = false;

    c!("writable schema" => txn.pragma_update(None, "writable_schema", true));
    for (name, sql) in tables {
        changed |=
            ensure_table(txn, *name, *sql).with_context(|| format!("ensuring table {}", name))?;
    }

    if changed {
        c!("increment schema version" => txn.pragma_update(None, "schema_version", version + 1));
    }
    c!("writable schema" => txn.pragma_update(None, "writable_schema", false));

    c!("integrity check" => txn.execute_batch("PRAGMA integrity_check"));
    Ok(())
}

fn migrate_v0_v1(txn: &Transaction) -> crate::Result<()> {
    let num_blocks: i64 = c!("getting block count" => txn.query_row("SELECT COUNT(*) FROM blocks_v0", [], |r| r.get(0)));
    let mut stmt = c!("getting old blocks (prep)" => txn.prepare("SELECT * FROM blocks_v0"));
    let block_iter = c!("getting old blocks" =>
        stmt.query_map([], |row| { Ok((row.get::<_, Vec<u8>>(2)?, row.get::<_, Vec<u8>>(3)?)) }));
    for (i, block) in block_iter.enumerate() {
        if num_blocks != 0 && i % 1000 == 0 {
            tracing::info!(
                "converting to new blocks, block {} of {} ({}%)",
                i,
                num_blocks,
                100 * i / (num_blocks as usize)
            );
        }
        let (cid, data) = c!("reading blobs" => block);
        let cid = Cid::try_from(cid).context("parsing CID")?;
        let block = libipld::Block::<DefaultParams>::new(cid, data).context("creating block")?;
        let mut set = BTreeSet::new();
        block
            .references(&mut set)
            .context("extracting references")?;
        put_block(
            txn,
            &block.cid().to_bytes(),
            block.data(),
            set.into_iter()
                .map(|cid| cid.to_bytes())
                .collect::<Vec<_>>(),
            None,
        )?;
    }
    tracing::info!("dropping table blocks_v0");
    c!("dropping old blocks" => txn.execute_batch("DROP TABLE blocks_v0"));
    drop(stmt);
    tracing::info!("migration from v0 to v1 done!");
    Ok(())
}

pub(crate) fn init_db(
    conn: &mut Connection,
    is_memory: bool,
    cache_pages: i64,
    synchronous: Synchronous,
) -> crate::Result<()> {
    let _span = tracing::debug_span!("initializing db").entered();

    // can’t be done inside a transaction
    init_pragmas(conn, is_memory, cache_pages)?;
    conn.pragma_update(None, "synchronous", &synchronous.to_string())
        .ctx("setting Synchronous mode")?;

    c!("foreign keys off" => conn.pragma_update(None, "foreign_keys", false));

    in_txn(conn, Some(("init", Duration::from_secs(1))), |txn| {
        let user_version = c!("getting user_version" => user_version(txn));
        if user_version > 2 {
            return Err(anyhow::anyhow!(
                "found future DB version {} (downgrades are not supported)",
                user_version
            )
            .into());
        }

        let migrate =
            user_version == 0 && c!("checking table `blocks`" => table_exists(txn, "blocks"));
        if migrate {
            tracing::info!("executing migration from v0 to v1");
            c!("renaming blocks to v0" => txn.execute_batch("ALTER TABLE blocks RENAME TO blocks_v0"));
            // drop the old refs table, since the content can be extracted from blocks_v0
            c!("dropping refs table" => txn.execute_batch("DROP TABLE IF EXISTS refs"));
        }

        ensure_tables(txn, TABLES)?;
        c!(DEBUG "creating indexes" => txn.execute_batch(INIT));
        c!(DEBUG "cleaning up temp pins" => txn.execute_batch(CLEANUP_TEMP_PINS));
        if let Err(BlockStoreError::SqliteError(QueryReturnedNoRows, _)) = get_store_stats(txn) {
            c!("faking store stats" => txn.execute_batch("INSERT INTO stats VALUES (0, 0);"));
        }

        if migrate {
            migrate_v0_v1(txn).context("migrating v0 -> v1")?;
        }

        Ok(())
    })?;

    c!("foreign keys on" => conn.pragma_update(None, "foreign_keys", false));
    Ok(())
}

pub(crate) fn integrity_check(conn: &mut Connection) -> crate::Result<Vec<String>> {
    let _span = tracing::debug_span!("db integrity check").entered();
    in_txn(conn, None, |txn| {
        let mut stmt = c!("checking sqlite integrity (prep)" => txn.prepare("SELECT integrity_check FROM pragma_integrity_check"));
        let result = c!("checking sqlite integrity" => stmt.query_map([], |row| row.get(0)))
            .collect::<rusqlite::Result<Vec<String>>>()
            .ctx("parsing sqlite integrity_check results")?;
        Ok(result)
    })
}

/// helper to log execution time of a block of code that returns a result
///
/// will log at info level if `expected_duration` is exceeded,
/// at warn level if the result is a failure, and
/// just at debug level if the operation is quick and successful.
///
/// this is an attempt to avoid spamming the log with lots of irrelevant info.
/// execute a statement in a write transaction
pub(crate) fn in_txn<T>(
    conn: &mut Connection,
    name: Option<(&str, Duration)>,
    mut f: impl FnMut(&Transaction) -> crate::Result<T>,
) -> crate::Result<T> {
    let _span = if let Some(name) = name.map(|x| x.0).filter(|x| !x.is_empty()) {
        tracing::debug_span!("txn", "{}", name).entered()
    } else {
        tracing::trace_span!("txn").entered()
    };
    let started = Instant::now();
    let mut attempts = 0;
    loop {
        let txn = c!("beginning transaction" => conn.transaction());
        let result = f(&txn);
        let result = result.and_then(|t| {
            c!("committing transaction" => txn.commit());
            Ok(t)
        });
        attempts += 1;
        match result {
            Ok(value) => {
                if let Some((name, expected)) = name {
                    let dt = started.elapsed();
                    if dt > expected {
                        tracing::info!("{} took {}ms", name, dt.as_millis());
                    }
                }
                break Ok(value);
            }
            Err(BlockStoreError::SqliteError(SqliteFailure(e, _), _)) if e.code == DatabaseBusy => {
                if attempts > 3 && started.elapsed().as_millis() > 100 {
                    tracing::warn!(
                        "getting starved ({} attempts so far, {}ms)",
                        attempts,
                        started.elapsed().as_millis()
                    );
                } else {
                    tracing::debug!("retrying transaction {:?}", name);
                }
            }
            Err(cause) => {
                tracing::error!("transaction rolled back! {:#}", cause);
                break Err(cause);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused)]
fn p(c: &Transaction, s: &str) {
    let mut stmt = c.prepare(s).unwrap();
    let columns = stmt.column_count();
    let x = stmt
        .query([])
        .unwrap()
        .mapped(|row| {
            Ok((0..columns)
                .map(|idx| format!("{:?}", row.get_ref_unwrap(idx)))
                .join(", "))
        })
        .map(|x| x.unwrap())
        .join("\n");
    println!("query: {}\nresults:\n{}", s, x);
}

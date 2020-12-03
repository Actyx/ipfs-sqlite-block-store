//! A sqlite based block store for content-addressed data that tries to do as much as possible
//! in the database.
//!
//! Tables:
//! cids: mapping from cid (blob < 64 bytes) to id (u64)
//! refs: m:n mapping from block ids to their children
//! blocks: the actual data for blocks, keyed by block id
//!    cids can exist in the system without having data associated with them!
//! alias: table that contains named pins for roots of graphs that should not be deleted by gc
//!    you can alias incomplete or in fact non-existing data. It is not necessary for a pinned dag
//!    to be complete.
use core::fmt;
use rusqlite::{
    config::DbConfig, params, types::FromSql, Connection, OptionalExtension, ToSql, Transaction,
    NO_PARAMS,
};
use std::{
    collections::BTreeSet,
    convert::TryFrom,
    marker::PhantomData,
    ops::DerefMut,
    path::Path,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
    time::Instant,
};
use tracing::*;

/// helper to log execution time of a block of code that returns a result
///
/// will log at info level if `expected_duration` is exceeded,
/// at warn level if the result is a failure, and
/// just at debug level if the operation is quick and successful.
///
/// this is an attempt to avoid spamming the log with lots of irrelevant info.
fn log_execution_time<T, E>(
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

const INIT: &'static str = r#"
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
-- PRAGMA synchronous = NORMAL;
PRAGMA synchronous = FULL;
PRAGMA page_size = 4096;
-- PRAGMA page_size = 8192;
-- PRAGMA page_size = 16384;
-- PRAGMA synchronous = OFF;
-- PRAGMA journal_mode = MEMORY;

CREATE TABLE IF NOT EXISTS cids (
    id INTEGER PRIMARY KEY,
    cid BLOB UNIQUE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cids_id
ON cids (id);

CREATE TABLE IF NOT EXISTS refs (
    parent_id INTEGER NOT NULL,
    child_id INTEGER NOT NULL,
    UNIQUE(parent_id,child_id)
    CONSTRAINT fk_parent_id
      FOREIGN KEY (parent_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
    CONSTRAINT fk_child_id
      FOREIGN KEY (child_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_refs_parent_id
ON refs (parent_id);

CREATE INDEX IF NOT EXISTS idx_refs_child_id
ON refs (child_id);

CREATE TABLE IF NOT EXISTS blocks (
    block_id INTEGER PRIMARY_KEY,
    block BLOB NOT NULL
);

-- for some reason this index is required to make the on delete cascade
-- fast, despite block_id being a PRIMARY_KEY.
CREATE INDEX IF NOT EXISTS idx_blocks_block_id
ON blocks (block_id);

CREATE TABLE IF NOT EXISTS aliases (
    name blob UNIQUE NOT NULL,
    block_id INTEGER NOT NULL,
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aliases_block_id
ON aliases (block_id);

CREATE TABLE IF NOT EXISTS temp_aliases (
    alias INTEGER NOT NULL,
    block_id INTEGER NOT NULL,
    UNIQUE(alias,block_id)
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_temp_aliases_block_id
ON temp_aliases (block_id);

CREATE INDEX IF NOT EXISTS idx_temp_aliases_alias
ON temp_aliases (alias);
"#;

pub struct BlockStore<C> {
    conn: Connection,
    expired_temp_aliases: Arc<Mutex<Vec<i64>>>,
    _c: PhantomData<C>,
}

/// a handle that contains a temporary alias
///
/// dropping this handle enqueue the alias for dropping before the next gc.
///
/// Note that implementing Clone for this would be a mistake.
pub struct TempAlias {
    id: AtomicI64,
    expired_temp_aliases: Arc<Mutex<Vec<i64>>>,
}

/// dump the temp alias id so you can find it in the database
impl fmt::Debug for TempAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.id.load(Ordering::SeqCst);
        let mut builder = f.debug_struct("TempAlias");
        if id > 0 {
            builder.field("id", &id);
        }
        builder.finish()
    }
}

impl Drop for TempAlias {
    fn drop(&mut self) {
        let id = self.id.get_mut();
        let alias = *id;
        if alias > 0 {
            // not sure if we have to guard against double drop, but it certainly does not hurt.
            *id = 0;
            self.expired_temp_aliases.lock().unwrap().push(alias);
        }
    }
}

fn get_id(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<Option<i64>> {
    txn.prepare_cached("SELECT id FROM cids WHERE cid=?")?
        .query_row(&[cid], |row| row.get(0))
        .optional()
}

fn get_block_size(txn: &Transaction) -> rusqlite::Result<i64> {
    txn.prepare("SELECT COALESCE(SUM(LENGTH(block)), 0) FROM blocks")?
        .query_row(NO_PARAMS, |row| row.get(0))
}

fn get_block_count(txn: &Transaction) -> rusqlite::Result<i64> {
    txn.prepare("SELECT count(*) FROM blocks")?
        .query_row(NO_PARAMS, |row| row.get(0))
}

fn get_or_create_id(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<i64> {
    let id = get_id(&txn, cid.to_sql()?)?;
    Ok(if let Some(id) = id {
        id
    } else {
        txn.prepare_cached("INSERT INTO cids (cid) VALUES (?)")?
            .execute(&[cid])?;
        txn.last_insert_rowid()
    })
}

fn incremental_gc(
    txn: &Transaction,
    min_blocks: usize,
    max_duration: Duration,
) -> rusqlite::Result<bool> {
    // find all ids that have neither a parent nor are aliased
    let mut id_query = txn.prepare_cached(
        r#"
WITH RECURSIVE
    descendant_of(id) AS
    (
        SELECT block_id FROM aliases UNION SELECT block_id FROM temp_aliases
        UNION ALL
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
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
    let ids = log_execution_time("gc_id_query", Duration::from_secs(1), || {
        id_query
            .query(NO_PARAMS)?
            .mapped(|row| row.get(0))
            .collect::<rusqlite::Result<Vec<i64>>>()
    })?;
    let mut delete_stmt = txn.prepare_cached("DELETE FROM cids WHERE id = ?")?;
    for (i, id) in ids.iter().enumerate() {
        if i >= min_blocks && t0.elapsed() > max_duration {
            return Ok(false);
        }
        trace!("deleting id {}", id);
        delete_stmt.execute(&[id])?;
    }
    Ok(true)
}

fn count_orphaned(txn: &Transaction) -> rusqlite::Result<u32> {
    let res = txn
        .prepare_cached(
            r#"
SELECT COUNT(block_id) FROM blocks
WHERE
    block_id NOT IN (SELECT id FROM cids);
        "#,
        )?
        .query_row(NO_PARAMS, |row| row.get(0))?;
    Ok(res)
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
fn incremental_delete_orphaned(
    txn: &Transaction,
    min_blocks: usize,
    max_duration: Duration,
) -> rusqlite::Result<bool> {
    let t0 = Instant::now();
    let ids: Vec<i64> = log_execution_time("determine_orphaned", Duration::from_secs(1), || {
        txn.prepare_cached("SELECT block_id FROM blocks WHERE block_id NOT IN (SELECT id FROM cids)")?
            .query(NO_PARAMS)?
            .mapped(|row| row.get(0))
            .collect::<rusqlite::Result<_>>()
    })?;
    let mut delete_stmt = txn.prepare_cached("DELETE FROM blocks WHERE block_id = ?")?;
    for (i, id) in ids.iter().enumerate() {
        let dt = t0.elapsed();
        if i >= min_blocks && dt > max_duration {
            info!(
                "stopped incremental delete after {}us and {} blocks",
                dt.as_micros(),
                i
            );
            return Ok(false);
        }
        trace!("deleting block for id {}", id);
        delete_stmt.execute(&[id])?;
    }
    Ok(true)
}

fn delete_temp_alias(txn: &Transaction, alias: i64) -> rusqlite::Result<()> {
    txn.prepare_cached("DELETE FROM temp_aliases WHERE alias = ?")?
        .execute(&[alias])?;
    Ok(())
}

pub(crate) fn add_block<C: ToSql>(
    txn: &Transaction,
    key: &C,
    data: &[u8],
    links: impl IntoIterator<Item = C>,
    alias: Option<&TempAlias>,
) -> rusqlite::Result<bool> {
    let id = get_or_create_id(&txn, &key)?;
    let block_exists = txn
        .prepare_cached("SELECT 1 FROM blocks WHERE block_id = ?")?
        .query_row(&[id], |_| Ok(()))
        .optional()?
        .is_some();
    // create a temporary alias for the block, even if it already exists
    if let Some(alias) = alias {
        let alias_id = alias.id.load(Ordering::SeqCst);
        if alias_id > 0 {
            txn.prepare_cached(
                "INSERT OR IGNORE INTO temp_aliases (alias, block_id) VALUES (?, ?)",
            )?
            .execute(&[alias_id, id])?;
        } else {
            // since we are not using an autoincrement column, this will reuse ids.
            // I think this is safe, but is it really? deserves some thought.
            let alias_id: i64 = txn
                .prepare_cached("SELECT COALESCE(MAX(alias), 1) + 1 FROM temp_aliases")?
                .query_row(NO_PARAMS, |row| row.get(0))?;
            txn.prepare_cached("INSERT INTO temp_aliases (alias, block_id) VALUES (?, ?)")?
                .execute(&[alias_id, id])?;
            alias.id.store(alias_id, Ordering::SeqCst);
        }
    }
    if !block_exists {
        txn.prepare_cached("INSERT INTO blocks (block_id, block) VALUES (?, ?)")?
            .execute(params![id, &data])?;

        let mut insert_ref =
            txn.prepare_cached("INSERT INTO refs (parent_id, child_id) VALUES (?,?)")?;
        for link in links {
            let child_id: i64 = get_or_create_id(&txn, link)?;
            insert_ref.execute(params![id, child_id])?;
        }
    }
    Ok(true)
}

/// Get a block
fn get_block(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<Option<Vec<u8>>> {
    let id = get_id(&txn, cid)?;
    Ok(if let Some(id) = id {
        txn.prepare_cached("SELECT block FROM blocks WHERE block_id = ?")?
            .query_row(&[id], |row| row.get(0))
            .optional()?
    } else {
        None
    })
}

/// Check if we have a block
fn has_block(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<bool> {
    Ok(txn
        .prepare_cached(
            "SELECT 1 FROM blocks, cids WHERE blocks.block_id = cids.id AND cids.cid = ?",
        )?
        .query_row(&[cid], |_| Ok(()))
        .optional()?
        .is_some())
}

/// Check if we have a cid
fn has_cid(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<bool> {
    Ok(txn
        .prepare_cached("SELECT 1 FROM cids WHERE cids.cid = ?")?
        .query_row(&[cid], |_| Ok(()))
        .optional()?
        .is_some())
}

fn get_ancestors(txn: &Transaction, id: i64) -> rusqlite::Result<BTreeSet<i64>> {
    let mut res = BTreeSet::<i64>::new();
    let mut stmt = txn.prepare_cached(
        r#"
WITH RECURSIVE
    ancestor_of(id) AS
    (
        -- non recursive part - simply look up the immediate parents
        SELECT parent_id FROM refs WHERE child_id=?
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT parent_id FROM refs JOIN ancestor_of WHERE ancestor_of.id=refs.child_id
    )
SELECT DISTINCT id FROM ancestor_of;
"#,
    )?;
    let mut rows = stmt.query(&[id])?;
    while let Some(row) = rows.next()? {
        res.insert(row.get(0)?);
    }
    Ok(res)
}

/// get the descendants of an cid.
/// This just uses the refs table, so it does not ensure that we actually have data for each cid.
/// The value itself is included.
fn get_descendants<C: ToSql + FromSql>(txn: &Transaction, cid: C) -> rusqlite::Result<Vec<C>> {
    let res = txn
        .prepare_cached(
            r#"
WITH RECURSIVE
    descendant_of(id) AS
    (
        SELECT id FROM cids WHERE cid = ?
        UNION ALL
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    ),
    descendant_ids as (
        SELECT DISTINCT id FROM descendant_of
    )
    -- retrieve corresponding cids - this is a set because of select distinct
    SELECT cid from cids,descendant_ids WHERE cids.id = descendant_ids.id;
"#,
        )?
        .query(&[cid])?
        .mapped(|row| row.get(0))
        .collect::<rusqlite::Result<Vec<C>>>()?;
    Ok(res)
}

/// get the set of descendants of an id for which we do not have the data yet.
/// The value itself is included.
/// It is safe to call this method for a cid we don't have yet.
fn get_missing_blocks<C: ToSql + FromSql>(txn: &Transaction, cid: C) -> rusqlite::Result<Vec<C>> {
    let id = get_or_create_id(&txn, cid)?;
    let res = txn.prepare_cached(
        r#"
WITH     RECURSIVE
    -- find descendants of cid, including the id of the cid itself
    descendant_of(id) AS (
        SELECT ?
        UNION ALL
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    ),
    -- find orphaned ids
    orphaned_ids as (
      SELECT DISTINCT id FROM descendant_of LEFT JOIN blocks ON descendant_of.id = blocks.block_id WHERE blocks.block_id IS NULL
    )
    -- retrieve corresponding cids - this is a set because of select distinct
SELECT cid from cids,orphaned_ids WHERE cids.id = orphaned_ids.id;
"#,
    )?
        .query(&[id])?
        .mapped(|row| row.get(0))
        .collect::<rusqlite::Result<Vec<C>>>()?;
    Ok(res)
}

fn get_cids<C: FromSql>(txn: &Transaction) -> rusqlite::Result<Vec<C>> {
    Ok(txn
        .prepare_cached(r#"SELECT cid FROM cids"#)?
        .query(NO_PARAMS)?
        .mapped(|row| row.get(0))
        .collect::<rusqlite::Result<Vec<C>>>()?)
}

fn init_db(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.execute_batch(INIT)?;
    assert!(conn.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?);
    Ok(())
}

/// execute a statement in a write transaction
pub(crate) fn in_txn<T>(
    conn: &mut Connection,
    f: impl FnOnce(&Transaction) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let txn = conn.transaction()?;
    let result = f(&txn);
    if result.is_ok() {
        txn.commit()?;
    }
    result
}

/// execute a statement in a readonly transaction
/// nested transactions are not allowed here.
pub(crate) fn in_ro_txn<T>(
    conn: &Connection,
    f: impl FnOnce(&Transaction) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let txn = conn.unchecked_transaction()?;
    let result = f(&txn);
    result
}

pub trait Block<C> {
    type I: Iterator<Item = C>;
    fn cid(&self) -> C;
    fn data(&self) -> &[u8];
    fn links(&self) -> Self::I;
}

impl<C: ToSql + FromSql> BlockStore<C> {
    pub fn memory() -> rusqlite::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
            _c: PhantomData,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
            _c: PhantomData,
        })
    }

    pub fn alias(&mut self, name: &[u8], key: Option<&C>) -> rusqlite::Result<()> {
        in_txn(&mut self.conn, |txn| {
            if let Some(key) = key {
                let id = get_or_create_id(txn, key)?;
                txn.prepare_cached("REPLACE INTO aliases (name, block_id) VALUES (?, ?)")?
                    .execute(params![name, id])?;
            } else {
                txn.prepare_cached("DELETE FROM aliases WHERE name = ?")?
                    .execute(&[name])?;
            }
            Ok(())
        })
    }

    pub fn get_block(&self, key: &C) -> rusqlite::Result<Option<Vec<u8>>> {
        in_ro_txn(&self.conn, |txn| Ok(get_block(txn, key)?))
    }

    pub fn has_block(&self, key: &C) -> rusqlite::Result<bool> {
        in_ro_txn(&self.conn, |txn| Ok(has_block(txn, key)?))
    }

    pub fn has_cid(&self, key: &C) -> rusqlite::Result<bool> {
        in_ro_txn(&self.conn, |txn| Ok(has_cid(txn, key)?))
    }

    pub fn add_block(
        &mut self,
        key: &C,
        data: &[u8],
        links: impl IntoIterator<Item = C>,
        alias: Option<&TempAlias>,
    ) -> rusqlite::Result<bool> {
        in_txn(&mut self.conn, |txn| {
            Ok(add_block(txn, key, data, links, alias)?)
        })
    }

    pub fn add_blocks(
        &mut self,
        blocks: impl IntoIterator<Item = impl Block<C>>,
        alias: Option<&TempAlias>,
    ) -> rusqlite::Result<()> {
        in_txn(&mut self.conn, move |txn| {
            for block in blocks.into_iter() {
                add_block(txn, &block.cid(), block.data(), block.links(), alias)?;
            }
            Ok(())
        })
    }

    pub fn incremental_gc(
        &mut self,
        min_blocks: usize,
        max_duration: Duration,
    ) -> rusqlite::Result<bool> {
        // atomically grab the expired_temp_aliases until now
        let expired_temp_aliases = {
            let mut result = Vec::new();
            std::mem::swap(
                self.expired_temp_aliases.lock().unwrap().deref_mut(),
                &mut result,
            );
            result
        };
        log_execution_time("gc", Duration::from_secs(1), || {
            in_txn(&mut self.conn, move |txn| {
                // get rid of dropped temp aliases, this should be fast
                for id in expired_temp_aliases {
                    delete_temp_alias(txn, id)?;
                }
                incremental_gc(&txn, min_blocks, max_duration)
            })
        })
    }

    pub fn incremental_delete_orphaned(
        &mut self,
        min_blocks: usize,
        max_duration: Duration,
    ) -> rusqlite::Result<bool> {
        log_execution_time("delete_orphaned", Duration::from_millis(100), || {
            in_txn(&mut self.conn, move |txn| {
                Ok(incremental_delete_orphaned(txn, min_blocks, max_duration)?)
            })
        })
    }

    pub fn get_missing_blocks(&self, cid: C) -> rusqlite::Result<Vec<C>> {
        log_execution_time("get_missing_blocks", Duration::from_millis(10), || {
            in_ro_txn(&self.conn, move |txn| {
                let result = get_missing_blocks(txn, cid)?;
                Ok(result)
            })
        })
    }

    pub fn get_descendants(&self, cid: C) -> rusqlite::Result<Vec<C>> {
        in_ro_txn(&self.conn, move |txn| Ok(get_descendants(txn, cid)?))
    }

    pub fn get_block_count(&self) -> rusqlite::Result<u64> {
        Ok(u64::try_from(in_ro_txn(&self.conn, move |txn| get_block_count(txn))?).unwrap())
    }

    pub fn get_block_size(&self) -> rusqlite::Result<u64> {
        Ok(u64::try_from(in_ro_txn(&self.conn, move |txn| get_block_size(txn))?).unwrap())
    }

    pub fn get_cids(&self) -> rusqlite::Result<Vec<C>> {
        in_ro_txn(&self.conn, |txn| get_cids(txn))
    }

    pub fn count_orphaned(&self) -> rusqlite::Result<u32> {
        in_ro_txn(&self.conn, move |txn| Ok(count_orphaned(txn)?))
    }

    pub fn create_temp_alias(&self) -> rusqlite::Result<TempAlias> {
        Ok(TempAlias {
            id: AtomicI64::new(0),
            expired_temp_aliases: self.expired_temp_aliases.clone(),
        })
    }
}

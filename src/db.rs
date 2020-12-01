//! A sqlite based block store for content-addressed data that tries to do as much as possible
//! in the database.
//!
//! Tables:
//! cids: mapping from cid (blob < 64 bytes) to id (u64)
//! refs: m:n mapping from block ids to their children
//! blocks: the actual data for blocks, keyed by block id
//!    cids can exist in the system without having data associated with them!
//! atime: table for a monotonic index that tracks block access time, for the gc grace period mechanism
//! alias: table that contains named pins for roots of graphs that should not be deleted by gc
//!    you can alias incomplete or in fact non-existing data. It is not necessary for a pinned dag
//!    to be complete.
use rusqlite::{
    config::DbConfig, params, types::FromSql, Connection, OptionalExtension, ToSql, Transaction,
    NO_PARAMS,
};
use std::{collections::BTreeSet, marker::PhantomData, path::Path};
use std::{convert::TryFrom, time::Instant};

const INIT: &'static str = r#"
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
-- PRAGMA synchronous = FULL;
PRAGMA page_size = 4096;
-- PRAGMA page_size = 8192;
-- PRAGMA page_size = 16384;
-- PRAGMA synchronous = OFF;
-- PRAGMA journal_mode = MEMORY;

CREATE TABLE IF NOT EXISTS cids (
    id INTEGER PRIMARY KEY,
    cid BLOB UNIQUE
);

CREATE TABLE IF NOT EXISTS refs (
    parent_id INTEGER,
    child_id INTEGER,
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
    block BLOB
);

-- for some reason this index is required to make the on delete cascade
-- fast, despite block_id being a PRIMARY_KEY.
CREATE INDEX IF NOT EXISTS idx_blocks_block_id
ON blocks (block_id);

CREATE TABLE IF NOT EXISTS atime (
    atime INTEGER PRIMARY KEY AUTOINCREMENT,
    block_id INTEGER UNIQUE,
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_atime_block_id
ON atime (block_id);

CREATE TABLE IF NOT EXISTS aliases (
    name blob UNIQUE,
    block_id INTEGER,
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aliases_block_id
ON aliases (block_id);
"#;

pub struct BlockStore<C> {
    conn: Connection,
    _c: PhantomData<C>,
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

fn update_atime(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<()> {
    if let Some(id) = get_id(txn, cid)? {
        let mut stmt = txn.prepare_cached("REPLACE INTO atime (block_id) VALUES (?)")?;
        stmt.execute(&[id])?;
    }
    Ok(())
}

fn perform_gc_old(txn: &Transaction, grace_atime: i64) -> rusqlite::Result<()> {
    // delete all ids that have neither a parent nor are aliased
    loop {
        let rows = txn
            .prepare_cached(
                r#"
    DELETE FROM
        cids
    WHERE
        (NOT EXISTS(SELECT 1 FROM refs WHERE child_id = id)) AND
        (NOT EXISTS(SELECT 1 FROM aliases WHERE block_id = id)) AND
        (SELECT atime FROM atime WHERE atime.block_id = id) < ?
        LIMIT 10000;
"#,
            )?
            .execute(&[grace_atime])?;
        println!("collected {} rows", rows);
        let cids: i64 = txn.query_row("SELECT COUNT(*) FROM cids", NO_PARAMS, |row| row.get(0))?;
        println!("remaining {}", cids);
        if rows == 0 {
            break;
        }
    }
    Ok(())
}

fn perform_gc(txn: &Transaction, grace_atime: i64) -> rusqlite::Result<()> {
    // delete all ids that have neither a parent nor are aliased
    txn.prepare_cached(
        r#"
WITH RECURSIVE
    descendant_of(id) AS
    (
        -- non recursive part - simply look up the immediate children
        SELECT block_id FROM aliases
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    )
DELETE FROM
    cids
WHERE
    id NOT IN (SELECT id FROM descendant_of) AND
    (SELECT atime FROM atime WHERE atime.block_id = id) < ?;
        "#,
    )?
    .execute(&[grace_atime])?;
    Ok(())
}

fn perform_gc_2(txn: &Transaction, grace_atime: i64) -> rusqlite::Result<()> {
    // find all ids that have neither a parent nor are aliased
    let mut id_query = txn.prepare_cached(
        r#"
WITH RECURSIVE
    descendant_of(id) AS
    (
        -- non recursive part - simply look up the immediate children
        SELECT block_id FROM aliases
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    )
SELECT id FROM
    cids
WHERE
    id NOT IN (SELECT id FROM descendant_of) AND
    (SELECT atime FROM atime WHERE atime.block_id = id) < ?;
        "#,
    )?;
    let mut delete_stmt = txn.prepare_cached("DELETE FROM cids WHERE id = ?")?;
    let t0 = Instant::now();
    let mut rows = id_query.query(&[grace_atime])?;
    let mut ids: Vec<i64> = Vec::new();
    while let Some(row) = rows.next()? {
        ids.push(row.get(0)?);
    }
    let dt = t0.elapsed();
    println!("figuring out the ids to delete took {}", dt.as_secs_f64());
    for (i, id) in ids.iter().enumerate() {
        if i % 10000 == 0 {
            println!("deleted 10000 blocks {}", i);
        }
        delete_stmt.execute(&[id])?;
    }
    Ok(())
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

fn delete_orphaned(txn: &Transaction) -> rusqlite::Result<()> {
    txn.prepare_cached(
        r#"
DELETE FROM
    blocks
WHERE
    block_id NOT IN (SELECT id FROM cids) LIMIT 10000;
        "#,
    )?
    .execute(NO_PARAMS)?;
    Ok(())
}

fn add_block<C: ToSql>(
    txn: &Transaction,
    key: &C,
    data: &[u8],
    links: impl IntoIterator<Item = C>,
) -> rusqlite::Result<bool> {
    let id = get_or_create_id(&txn, &key)?;
    let block_exists = txn
        .prepare_cached("SELECT 1 FROM blocks WHERE block_id = ?")?
        .query_row(&[id], |_| Ok(()))
        .optional()?
        .is_some();
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
    // update atime
    txn.prepare_cached("REPLACE INTO atime (block_id) VALUES (?)")?
        .execute(&[id])?;
    Ok(true)
}

fn get_current_atime(txn: &Transaction) -> rusqlite::Result<Option<i64>> {
    txn.prepare_cached("SELECT seq FROM sqlite_sequence WHERE name='atime'")?
        .query_row(NO_PARAMS, |row| row.get(0))
        .optional()
}

/// Get a block, and update its atime
fn get_block(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<Option<Vec<u8>>> {
    let id = get_id(&txn, cid)?;
    Ok(if let Some(id) = id {
        // txn.prepare_cached("REPLACE INTO atime (block_id) VALUES (?)")?
        //     .execute(&[id])?;
        txn.prepare_cached("SELECT block FROM blocks WHERE block_id = ?")?
            .query_row(&[id], |row| row.get(0))
            .optional()?
    } else {
        None
    })
}

/// Check if we have a block, without updating atime.
fn has_block(txn: &Transaction, cid: impl ToSql) -> rusqlite::Result<bool> {
    Ok(txn
        .prepare_cached(
            "SELECT 1 FROM blocks, cids WHERE blocks.block_id = cids.id AND cids.cid = ?",
        )?
        .query_row(&[cid], |_| Ok(()))
        .optional()?
        .is_some())
}

/// Check if we have a cid, without updating atime.
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
    let mut res = Vec::<C>::new();
    let mut stmt = txn.prepare_cached(
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
    )?;
    let mut rows = stmt.query(&[cid])?;
    while let Some(row) = rows.next()? {
        res.push(row.get(0)?);
    }
    Ok(res)
}

/// get the set of descendants of an id for which we do not have the data yet.
/// The value itself is included.
/// It is safe to call this method for a cid we don't have yet.
fn get_missing_blocks<C: ToSql + FromSql>(txn: &Transaction, cid: C) -> rusqlite::Result<Vec<C>> {
    let id = get_or_create_id(&txn, cid)?;
    let mut stmt = txn.prepare_cached(
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
    )?;
    let mut res = Vec::new();
    let mut rows = stmt.query(&[id])?;
    while let Some(row) = rows.next()? {
        res.push(row.get(0)?);
    }
    Ok(res)
}

fn get_cids<C: FromSql>(txn: &Transaction) -> rusqlite::Result<Vec<C>> {
    let mut stmt = txn.prepare_cached(r#"SELECT cid FROM cids"#)?;
    let mut rows = stmt.query(NO_PARAMS)?;
    let mut res = Vec::new();
    while let Some(row) = rows.next()? {
        res.push(row.get(0)?);
    }
    Ok(res)
}

fn init_db(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.execute_batch(INIT)?;
    assert!(conn.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?);
    Ok(())
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
            _c: PhantomData,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
            _c: PhantomData,
        })
    }

    pub fn alias(&mut self, name: &[u8], key: Option<&C>) -> rusqlite::Result<()> {
        self.in_txn(|txn| {
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
        self.in_ro_txn(|txn| Ok(get_block(txn, key)?))
    }

    pub fn has_block(&self, key: &C) -> rusqlite::Result<bool> {
        self.in_ro_txn(|txn| Ok(has_block(txn, key)?))
    }

    pub fn has_cid(&self, key: &C) -> rusqlite::Result<bool> {
        self.in_ro_txn(|txn| Ok(has_cid(txn, key)?))
    }

    pub fn add_block(
        &mut self,
        key: &C,
        data: &[u8],
        links: impl IntoIterator<Item = C>,
    ) -> rusqlite::Result<bool> {
        self.in_txn(|txn| Ok(add_block(txn, key, data, links)?))
    }

    pub fn add_blocks(
        &mut self,
        blocks: impl IntoIterator<Item = impl Block<C>>,
    ) -> rusqlite::Result<()> {
        self.in_txn(move |txn| {
            for block in blocks.into_iter() {
                add_block(txn, &block.cid(), block.data(), block.links())?;
            }
            Ok(())
        })
    }

    pub fn gc(&mut self, grace_atime: i64) -> rusqlite::Result<Option<i64>> {
        self.in_txn(move |txn| {
            perform_gc_2(&txn, grace_atime)?;
            Ok(get_current_atime(txn)?)
        })
    }

    pub fn delete_orphaned(&mut self) -> rusqlite::Result<()> {
        self.in_txn(move |txn| Ok(delete_orphaned(txn)?))
    }

    pub fn get_missing_blocks(&self, cid: C) -> rusqlite::Result<Vec<C>> {
        self.in_ro_txn(move |txn| {
            let result = get_missing_blocks(txn, cid)?;
            Ok(result)
        })
    }

    pub fn get_descendants(&self, cid: C) -> rusqlite::Result<Vec<C>> {
        self.in_ro_txn(move |txn| Ok(get_descendants(txn, cid)?))
    }

    pub fn get_block_count(&self) -> rusqlite::Result<u64> {
        Ok(u64::try_from(self.in_ro_txn(move |txn| get_block_count(txn))?).unwrap())
    }

    pub fn get_block_size(&self) -> rusqlite::Result<u64> {
        Ok(u64::try_from(self.in_ro_txn(move |txn| get_block_size(txn))?).unwrap())
    }

    pub fn get_cids(&self) -> rusqlite::Result<Vec<C>> {
        self.in_ro_txn(|txn| get_cids(txn))
    }

    pub fn count_orphaned(&self) -> rusqlite::Result<u32> {
        self.in_ro_txn(move |txn| Ok(count_orphaned(txn)?))
    }

    /// execute a statement in a write transaction
    fn in_txn<T>(
        &mut self,
        f: impl FnOnce(&Transaction) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let txn = self.conn.transaction()?;
        let result = f(&txn);
        if result.is_ok() {
            txn.commit()?;
        }
        result
    }

    /// execute a statement in a readonly transaction
    /// nested transactions are not allowed here.
    fn in_ro_txn<T>(
        &self,
        f: impl FnOnce(&Transaction) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let txn = self.conn.unchecked_transaction()?;
        let result = f(&txn);
        result
    }
}

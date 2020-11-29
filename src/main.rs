// TODO: make sure cid -> id mapping is also created if there is no block!?
use anyhow::anyhow;
use cid::Cid;
use rusqlite::{
    config::DbConfig,
    params,
    types::ToSqlOutput,
    types::{FromSql, FromSqlError, ValueRef},
    Connection, OptionalExtension, ToSql, Transaction, NO_PARAMS,
};
use std::{collections::BTreeSet, convert::TryFrom, io::Cursor, path::Path};
use tracing::*;

const INIT2: &'static str = r#"
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
-- PRAGMA synchronous = FULL;
PRAGMA page_size = 4096;
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
    block BLOB,
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
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

CREATE INDEX idx_atime_block_id
ON atime (block_id);

CREATE TABLE IF NOT EXISTS aliases (
    name blob UNIQUE,
    block_id INTEGER,
    CONSTRAINT fk_block_id
      FOREIGN KEY (block_id)
      REFERENCES cids(id)
      ON DELETE CASCADE
);

CREATE INDEX idx_aliases_block_id
ON aliases (block_id);
"#;

struct CidBytes {
    size: u8,
    data: [u8; 63],
}

impl CidBytes {
    fn len(&self) -> usize {
        self.size as usize
    }
}

impl AsRef<[u8]> for CidBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data[0..self.len()]
    }
}

impl Default for CidBytes {
    fn default() -> Self {
        Self {
            size: 0,
            data: [0; 63],
        }
    }
}

impl TryFrom<&Cid> for CidBytes {
    type Error = cid::Error;

    fn try_from(value: &Cid) -> Result<Self, Self::Error> {
        let mut res = Self::default();
        value.write_bytes(&mut res)?;
        Ok(res)
    }
}

impl TryFrom<&CidBytes> for Cid {
    type Error = cid::Error;

    fn try_from(value: &CidBytes) -> Result<Self, Self::Error> {
        Cid::read_bytes(Cursor::new(value.as_ref()))
    }
}

impl TryFrom<&[u8]> for CidBytes {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut res = CidBytes::default();
        if value.len() < 64 {
            res.size = value.len() as u8;
            res.data[0..value.len()].copy_from_slice(value);
            Ok(res)
        } else {
            Err(anyhow!("too big"))
        }
    }
}

impl ToSql for CidBytes {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(self.as_ref())))
    }
}

impl FromSql for CidBytes {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        Ok(CidBytes::try_from(bytes).map_err(|_| FromSqlError::InvalidType)?)
    }
}

impl std::io::Write for CidBytes {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.len();
        let cap: usize = 63 - len;
        let n = cap.min(buf.len());
        &self.data[len..len + n].copy_from_slice(&buf[0..n]);
        self.size += n as u8;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct BlockStore {
    conn: Connection,
}

fn get_id(txn: &Transaction, cid: impl AsRef<[u8]>) -> rusqlite::Result<Option<i64>> {
    txn.prepare_cached("SELECT id FROM cids WHERE cid=?")?
        .query_row(&[cid.as_ref()], |row| row.get(0))
        .optional()
}

fn get_or_create_id(txn: &Transaction, cid: impl AsRef<[u8]>) -> rusqlite::Result<i64> {
    let id = get_id(&txn, cid.as_ref())?;
    Ok(if let Some(id) = id {
        id
    } else {
        txn.prepare_cached("INSERT INTO cids (cid) VALUES (?)")?
            .execute(&[cid.as_ref()])?;
        txn.last_insert_rowid()
    })
}

fn update_atime(txn: &Transaction, cid: impl AsRef<[u8]>) -> rusqlite::Result<()> {
    if let Some(id) = get_id(txn, cid)? {
        let mut stmt = txn.prepare_cached("REPLACE INTO atime (block_id) VALUES (?)")?;
        stmt.execute(&[id])?;
    }
    Ok(())
}

fn perform_gc(txn: &Transaction, grace_atime: i64) -> anyhow::Result<()> {
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

fn perform_gc_2(txn: &Transaction, grace_atime: i64) -> anyhow::Result<()> {
    // delete all ids that have neither a parent nor are aliased
    loop {
        let rows = txn
            .prepare_cached(
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
    (SELECT atime FROM atime WHERE atime.block_id = id) < ? LIMIT 10000;
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

fn add_block(txn: &Transaction, key: &[u8], data: &[u8], links: &[&[u8]]) -> anyhow::Result<bool> {
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
    txn.prepare_cached("SELECT MAX(atime) FROM atime")?
        .query_row(NO_PARAMS, |row| row.get(0))
        .optional()
}

/// Get a block, and update its atime
fn get_block(txn: &Transaction, cid: &[u8]) -> rusqlite::Result<Option<Vec<u8>>> {
    let id = get_id(&txn, cid)?;
    Ok(if let Some(id) = id {
        txn.prepare_cached("REPLACE INTO atime (block_id) VALUES (?)")?
            .execute(&[id])?;
        txn.prepare_cached("SELECT block FROM blocks WHERE block_id = ?")?
            .query_row(&[id], |row| row.get(0))
            .optional()?
    } else {
        None
    })
}

/// Check if we have a block, without updating atime.
fn has_block(txn: &Transaction, cid: impl AsRef<[u8]>) -> rusqlite::Result<bool> {
    Ok(txn
        .prepare_cached(
            "SELECT 1 FROM blocks, cids WHERE blocks.block_id = cids.id AND cids.cid = ?",
        )?
        .query_row(&[cid.as_ref()], |_| Ok(()))
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

/// get the descendants of an id.
/// This just uses the refs table, so it does not ensure that we actually have data for each id.
/// The value itself is included.
fn get_descendants(txn: &Transaction, id: i64) -> rusqlite::Result<BTreeSet<i64>> {
    let mut res = BTreeSet::<i64>::new();
    let mut stmt = txn.prepare_cached(
        r#"
WITH RECURSIVE
    descendant_of(id) AS
    (
        -- non recursive part - simply look up the immediate children
        SELECT ?
        UNION ALL
        -- recursive part - look up parents of all returned ids
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    )
SELECT DISTINCT id FROM descendant_of;
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
fn get_descendants_of_cid(
    txn: &Transaction,
    cid: impl AsRef<[u8]>,
) -> rusqlite::Result<BTreeSet<Vec<u8>>> {
    let mut res = BTreeSet::<Vec<u8>>::new();
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
    SELECT cid from cids,descendant_ids WHERE cids.id = descendant_ids.id;
"#,
    )?;
    let mut rows = stmt.query(&[cid.as_ref()])?;
    while let Some(row) = rows.next()? {
        res.insert(row.get(0)?);
    }
    Ok(res)
}

/// get the set of descendants of an id for which we do not have the data yet.
/// The value itself is included.
fn get_missing_blocks(
    txn: &Transaction,
    cid: impl AsRef<[u8]>,
) -> rusqlite::Result<BTreeSet<Vec<u8>>> {
    let mut res = BTreeSet::new();
    let mut stmt = txn.prepare_cached(
        r#"
WITH RECURSIVE
    -- find descendants of cid, including the id of the cid itself
    descendant_of(id) AS (
        SELECT id FROM cids WHERE cid = ?
        UNION ALL
        SELECT DISTINCT child_id FROM refs JOIN descendant_of WHERE descendant_of.id=refs.parent_id
    ),
    -- find orphaned ids
    orphaned_ids as (
      SELECT DISTINCT id FROM descendant_of LEFT JOIN blocks ON descendant_of.id = blocks.block_id WHERE blocks.block_id IS NULL
    )
    -- retrieve corresponding cids
SELECT cid from cids,orphaned_ids WHERE cids.id = orphaned_ids.id;
"#,
    )?;
    let mut rows = stmt.query(&[cid.as_ref()])?;
    while let Some(row) = rows.next()? {
        res.insert(row.get(0)?);
    }
    Ok(res)
}

struct Block {
    cid: Vec<u8>,
    data: Vec<u8>,
    links: Vec<Vec<u8>>,
}

impl Block {
    fn new(cid: Vec<u8>, data: Vec<u8>, links: Vec<Vec<u8>>) -> Self {
        Self { cid, data, links }
    }
}

impl BlockStore {
    fn memory() -> anyhow::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        BlockStore::init_db(&mut conn)?;
        Ok(Self { conn })
    }

    fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut conn = Connection::open(path)?;
        BlockStore::init_db(&mut conn)?;
        Ok(Self { conn })
    }

    fn init_db(conn: &mut Connection) -> anyhow::Result<()> {
        conn.execute_batch(INIT2)?;
        assert!(conn.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?);
        Ok(())
    }

    fn alias(&mut self, name: &[u8], key: Option<&[u8]>) -> anyhow::Result<()> {
        let txn = self.conn.transaction()?;
        if let Some(key) = key {
            let id = get_id(&txn, key)?;
            txn.prepare_cached("REPLACE INTO aliases (name, block_id) VALUES (?, ?)")?
                .execute(params![name, id])?;
        } else {
            txn.prepare_cached("DELETE FROM ALIASES WHERE name = ?")?
                .execute(&[name])?;
        }
        txn.commit()?;
        Ok(())
    }

    /// execute a statement in a transaction
    fn in_txn<T>(
        &mut self,
        f: impl FnOnce(&Transaction) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let txn = self.conn.transaction()?;
        let result = f(&txn);
        if result.is_ok() {
            txn.commit()?;
        }
        result
    }

    fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.in_txn(|txn| Ok(get_block(&txn, key)?))
    }

    fn has(&mut self, key: impl AsRef<[u8]>) -> anyhow::Result<bool> {
        self.in_txn(|txn| Ok(has_block(&txn, key)?))
    }

    fn add(&mut self, key: &[u8], data: &[u8], links: &[&[u8]]) -> anyhow::Result<bool> {
        self.in_txn(|txn| Ok(add_block(&txn, key, data, links)?))
    }

    fn add_blocks(&mut self, blocks: Vec<Block>) -> anyhow::Result<()> {
        self.in_txn(move |txn| {
            for block in blocks {
                let links = block.links.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
                add_block(&txn, &block.cid, &block.data, &links)?;
            }
            Ok(())
        })
    }

    fn gc(&mut self, grace_atime: i64) -> anyhow::Result<Option<i64>> {
        self.in_txn(move |txn| {
            perform_gc(&txn, grace_atime)?;
            Ok(get_current_atime(&txn)?)
        })
    }

    fn gc_2(&mut self, grace_atime: i64) -> anyhow::Result<Option<i64>> {
        self.in_txn(move |txn| {
            perform_gc_2(&txn, grace_atime)?;
            Ok(get_current_atime(&txn)?)
        })
    }

    fn get_ancestors(&mut self, cid: &[u8]) -> anyhow::Result<BTreeSet<i64>> {
        self.in_txn(move |txn| {
            Ok(if let Some(id) = get_id(&txn, cid)? {
                get_ancestors(&txn, id)?
            } else {
                Default::default()
            })
        })
    }

    fn get_descendants(&mut self, cid: &[u8]) -> anyhow::Result<BTreeSet<i64>> {
        self.in_txn(move |txn| {
            Ok(if let Some(id) = get_id(&txn, cid)? {
                get_descendants(&txn, id)?
            } else {
                Default::default()
            })
        })
    }

    fn get_missing_blocks(&mut self, cid: impl AsRef<[u8]>) -> anyhow::Result<BTreeSet<Vec<u8>>> {
        self.in_txn(move |txn| {
            let result = get_missing_blocks(&txn, cid)?;
            Ok(result)
        })
    }

    fn get_descendants_of_cid(
        &mut self,
        cid: impl AsRef<[u8]>,
    ) -> anyhow::Result<BTreeSet<Vec<u8>>> {
        self.in_txn(move |txn| Ok(get_descendants_of_cid(&txn, cid)?))
    }
}

fn build_chain(prefix: &str, n: usize) -> anyhow::Result<(Vec<u8>, Vec<Block>)> {
    assert!(n > 0);
    let mut blocks = Vec::with_capacity(n);
    let mk_node = |i: usize| format!("{}-{}", prefix, i);
    let mk_data = |i: usize| format!("{}-{}-data", prefix, i);
    let mut prev: Option<String> = None;
    for i in 0..n {
        let node = mk_node(i);
        let data = mk_data(i);
        let links = prev
            .iter()
            .map(|x| x.as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();
        blocks.push(Block::new(
            node.as_bytes().to_vec(),
            data.as_bytes().to_vec(),
            links,
        ));
        prev = Some(node);
    }
    Ok((prev.unwrap().as_bytes().to_vec(), blocks))
}

fn build_tree_0(
    prefix: &str,
    branch: u64,
    depth: u64,
    blocks: &mut Vec<Block>,
) -> anyhow::Result<Vec<u8>> {
    let node = format!("{}", prefix).as_bytes().to_vec();
    let data_size = if depth == 0 { 1024 * 2 } else { 1024 };
    let mut data = vec![0u8; data_size];
    data[0..node.len()].copy_from_slice(node.as_ref());
    let children = if depth == 0 {
        Vec::new()
    } else {
        let mut children = Vec::new();
        for i in 0..branch {
            let cid = build_tree_0(&format!("{}-{}", prefix, i), branch, depth - 1, blocks)?;
            children.push(cid);
        }
        children
    };
    let block = Block::new(node, data, children);
    let cid = block.cid.clone();
    blocks.push(block);
    Ok(cid)
}

fn build_tree(prefix: &str, branch: u64, depth: u64) -> anyhow::Result<(Vec<u8>, Vec<Block>)> {
    let mut tmp = Vec::new();
    let res = build_tree_0(prefix, branch, depth, &mut tmp)?;
    Ok((res, tmp))
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut store = BlockStore::open("test.sqlite")?;
    for i in 0..10 {
        println!("Adding filler tree {}", i);
        let (tree_root, tree_blocks) = build_tree(&format!("tree-{}", i), 10, 4)?;
        store.add_blocks(tree_blocks)?;
        if i % 2 == 0 {
            store.alias(
                &format!("tree-alias-{}", i).as_bytes(),
                Some(tree_root.as_ref()),
            )?;
        }
    }
    let (tree_root, tree_blocks) = build_tree("tree", 10, 4)?;
    let (list_root, list_blocks) = build_chain("chain", 10000)?;
    // for block in list_blocks {
    //     store.add(block.cid.as_ref(), block.data.as_ref(), &block.links.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
    // }
    // for block in tree_blocks {
    //     store.add(block.cid.as_ref(), block.data.as_ref(), &block.links.iter().map(|x| x.as_ref()).collect::<Vec<_>>())?;
    // }
    store.add_blocks(tree_blocks)?;
    store.add_blocks(list_blocks)?;
    // println!(
    //     "descendants of {:?} {:?}",
    //     tree_root,
    //     store.get_descendants(tree_root.as_ref())
    // );
    // println!(
    //     "descendants of {:?} {:?}",
    //     list_root,
    //     store.get_descendants(list_root.as_ref())
    // );
    store.add(b"a", b"adata", &[b"b", b"c"])?;
    println!("{:?}", store.get_missing_blocks(b"a")?);
    store.add(b"b", b"bdata", &[])?;
    store.add(b"c", b"cdata", &[])?;
    println!("{:?}", store.get_descendants_of_cid(b"a")?);
    store.add(b"d", b"ddata", &[b"b", b"c"])?;

    store.alias(b"source1", Some(b"a"))?;
    store.alias(b"source2", Some(b"d"))?;
    store.gc_2(100000000)?;
    // let atime = store.gc(100000)?;
    // println!("{:?}", atime);
    // println!("ancestors {:?}", store.get_ancestors(b"b"));
    // println!("descendants {:?}", store.get_descendants(b"a"));
    Ok(())
}

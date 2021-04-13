#![allow(clippy::many_single_char_names)]
use crate::{
    cache::CacheTracker,
    cache::InMemCacheTracker,
    cache::{SortByIdCacheTracker, SqliteCacheTracker},
    Block, BlockStore, Config, DbPath, OwnedBlock, SizeTargets,
};
use fnv::FnvHashSet;
use libipld::{
    cbor::DagCborCodec,
    cid::Cid,
    multihash::{Code, MultihashDigest},
};
use libipld::{prelude::*, DagCbor};
use rusqlite::{params, Connection};
use std::time::Duration;
use tempdir::TempDir;

#[derive(Debug, DagCbor)]
struct Node {
    links: Vec<Cid>,
    text: String,
}

impl Node {
    pub fn leaf(text: &str) -> Self {
        Self {
            links: Vec::new(),
            text: text.into(),
        }
    }

    pub fn branch(text: &str, links: impl IntoIterator<Item = Cid>) -> Self {
        Self {
            links: links.into_iter().collect(),
            text: text.into(),
        }
    }
}

enum SizeOrLinks {
    Size(usize),
    Links(Vec<Cid>),
}

impl From<Vec<Cid>> for SizeOrLinks {
    fn from(value: Vec<Cid>) -> Self {
        Self::Links(value)
    }
}

impl From<usize> for SizeOrLinks {
    fn from(value: usize) -> Self {
        Self::Size(value)
    }
}

/// creates a simple leaf block
fn block(name: &str) -> OwnedBlock {
    let ipld = Node::leaf(name);
    let bytes = DagCborCodec.encode(&ipld).unwrap();
    let hash = Code::Sha2_256.digest(&bytes);
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    OwnedBlock::new(Cid::new_v1(0x71, hash), bytes)
}

/// creates a block with some links
fn links(name: &str, children: Vec<&OwnedBlock>) -> OwnedBlock {
    let ipld = Node::branch(name, children.iter().map(|b| *b.cid()).collect::<Vec<_>>());
    let bytes = DagCborCodec.encode(&ipld).unwrap();
    let hash = Code::Sha2_256.digest(&bytes);
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    OwnedBlock::new(Cid::new_v1(0x71, hash), bytes)
}

/// creates a block with a min size
fn sized(name: &str, min_size: usize) -> OwnedBlock {
    let mut text = name.to_string();
    while text.len() < min_size {
        text += " ";
    }
    let ipld = Node::leaf(&text);
    let bytes = DagCborCodec.encode(&ipld).unwrap();
    let hash = Code::Sha2_256.digest(&bytes);
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    OwnedBlock::new(Cid::new_v1(0x71, hash), bytes)
}

/*fn pb(name: &str) -> Cid {
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    let hash = Code::Sha2_256.digest(name.as_bytes());
    Cid::new_v1(0x70, hash)
}*/

/// creates a block with the name "unpinned-<i>" and a size of 1000
fn unpinned(i: usize) -> OwnedBlock {
    sized(&format!("{}", i), 1000 - 16)
}

/// creates a block with the name "pinned-<i>" and a size of 1000
fn pinned(i: usize) -> OwnedBlock {
    sized(&format!("pinned-{}", i), 1000 - 16)
}

#[test]
fn insert_get() -> anyhow::Result<()> {
    let mut store = BlockStore::memory(Config::default())?;
    let b = block("b");
    let c = block("c");
    let a = links("a", vec![&b, &c, &c]);
    store.put_block(&a, None)?;
    // we should have all three cids
    assert!(store.has_cid(a.cid())?);
    assert!(store.has_cid(b.cid())?);
    assert!(store.has_cid(c.cid())?);
    // but only the first block
    assert!(store.has_block(a.cid())?);
    assert!(!store.has_block(b.cid())?);
    assert!(!store.has_block(c.cid())?);
    // check the data
    assert_eq!(store.get_block(a.cid())?, Some(a.data().to_vec()));
    // check descendants
    assert_eq!(
        store.get_descendants::<Vec<_>>(a.cid())?,
        vec![*a.cid(), *b.cid(), *c.cid()]
    );
    // check missing blocks - should be b and c
    assert_eq!(
        store.get_missing_blocks::<Vec<_>>(a.cid())?,
        vec![*b.cid(), *c.cid()]
    );
    // alias the root
    store.alias(b"alias1", Some(a.cid()))?;
    store.gc()?;
    // after gc, we shold still have the block
    assert!(store.has_block(a.cid())?);
    store.alias(b"alias1", None)?;
    store.gc()?;
    // after gc, we shold no longer have the block
    assert!(!store.has_block(a.cid())?);
    Ok(())
}

#[test]
fn incremental_insert() -> anyhow::Result<()> {
    let mut store = BlockStore::memory(Config::default())?;
    let b = block("b");
    let d = block("d");
    let e = block("e");
    let c = links("c", vec![&d, &e]);
    let a = links("a", vec![&b, &c]);
    // alias before even adding the block
    store.alias(b"alias1", Some(a.cid()))?;
    assert!(store.has_cid(a.cid())?);
    store.put_block(&a, None)?;
    store.gc()?;
    store.put_block(&c, None)?;
    store.gc()?;
    // we should have all five cids
    assert!(store.has_cid(a.cid())?);
    assert!(store.has_cid(b.cid())?);
    assert!(store.has_cid(c.cid())?);
    assert!(store.has_cid(d.cid())?);
    assert!(store.has_cid(e.cid())?);
    // but only blocks a and c
    assert!(store.has_block(a.cid())?);
    assert!(!store.has_block(b.cid())?);
    assert!(store.has_block(c.cid())?);
    assert!(!store.has_block(d.cid())?);
    assert!(!store.has_block(e.cid())?);
    // check the data
    assert_eq!(store.get_block(a.cid())?, Some(a.data().to_vec()));
    // check descendants
    assert_eq!(
        store.get_descendants::<FnvHashSet<_>>(a.cid())?,
        [a.cid(), b.cid(), c.cid(), d.cid(), e.cid()]
            .iter()
            .copied()
            .copied()
            .collect::<FnvHashSet<_>>()
    );
    // check missing blocks - should be b and c
    assert_eq!(
        store.get_missing_blocks::<FnvHashSet<_>>(a.cid())?,
        [b.cid(), d.cid(), e.cid()]
            .iter()
            .copied()
            .copied()
            .collect::<FnvHashSet<_>>()
    );
    // alias the root
    store.alias(b"alias1", Some(a.cid()))?;
    store.gc()?;
    // after gc, we shold still have the block
    assert!(store.has_block(a.cid())?);
    store.alias(b"alias1", Some(c.cid()))?;
    store.gc()?;
    assert!(!store.has_block(a.cid())?);
    assert!(store.has_block(c.cid())?);
    Ok(())
}

#[test]
fn size_targets() -> anyhow::Result<()> {
    // create a store with a non-empty size target to enable keeping non-pinned stuff around
    let mut store = BlockStore::memory(
        Config::default()
            .with_size_targets(SizeTargets::new(10, 10000))
            .with_cache_tracker(SortByIdCacheTracker),
    )?;

    // add some pinned stuff at the very beginning
    for i in 0..2 {
        let block = pinned(i);
        store.put_block(&block, None)?;
        store.alias(block.cid().to_bytes(), Some(&block.cid()))?;
    }

    // add data that is within the size targets
    for i in 0..8 {
        let block = unpinned(i);
        store.put_block(&block, None)?;
    }

    // check that gc does nothing
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);
    store.incremental_gc(5, Duration::from_secs(100000))?;
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);

    // add some more stuff to exceed the size targets
    for i in 8..13 {
        let block = unpinned(i);
        store.put_block(&block, None)?;
    }

    // check that gc gets triggered and removes min_blocks
    store.incremental_gc(10, Duration::from_secs(100000))?;
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);

    let cids = store.get_block_cids::<FnvHashSet<_>>()?;
    // check that the 2 pinned ones are still there despite being added first
    // and that only the 8 latest unpinned ones to be added remain
    let expected_cids = (0..2)
        .map(pinned)
        .chain((5..13).map(unpinned))
        .map(|block| *block.cid())
        .collect::<FnvHashSet<_>>();
    assert_eq!(cids, expected_cids);
    Ok(())
}

#[test]
fn in_mem_cache_tracker() -> anyhow::Result<()> {
    cache_test(InMemCacheTracker::new(|access, _| Some(access)))
}

#[test]
fn sqlite_cache_tracker() -> anyhow::Result<()> {
    cache_test(SqliteCacheTracker::memory(|access, _| Some(access))?)
}

fn cache_test(tracker: impl CacheTracker + 'static) -> anyhow::Result<()> {
    // let tracker = ;

    // create a store with a non-empty size target to enable keeping non-pinned stuff around
    let mut store = BlockStore::memory(
        Config::default()
            .with_size_targets(SizeTargets::new(10, 10000))
            .with_cache_tracker(tracker),
    )?;

    // add some pinned stuff at the very beginning
    for i in 0..2 {
        let block = pinned(i);
        store.put_block(&block, None)?;
        store.alias(block.cid().to_bytes(), Some(block.cid()))?;
    }

    // add data that is within the size targets
    for i in 0..8 {
        let block = unpinned(i);
        store.put_block(&block, None)?;
    }

    // check that gc does nothing
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);
    store.incremental_gc(5, Duration::from_secs(100000))?;
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);

    // add some more stuff to exceed the size targets
    for i in 8..13 {
        let block = unpinned(i);
        store.put_block(&block, None)?;
    }

    // access one of the existing unpinned blocks to move it to the front
    assert_eq!(
        store.get_block(&unpinned(0).cid())?,
        Some(unpinned(0).data().to_vec())
    );

    // check that gc gets triggered and removes min_blocks
    store.incremental_gc(10, Duration::from_secs(100000))?;
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);

    let cids = store.get_block_cids::<FnvHashSet<_>>()?;
    // check that the 2 pinned ones are still there despite being added first
    // and that the recently accessed block is still there
    let expected_cids = (0..2)
        .map(pinned)
        .chain(Some(unpinned(0)))
        .chain((6..13).map(unpinned))
        .map(|block| *block.cid())
        .collect::<FnvHashSet<_>>();
    assert_eq!(cids, expected_cids);
    Ok(())
}

const OLD_INIT: &str = r#"
CREATE TABLE IF NOT EXISTS blocks (
    key BLOB PRIMARY KEY,
    pinned INTEGER DEFAULT 0,
    cid BLOB,
    data BLOB
) WITHOUT ROWID;
"#;

#[test]
fn test_migration() -> anyhow::Result<()> {
    let tmp = TempDir::new("test_migration")?;
    let path = tmp.path().join("db");
    let conn = Connection::open(&path)?;
    conn.execute_batch(OLD_INIT)?;
    let mut blocks = Vec::with_capacity(5);
    for i in 0..blocks.capacity() {
        let data = (i as u64).to_be_bytes().to_vec();
        let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(&data));
        conn.prepare_cached("INSERT INTO blocks (key, pinned, cid, data) VALUES (?1, 1, ?2, ?3)")?
            .execute(params![cid.to_string(), cid.to_bytes(), data])?;
        blocks.push((cid, data));
    }
    let store = BlockStore::open(path, Config::default())?;
    for (cid, data) in blocks {
        assert_eq!(store.get_block(&cid)?, Some(data));
    }
    Ok(())
}

#[test]
fn test_resolve() -> anyhow::Result<()> {
    let mut store = BlockStore::memory(Config::default())?;
    let block = pinned(0);
    store.put_block(&block, None)?;
    store.alias(&b"leaf"[..], Some(block.cid()))?;
    let cid2 = store.resolve(&b"leaf"[..])?;
    assert_eq!(Some(*block.cid()), cid2);
    Ok(())
}

#[test]
fn test_reverse_alias() -> anyhow::Result<()> {
    let mut store = BlockStore::memory(Config::default())?;
    let block = pinned(0);
    assert_eq!(store.reverse_alias(block.cid())?, None);
    store.put_block(&block, None)?;
    assert_eq!(store.reverse_alias(block.cid())?, Some(vec![]));
    store.alias(&b"leaf"[..], Some(block.cid()))?;
    assert_eq!(
        store.reverse_alias(block.cid())?,
        Some(vec![b"leaf".to_vec()])
    );
    let block2 = links("1", vec![&block]); // needs link to cid
    store.put_block(&block2, None)?;
    store.alias(&b"root"[..], Some(block2.cid()))?;
    assert_eq!(
        store.reverse_alias(block.cid())?,
        Some(vec![b"leaf".to_vec(), b"root".to_vec()])
    );
    Ok(())
}

#[test]
fn test_vacuum() -> anyhow::Result<()> {
    let store = BlockStore::memory(Config::default())?;
    store.vacuum()?;
    Ok(())
}

#[test]
fn test_aliases() -> anyhow::Result<()> {
    let mut store = BlockStore::memory(Config::default())?;
    let block = pinned(0);
    let cid = block.cid();
    store.put_block(&block, None)?;
    store.alias(&b"a", Some(cid))?;
    store.alias(&b"b", Some(cid))?;
    store.alias(&b"c", Some(cid))?;
    let mut aliases: Vec<(Vec<u8>, Cid)> = store.aliases()?;
    aliases.sort_by_key(|x| x.0.clone());
    assert_eq!(
        aliases,
        vec![
            (b"a".to_vec(), *cid),
            (b"b".to_vec(), *cid),
            (b"c".to_vec(), *cid),
        ]
    );
    Ok(())
}

#[test]
fn temp_pin() -> anyhow::Result<()> {
    let mut store = BlockStore::memory(Config::default())?;
    let a = block("a");
    let b = block("b");
    let alias = store.temp_pin();

    store.put_block(&a, Some(&alias))?;
    store.gc()?;
    assert!(store.has_block(a.cid())?);

    store.put_block(&b, Some(&alias))?;
    store.gc()?;
    assert!(store.has_block(b.cid())?);

    drop(alias);
    store.gc()?;
    assert!(!store.has_block(a.cid())?);
    assert!(!store.has_block(b.cid())?);

    Ok(())
}

#[test]
fn broken_db() -> anyhow::Result<()> {
    let tmp = TempDir::new("broken_db")?;
    let path = tmp.path().join("mini.sqlite");
    std::fs::copy("test-data/mini.sqlite", &path)?;
    let store = BlockStore::open_path(DbPath::File(path), Config::default())?;
    assert!(store.integrity_check().is_ok());

    let path = tmp.path().join("broken.sqlite");
    std::fs::copy("test-data/broken.sqlite", &path)?;
    let store = BlockStore::open_path(DbPath::File(path), Config::default())?;
    assert!(store.integrity_check().is_err());
    Ok(())
}

#[test]
fn shared_file() -> anyhow::Result<()> {
    let tmp = TempDir::new("shared_file")?;
    let path = tmp.path().join("test.sqlite");
    let mut db1 = BlockStore::open_path(DbPath::File(path.clone()), Config::default())?;
    let db2 = BlockStore::open_path(DbPath::File(path), Config::default())?;

    for i in 0..10 {
        let block = block(&format!("block-{}", i));
        db1.put_block(&block, None)?;
        assert_eq!(db2.get_block(block.cid())?, Some(block.data().to_vec()));
    }
    Ok(())
}

#[test]
fn large_dag_gc() -> anyhow::Result<()> {
    let mut store = BlockStore::memory(Config::default())?;
    let mut l = Vec::new();
    for i in 0..100 {
        let block = links(&format!("node-{}", i), l.iter().collect());
        store.put_block(&block, None)?;
        l.push(block);
    }
    // pin the root
    let cid = *l.last().as_ref().unwrap().cid();
    store.alias((&cid).to_bytes(), Some(&cid))?;
    // this takes forever
    store.gc()?;
    Ok(())
}

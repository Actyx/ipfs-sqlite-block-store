#![allow(clippy::many_single_char_names)]
use crate::{cache::InMemCacheTracker, cache::SortByIdCacheTracker, Config, SizeTargets, Store};
use fnv::FnvHashSet;
use libipld::cid::Cid;
use libipld::multihash::{Code, MultihashDigest};
use rusqlite::{params, Connection};
use std::time::Duration;
use tempdir::TempDir;

fn cid(name: &str) -> Cid {
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    let hash = Code::Sha2_256.digest(name.as_bytes());
    Cid::new_v1(0x71, hash)
}

/*fn pb(name: &str) -> Cid {
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    let hash = Code::Sha2_256.digest(name.as_bytes());
    Cid::new_v1(0x70, hash)
}*/

fn unpinned(i: usize) -> Cid {
    cid(&format!("{}", i))
}

fn pinned(i: usize) -> Cid {
    cid(&format!("pinned-{}", i))
}

fn data(cid: &Cid, n: usize) -> Vec<u8> {
    let mut res = vec![0u8; n];
    let text = cid.to_string();
    let bytes = text.as_bytes();
    let len = res.len().min(bytes.len());
    res[0..len].copy_from_slice(&bytes[0..len]);
    res
}

#[test]
fn insert_get() -> anyhow::Result<()> {
    let mut store = Store::memory(Config::default())?;
    let a = cid("a");
    let b = cid("b");
    let c = cid("c");
    store.add_block(&a, b"abcd", vec![b, c], None)?;
    // we should have all three cids
    assert!(store.has_cid(&a)?);
    assert!(store.has_cid(&b)?);
    assert!(store.has_cid(&c)?);
    // but only the first block
    assert!(store.has_block(&a)?);
    assert!(!store.has_block(&b)?);
    assert!(!store.has_block(&c)?);
    // check the data
    assert_eq!(store.get_block(&a)?, Some(b"abcd".to_vec()));
    // check descendants
    assert_eq!(store.get_descendants::<Vec<_>>(&a)?, vec![a, b, c]);
    // check missing blocks - should be b and c
    assert_eq!(store.get_missing_blocks::<Vec<_>>(&a)?, vec![b, c]);
    // alias the root
    store.alias(b"alias1", Some(&a))?;
    store.gc()?;
    // after gc, we shold still have the block
    assert!(store.has_block(&a)?);
    store.alias(b"alias1", None)?;
    store.gc()?;
    // after gc, we shold no longer have the block
    assert!(!store.has_block(&a)?);
    Ok(())
}

#[test]
fn incremental_insert() -> anyhow::Result<()> {
    let mut store = Store::memory(Config::default())?;
    let a = cid("a");
    let b = cid("b");
    let c = cid("c");
    let d = cid("d");
    let e = cid("e");
    // alias before even adding the block
    store.alias(b"alias1", Some(&a))?;
    assert!(store.has_cid(&a)?);
    store.add_block(&a, b"abcd", vec![b, c], None)?;
    store.gc()?;
    store.add_block(&c, b"fubar", vec![d, e], None)?;
    store.gc()?;
    // we should have all five cids
    assert!(store.has_cid(&a)?);
    assert!(store.has_cid(&b)?);
    assert!(store.has_cid(&c)?);
    assert!(store.has_cid(&d)?);
    assert!(store.has_cid(&e)?);
    // but only blocks a and c
    assert!(store.has_block(&a)?);
    assert!(!store.has_block(&b)?);
    assert!(store.has_block(&c)?);
    assert!(!store.has_block(&d)?);
    assert!(!store.has_block(&e)?);
    // check the data
    assert_eq!(store.get_block(&a)?, Some(b"abcd".to_vec()));
    // check descendants
    assert_eq!(store.get_descendants::<Vec<_>>(&a)?, vec![a, b, c, d, e]);
    // check missing blocks - should be b and c
    assert_eq!(store.get_missing_blocks::<Vec<_>>(&a)?, vec![b, d, e]);
    // alias the root
    store.alias(b"alias1", Some(&a))?;
    store.gc()?;
    // after gc, we shold still have the block
    assert!(store.has_block(&a)?);
    store.alias(b"alias1", Some(&c))?;
    store.gc()?;
    assert!(!store.has_block(&a)?);
    assert!(store.has_block(&c)?);
    Ok(())
}

#[test]
fn temp_alias() -> anyhow::Result<()> {
    let mut store = Store::memory(Config::default())?;
    let a = cid("a");
    let b = cid("b");
    let alias = store.temp_alias();

    store.add_block(&a, b"abcd", vec![], Some(&alias))?;
    store.gc()?;
    assert!(store.has_block(&a)?);

    store.add_block(&b, b"fubar", vec![], Some(&alias))?;
    store.gc()?;
    assert!(store.has_block(&b)?);

    drop(alias);
    store.gc()?;
    assert!(!store.has_block(&a)?);
    assert!(!store.has_block(&b)?);

    Ok(())
}

#[test]
fn size_targets() -> anyhow::Result<()> {
    // create a store with a non-empty size target to enable keeping non-pinned stuff around
    let mut store = Store::memory(
        Config::default()
            .with_size_targets(SizeTargets::new(10, 10000))
            .with_cache_tracker(SortByIdCacheTracker),
    )?;

    // add some pinned stuff at the very beginning
    for i in 0..2 {
        let cid = pinned(i);
        let data = data(&cid, 1000);
        store.add_block(&cid, &data, vec![], None)?;
        store.alias(cid.to_bytes(), Some(&cid))?;
    }

    // add data that is within the size targets
    for i in 0..8 {
        let cid = unpinned(i);
        let data = data(&cid, 1000);
        store.add_block(&cid, &data, vec![], None)?;
    }

    // check that gc does nothing
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);
    store.incremental_gc(5, Duration::from_secs(100000))?;
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);

    // add some more stuff to exceed the size targets
    for i in 8..13 {
        let cid = cid(&format!("{}", i));
        let data = data(&cid, 1000);
        store.add_block(&cid, &data, vec![], None)?;
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
        .collect::<FnvHashSet<_>>();
    assert_eq!(cids, expected_cids);
    Ok(())
}

#[test]
fn in_mem_cache() -> anyhow::Result<()> {
    let tracker = InMemCacheTracker::new(|access, _, _| Some(access));

    // create a store with a non-empty size target to enable keeping non-pinned stuff around
    let mut store = Store::memory(
        Config::default()
            .with_size_targets(SizeTargets::new(10, 10000))
            .with_cache_tracker(tracker),
    )?;

    // add some pinned stuff at the very beginning
    for i in 0..2 {
        let cid = pinned(i);
        let data = data(&cid, 1000);
        store.add_block(&cid, &data, vec![], None)?;
        store.alias(cid.to_bytes(), Some(&cid))?;
    }

    // add data that is within the size targets
    for i in 0..8 {
        let cid = unpinned(i);
        let data = data(&cid, 1000);
        store.add_block(&cid, &data, vec![], None)?;
    }

    // check that gc does nothing
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);
    store.incremental_gc(5, Duration::from_secs(100000))?;
    assert_eq!(store.get_store_stats()?.count, 10);
    assert_eq!(store.get_store_stats()?.size, 10000);

    // add some more stuff to exceed the size targets
    for i in 8..13 {
        let cid = cid(&format!("{}", i));
        let data = data(&cid, 1000);
        store.add_block(&cid, &data, vec![], None)?;
    }

    // access one of the existing unpinned blocks to move it to the front
    assert_eq!(
        store.get_block(&unpinned(0))?,
        Some(data(&unpinned(0), 1000))
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
    let mut store = Store::open(path, Config::default())?;
    for (cid, data) in blocks {
        assert_eq!(store.get_block(&cid)?, Some(data));
    }
    Ok(())
}

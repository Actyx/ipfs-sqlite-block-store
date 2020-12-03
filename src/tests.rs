use crate::Store;
use libipld::cid::Cid;
use libipld::multihash::{Code, MultihashDigest};

fn cid(name: &str) -> Cid {
    let hash = Code::Sha2_256.digest(name.as_bytes());
    Cid::new_v1(0x71, hash)
}

// fn cid(name: &str) -> CidBytes {
//     let mut res = CidBytes::default();
//     res.write(name.as_bytes()).unwrap();
//     res
// }

#[test]
fn insert_get() -> anyhow::Result<()> {
    let mut store = Store::memory()?;
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
    assert_eq!(store.get_descendants(&a)?, vec![a, b, c]);
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
    let mut store = Store::memory()?;
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
    assert_eq!(store.get_descendants(&a)?, vec![a, b, c, d, e]);
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
    let mut store = Store::memory()?;
    let a = cid("a");
    let b = cid("b");
    let alias = store.temp_alias()?;

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

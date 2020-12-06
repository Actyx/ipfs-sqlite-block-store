use libipld::Cid;
use multihash::{Code, MultihashDigest};
use sqlite_block_store::{
    cache::InMemCacheTracker, cache::SqliteCacheTracker, Config, SizeTargets, Store,
};

fn cid(name: &str) -> Cid {
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    let hash = Code::Sha2_256.digest(name.as_bytes());
    Cid::new_v1(0x71, hash)
}

fn unpinned(i: usize) -> Cid {
    cid(&format!("{}", i))
}

fn data(cid: &Cid, n: usize) -> Vec<u8> {
    let mut res = vec![0u8; n];
    let text = cid.to_string();
    let bytes = text.as_bytes();
    let len = res.len().min(bytes.len());
    &res[0..len].copy_from_slice(&bytes[0..len]);
    res
}

fn main() -> anyhow::Result<()> {
    // a tracker that only cares about access time
    let tracker =
        SqliteCacheTracker::open("cache-test-access.sqlite", |access, _, _| Some(access))?;
    // let tracker = InMemCacheTracker::new(|access, _, _| Some(access));
    let mut store = Store::open(
        "cache-test.sqlite",
        Config::default()
            .with_size_targets(SizeTargets::new(1000, 1000000))
            .with_cache_tracker(tracker),
    )?;
    for i in 0..100000 {
        let cid = unpinned(i);
        store.add_block(&cid, &data(&cid, 10000), vec![], None)?;
    }
    for _ in 0..10 {
        for i in 0..10000 {
            store.get_block(&unpinned(i))?;
        }
    }
    Ok(())
}

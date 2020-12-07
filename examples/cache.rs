use std::time::Instant;

use ipfs_sqlite_block_store::{
    cache::SqliteCacheTracker, BlockStore, Config, OwnedBlock, SizeTargets,
};
use itertools::*;
use libipld::Cid;
use multihash::{Code, MultihashDigest};
use tracing::*;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

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
    res[0..len].copy_from_slice(&bytes[0..len]);
    res
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    // a tracker that only cares about access time
    let tracker =
        SqliteCacheTracker::open("cache-test-access.sqlite", |access, _, _| Some(access))?;
    // let tracker = InMemCacheTracker::new(|access, _, _| Some(access));
    // let tracker = NoopCacheTracker;
    let mut store = BlockStore::open(
        "cache-test.sqlite",
        Config::default()
            .with_size_targets(SizeTargets::new(1000, 1000000))
            .with_cache_tracker(tracker),
    )?;
    let n = 100000;
    for is in &(0..n).chunks(1000) {
        info!("adding 1000 blocks");
        let blocks = is
            .map(|i| {
                let cid = unpinned(i);
                let data = data(&cid, 1000);
                OwnedBlock::new(cid, data, vec![])
            })
            .collect::<Vec<_>>();
        store.add_blocks(blocks, None)?;
    }
    let mut sum = 0usize;
    let mut count = 0usize;
    let t0 = Instant::now();
    for j in 0..2 {
        info!("Accessing all blocks, round {}", j);
        for i in 0..n {
            sum += store
                .get_block(&unpinned(i))?
                .map(|x| x.len())
                .unwrap_or_default();
            count += 1;
        }
    }
    let dt = t0.elapsed();
    info!(
        "total accessed {} bytes, {} blocks, in {}s",
        sum,
        count,
        dt.as_secs_f64()
    );
    store.gc()?;
    Ok(())
}

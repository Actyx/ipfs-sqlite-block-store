use ipfs_sqlite_block_store::{
    cache::{AsyncCacheTracker, Spawner, SqliteCacheTracker},
    Block, BlockStore, Config, OwnedBlock, SizeTargets,
};
use itertools::*;
use libipld::{cbor::DagCborCodec, codec::Codec, Cid, DagCbor};
use multihash::{Code, MultihashDigest};
use std::time::Instant;
use tracing::*;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

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

/// creates a block with the name "unpinned-<i>" and a size of 1000
fn unpinned(i: usize) -> OwnedBlock {
    sized(&format!("{}", i), 10000 - 16)
}

struct TokioSpawner;

impl Spawner for TokioSpawner {
    fn spawn_blocking(&self, f: impl FnOnce() + Send + 'static) {
        tokio::task::spawn_blocking(|| f());
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    // a tracker that only cares about access time
    let tracker = SqliteCacheTracker::open("cache-test-access.sqlite", |access, _| Some(access))?;
    let tracker = AsyncCacheTracker::new(TokioSpawner, tracker);
    // let tracker = InMemCacheTracker::new(|access, _, _| Some(access));
    // let tracker = NoopCacheTracker;
    let mut store = BlockStore::open(
        "cache-test.sqlite",
        Config::default()
            .with_size_targets(SizeTargets::new(1000, 1000000))
            .with_cache_tracker(tracker),
    )?;
    let n = 100000;
    let mut cids = Vec::new();
    for is in &(0..n).chunks(1000) {
        info!("adding 1000 blocks");
        let blocks = is.map(|i| unpinned(i)).collect::<Vec<_>>();
        for block in &blocks {
            cids.push(*block.cid());
        }
        store.put_blocks(blocks, None)?;
    }
    let mut sum = 0usize;
    let mut count = 0usize;
    let t0 = Instant::now();
    for j in 0..2 {
        info!("Accessing all blocks, round {}", j);
        for cid in &cids {
            sum += store.get_block(cid)?.map(|x| x.len()).unwrap_or_default();
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

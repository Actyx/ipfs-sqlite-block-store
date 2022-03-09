use ipfs_sqlite_block_store::{Config, DbPath};
use itertools::Itertools;
use libipld::{cbor::DagCborCodec, codec::Codec, Cid, DagCbor};
use maplit::hashset;
use multihash::{Code, MultihashDigest};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

type Block = libipld::Block<libipld::DefaultParams>;
type BlockStore = ipfs_sqlite_block_store::BlockStore<libipld::DefaultParams>;

#[test]
fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    const STREAMS: usize = 5;
    const ROUNDS: usize = 200;

    const TARGET_SIZE: u64 = 10_000;
    const TARGET_COUNT: u64 = 1_000;

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("db");

    let mut store = BlockStore::open_path(
        DbPath::File(db_path),
        Config::default().with_size_targets(TARGET_COUNT, TARGET_SIZE),
    )
    .unwrap();

    let stopped = Arc::new(AtomicBool::new(false));
    let handle = std::thread::spawn({
        let stopped = stopped.clone();
        let mut store = store.additional_connection().unwrap();
        // this thread is responsible for continuously GC’ing to disturb things
        move || {
            while !stopped.load(Ordering::Acquire) {
                eprintln!("gc");
                store.gc().unwrap();
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    });

    let mut trees = (0..STREAMS)
        .map(|r| {
            let mut store = store.additional_connection().unwrap();
            (0..ROUNDS)
                .map(move |i| block(format!("block-{}-{}", r, i).as_str()))
                .tuple_windows()
                .enumerate()
                .map(move |(i, (a, b, c, d, e))| {
                    let mut pin = store.temp_pin();
                    let root = links("root", vec![&a, &b, &c, &d, &e]);
                    store
                        .put_blocks(vec![a, b, c, d, e], Some(&mut pin))
                        .unwrap();
                    let cid = *root.cid();
                    store.put_block(root, Some(&mut pin)).unwrap();
                    store
                        .alias(format!("theRoot-{}", r).as_bytes(), Some(&cid))
                        .unwrap();
                    i
                })
        })
        .collect::<Vec<_>>();

    'a: loop {
        for (count, x) in trees.iter_mut().enumerate() {
            std::thread::sleep(Duration::from_millis(1));
            if let Some(i) = x.next() {
                println!("loop {}", i);
                for r in 0..STREAMS {
                    if count < r {
                        continue;
                    }
                    let cid = store
                        .resolve(format!("theRoot-{}", r).as_bytes())
                        .unwrap()
                        .unwrap();
                    let root: Node = DagCborCodec
                        .decode(store.get_block(&cid).unwrap().unwrap().as_slice())
                        .unwrap();
                    let mut cids = hashset! {cid};
                    cids.extend(root.links.iter().copied());
                    assert_eq!(store.get_descendants::<HashSet<_>>(&cid).unwrap(), cids);
                    for (idx, cid) in root.links.iter().enumerate() {
                        let b: Node = DagCborCodec
                            .decode(store.get_block(cid).unwrap().unwrap().as_slice())
                            .unwrap();
                        assert_eq!(Node::leaf(&*format!("block-{}-{}", r, i + idx)), b);
                    }
                }
            } else {
                break 'a;
            }
        }
    }
    stopped.store(true, Ordering::Release);
    handle.join().unwrap();

    let stats = store.get_store_stats().unwrap();
    println!("stats {:?}", stats);
    assert!(stats.count() < 2 * TARGET_COUNT);
    assert!(stats.size() < 2 * TARGET_SIZE);

    store.gc().unwrap();

    let stats = store.get_store_stats().unwrap();
    println!("stats {:?}", stats);
    assert!(stats.count() < TARGET_COUNT);
    assert!(stats.size() < TARGET_SIZE);

    Ok(())
}

#[derive(Debug, DagCbor, PartialEq)]
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

/// creates a simple leaf block
fn block(name: &str) -> Block {
    let ipld = Node::leaf(name);
    let bytes = DagCborCodec.encode(&ipld).unwrap();
    let hash = Code::Sha2_256.digest(&bytes);
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    Block::new_unchecked(Cid::new_v1(0x71, hash), bytes)
}

/// creates a block with some links
fn links(name: &str, children: Vec<&Block>) -> Block {
    let ipld = Node::branch(name, children.iter().map(|b| *b.cid()).collect::<Vec<_>>());
    let bytes = DagCborCodec.encode(&ipld).unwrap();
    let hash = Code::Sha2_256.digest(&bytes);
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    Block::new_unchecked(Cid::new_v1(0x71, hash), bytes)
}

use std::time::Instant;

use ipfs_sqlite_block_store::{Block, BlockStore, Config, OwnedBlock};
use libipld::cid::Cid;
use multihash::{Code, MultihashDigest};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

fn cid(name: &str) -> Cid {
    let hash = Code::Sha2_256.digest(name.as_bytes());
    Cid::new_v1(0x71, hash)
}

fn fmt_cid(cid: Cid) -> String {
    cid.to_string()[8..12].to_string()
}

fn fmt_cids(x: impl IntoIterator<Item = Cid>) -> String {
    x.into_iter().map(fmt_cid).collect::<Vec<_>>().join(",")
}

fn build_tree_0(
    prefix: &str,
    branch: u64,
    depth: u64,
    blocks: &mut Vec<OwnedBlock>,
) -> anyhow::Result<Cid> {
    let node = cid(prefix);
    let data_size = if depth == 0 { 1024 * 16 } else { 1024 };
    let mut data = vec![0u8; data_size];
    data[0..prefix.as_bytes().len()].copy_from_slice(prefix.as_bytes());
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
    let block = OwnedBlock::new(node, data, children);
    let cid = *block.cid();
    blocks.push(block);
    Ok(cid)
}

fn build_tree(prefix: &str, branch: u64, depth: u64) -> anyhow::Result<(Cid, Vec<OwnedBlock>)> {
    let mut tmp = Vec::new();
    let res = build_tree_0(prefix, branch, depth, &mut tmp)?;
    Ok((res, tmp))
}

fn build_chain(prefix: &str, n: usize) -> anyhow::Result<(Cid, Vec<OwnedBlock>)> {
    assert!(n > 0);
    let mut blocks = Vec::with_capacity(n);
    let mk_node = |i: usize| cid(&format!("{}-{}", prefix, i));
    let mk_data = |i: usize| format!("{}-{}-data", prefix, i).as_bytes().to_vec();
    let mut prev: Option<Cid> = None;
    for i in 0..n {
        let node = mk_node(i);
        let data = mk_data(i);
        let links = prev.iter().copied().collect::<Vec<Cid>>();
        blocks.push(OwnedBlock::new(node, data, links));
        prev = Some(node);
    }
    Ok((prev.unwrap(), blocks))
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let mut store = BlockStore::open("test.sqlite", Config::default())?;
    for i in 0..10 {
        println!("Adding filler tree {}", i);
        let (tree_root, tree_blocks) = build_tree(&format!("tree-{}", i), 10, 4)?;
        store.put_blocks(tree_blocks, None)?;
        if i % 2 == 0 {
            store.alias(&format!("tree-alias-{}", i).as_bytes(), Some(&tree_root))?;
        }
    }
    let (tree_root, tree_blocks) = build_tree("test-tree", 10, 4)?;
    let (_list_root, list_blocks) = build_chain("chain", 10000)?;
    store.put_blocks(tree_blocks, None)?;
    store.put_blocks(list_blocks, None)?;
    println!(
        "descendants of {:?} {}",
        tree_root,
        store.get_descendants::<Vec<_>>(&tree_root)?.len(),
    );
    store.put_block(&cid("a"), b"adata", vec![cid("b"), cid("c")], None)?;
    println!(
        "{:?}",
        fmt_cids(store.get_missing_blocks::<Vec<_>>(&cid("a"))?)
    );
    store.put_block(&cid("b"), b"bdata", vec![], None)?;
    store.put_block(&cid("c"), b"cdata", vec![], None)?;
    println!(
        "{:?}",
        fmt_cids(store.get_descendants::<Vec<_>>(&cid("a"))?)
    );
    store.put_block(&cid("d"), b"ddata", vec![cid("b"), cid("c")], None)?;

    store.alias(b"source1", Some(&cid("a")))?;
    store.alias(b"source2", Some(&cid("d")))?;
    println!("starting gc");
    let t0 = Instant::now();
    store.gc()?;
    let dt = Instant::now() - t0;
    println!("{}", dt.as_secs_f64());
    Ok(())
}

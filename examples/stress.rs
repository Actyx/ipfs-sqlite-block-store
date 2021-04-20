use std::time::Instant;

use ipfs_sqlite_block_store::{BlockStore, Config};
use libipld::{cbor::DagCborCodec, cid::Cid, codec::Codec, DagCbor};
use multihash::{Code, MultihashDigest};
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

type Block = libipld::Block<libipld::DefaultParams>;

#[derive(Debug, DagCbor)]
struct Node {
    links: Vec<Cid>,
    text: String,
}

impl Node {
    pub fn branch(text: &str, links: impl IntoIterator<Item = Cid>) -> Self {
        Self {
            links: links.into_iter().collect(),
            text: text.into(),
        }
    }
}

/// creates a block
/// leaf blocks will be larger than branch blocks
fn block(name: &str, links: impl IntoIterator<Item = Cid>) -> Block {
    let links = links.into_iter().collect::<Vec<_>>();
    let data_size = if links.is_empty() {
        1024 * 16 - 16
    } else {
        512
    };
    let mut name = name.to_string();
    while name.len() < data_size {
        name += " ";
    }
    let ipld = Node::branch(&name, links);
    let bytes = DagCborCodec.encode(&ipld).unwrap();
    let hash = Code::Sha2_256.digest(&bytes);
    // https://github.com/multiformats/multicodec/blob/master/table.csv
    Block::new_unchecked(Cid::new_v1(0x71, hash), bytes)
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
    blocks: &mut Vec<Block>,
) -> anyhow::Result<Cid> {
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
    let block = block(prefix, children);
    let cid = *block.cid();
    blocks.push(block);
    Ok(cid)
}

fn build_tree(prefix: &str, branch: u64, depth: u64) -> anyhow::Result<(Cid, Vec<Block>)> {
    let mut tmp = Vec::new();
    let res = build_tree_0(prefix, branch, depth, &mut tmp)?;
    Ok((res, tmp))
}

fn build_chain(prefix: &str, n: usize) -> anyhow::Result<(Cid, Vec<Block>)> {
    anyhow::ensure!(n > 0);
    let mut blocks = Vec::with_capacity(n);
    let mk_node = |i: usize, links| block(&format!("{}-{}", prefix, i), links);
    let mut prev: Option<Cid> = None;
    for i in 0..n {
        let node = mk_node(i, prev);
        prev = Some(*node.cid());
        blocks.push(node);
    }
    Ok((prev.unwrap(), blocks))
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let mut store = BlockStore::<libipld::DefaultParams>::open("test.sqlite", Config::default())?;
    for i in 0..10 {
        println!("Adding filler tree {}", i);
        let (tree_root, tree_blocks) = build_tree(&format!("tree-{}", i), 10, 4)?;
        store.put_blocks(tree_blocks, None)?;
        if i % 2 == 0 {
            store.alias(&format!("tree-alias-{}", i).as_bytes(), Some(&tree_root))?;
        }
    }
    let (tree_root, tree_blocks) = build_tree("test-tree", 10, 4)?;
    let (list_root, list_blocks) = build_chain("chain", 10000)?;
    store.put_blocks(tree_blocks, None)?;
    store.put_blocks(list_blocks, None)?;
    let t0 = Instant::now();
    store.get_missing_blocks::<Vec<_>>(&list_root)?;
    store.alias("list-alias-1".as_bytes(), Some(&list_root))?;
    println!("get_missing_blocks {}", t0.elapsed().as_secs_f64());
    println!(
        "descendants of {:?} {}",
        tree_root,
        store.get_descendants::<Vec<_>>(&tree_root)?.len(),
    );
    store.put_block(&block("a", None), None)?;
    println!(
        "{:?}",
        fmt_cids(store.get_missing_blocks::<Vec<_>>(block("a", None).cid())?)
    );
    store.put_block(&block("b", None), None)?;
    store.put_block(&block("c", None), None)?;
    println!(
        "{:?}",
        fmt_cids(store.get_descendants::<Vec<_>>(block("a", None).cid())?)
    );
    store.put_block(&block("d", None), None)?;

    store.alias(b"source1", Some(block("a", None).cid()))?;
    store.alias(b"source2", Some(block("d", None).cid()))?;
    println!("starting gc");
    let t0 = Instant::now();
    store.gc()?;
    let dt = Instant::now() - t0;
    println!("{}", dt.as_secs_f64());
    Ok(())
}

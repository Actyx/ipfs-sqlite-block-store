use sqlite_block_store::{Block, BlockStore};

pub struct VecBlock {
    cid: Vec<u8>,
    data: Vec<u8>,
    links: Vec<Vec<u8>>,
}

impl Block<Vec<u8>> for VecBlock {
    type I = Box<dyn Iterator<Item = Vec<u8>>>;

    fn cid(&self) -> Vec<u8> {
        self.cid.clone()
    }

    fn data(&self) -> &[u8] {
        self.data.as_ref()
    }

    fn links(&self) -> Self::I {
        Box::new(self.links.clone().into_iter())
    }
}

impl VecBlock {
    fn new(cid: Vec<u8>, data: Vec<u8>, links: Vec<Vec<u8>>) -> Self {
        Self { cid, data, links }
    }
}

fn build_tree_0(
    prefix: &str,
    branch: u64,
    depth: u64,
    blocks: &mut Vec<VecBlock>,
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
    let block = VecBlock::new(node, data, children);
    let cid = block.cid.clone();
    blocks.push(block);
    Ok(cid)
}

fn build_tree(prefix: &str, branch: u64, depth: u64) -> anyhow::Result<(Vec<u8>, Vec<VecBlock>)> {
    let mut tmp = Vec::new();
    let res = build_tree_0(prefix, branch, depth, &mut tmp)?;
    Ok((res, tmp))
}

fn build_chain(prefix: &str, n: usize) -> anyhow::Result<(Vec<u8>, Vec<VecBlock>)> {
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
        blocks.push(VecBlock::new(
            node.as_bytes().to_vec(),
            data.as_bytes().to_vec(),
            links,
        ));
        prev = Some(node);
    }
    Ok((prev.unwrap().as_bytes().to_vec(), blocks))
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut store = BlockStore::open("test.sqlite")?;
    for i in 0..10 {
        println!("Adding filler tree {}", i);
        let (tree_root, tree_blocks) = build_tree(&format!("tree-{}", i), 10, 4)?;
        store.add_blocks(tree_blocks)?;
        if i % 2 == 0 {
            store.alias(&format!("tree-alias-{}", i).as_bytes(), Some(&tree_root))?;
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
    store.add_block(&b"a".to_vec(), b"adata", vec![b"b".to_vec(), b"c".to_vec()])?;
    println!("{:?}", store.get_missing_blocks(b"a".to_vec())?);
    store.add_block(&b"b".to_vec(), b"bdata", vec![])?;
    store.add_block(&b"c".to_vec(), b"cdata", vec![])?;
    println!("{:?}", store.get_descendants(b"a".to_vec())?);
    store.add_block(&b"d".to_vec(), b"ddata", vec![b"b".to_vec(), b"c".to_vec()])?;

    store.alias(b"source1", Some(&b"a".to_vec()))?;
    store.alias(b"source2", Some(&b"d".to_vec()))?;
    store.gc(100000000)?;
    // let atime = store.gc(100000)?;
    // println!("{:?}", atime);
    // println!("ancestors {:?}", store.get_ancestors(b"b"));
    // println!("descendants {:?}", store.get_descendants(b"a"));
    Ok(())
}

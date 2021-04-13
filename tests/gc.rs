use anyhow::Result;
use fnv::FnvHashSet;
use ipfs_sqlite_block_store::{BlockStore, Config};
use libipld::cbor::DagCborCodec;
use libipld::multihash::Code;
use libipld::{Block, Cid, DagCbor, DefaultParams};
use rand::{thread_rng, RngCore};
use std::time::Instant;

fn tracing_try_init() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();
}

#[test]
fn gc() -> Result<()> {
    tracing_try_init();
    let mut builder = DagBuilder::new()?;
    let now = Instant::now();
    for _ in 0..100 {
        builder.add_head()?;
    }
    println!("created dags in {}ms", now.elapsed().as_millis());
    let now = Instant::now();
    builder.check()?;
    println!("checked dags in {}ms", now.elapsed().as_millis());
    for _ in 0..builder.heads() {
        let now = Instant::now();
        builder.remove_head()?;
        builder.check()?;
        println!("removed dag in {}ms", now.elapsed().as_millis());
    }
    assert!(builder.store.get_block_cids::<Vec<_>>()?.is_empty());
    Ok(())
}

#[derive(DagCbor)]
struct Node {
    nonce: u64,
    children: Vec<Cid>,
}

struct DagBuilder {
    store: BlockStore,
    heads: FnvHashSet<Cid>,
}

impl DagBuilder {
    fn new() -> Result<Self> {
        Ok(Self {
            store: BlockStore::memory(Config::default())?,
            heads: Default::default(),
        })
    }

    fn heads(&self) -> usize {
        self.heads.len()
    }

    fn add_head(&mut self) -> Result<()> {
        let mut rng = thread_rng();
        let n_children = if self.heads.is_empty() {
            0
        } else {
            rng.next_u32() as usize % self.heads.len()
        };
        let n_children_rm = if n_children == 0 {
            0
        } else {
            rng.next_u32() as usize % n_children
        };
        let nonce = rng.next_u64();
        let mut children = Vec::with_capacity(n_children);
        children.extend(self.heads.iter().take(n_children));
        let node = Node { nonce, children };
        let block = Block::<DefaultParams>::encode(DagCborCodec, Code::Sha2_256, &node)?;
        self.store
            .alias(block.cid().to_bytes(), Some(block.cid()))?;
        self.store.put_block(&block, None)?;
        for cid in node.children.into_iter().take(n_children_rm) {
            self.store.alias(cid.to_bytes(), None)?;
            self.heads.remove(&cid);
        }
        self.heads.insert(*block.cid());
        Ok(())
    }

    fn remove_head(&mut self) -> Result<()> {
        if let Some(cid) = self.heads.iter().next().copied() {
            self.store.alias(cid.to_bytes(), None)?;
            self.heads.remove(&cid);
        }
        Ok(())
    }

    fn check(&mut self) -> Result<()> {
        let now = Instant::now();
        self.store.gc()?;
        println!("gc in {}ms", now.elapsed().as_millis());

        let now = Instant::now();
        let mut live = FnvHashSet::default();
        let mut stack = self.heads.clone();
        while let Some(cid) = stack.iter().next().copied() {
            stack.remove(&cid);
            if live.contains(&cid) {
                continue;
            }
            let bytes = self.store.get_block(&cid)?.unwrap();
            Block::<DefaultParams>::new_unchecked(cid, bytes).references(&mut stack)?;
            live.insert(cid);
        }
        println!("computed closure in {}ms", now.elapsed().as_millis());
        Ok(())
    }
}

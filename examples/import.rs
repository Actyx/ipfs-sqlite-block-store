use ipfs_sqlite_block_store::{BlockStore, Config, OwnedBlock};
use itertools::*;
use libipld::cid::Cid;
use libipld::store::DefaultParams;
use rusqlite::{params, Connection, OpenFlags};
use std::convert::TryFrom;
use std::path::Path;
use tracing::*;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

pub fn query_roots(path: &Path) -> anyhow::Result<Vec<(String, Cid)>> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let len: u32 = conn.query_row("SELECT COUNT(1) FROM roots", params![], |row| row.get(0))?;
    let mut stmt = conn.prepare("SELECT * FROM roots")?;
    let roots_iter = stmt.query_map(params![], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(2)?))
    })?;
    let mut roots = Vec::with_capacity(len as usize);
    for res in roots_iter {
        let (key, cid) = res?;
        let cid = Cid::try_from(cid)?;
        roots.push((key, cid));
    }
    Ok(roots)
}

pub struct OldBlock {
    key: Vec<u8>,
    cid: Vec<u8>,
    data: Vec<u8>,
}

pub struct IpldBlock(libipld::Block<DefaultParams>);

impl ipfs_sqlite_block_store::Block for IpldBlock {
    type I = std::vec::IntoIter<Cid>;

    fn cid(&self) -> &Cid {
        self.0.cid()
    }

    fn data(&self) -> &[u8] {
        self.0.data()
    }

    fn links(&self) -> Self::I {
        let mut links = Vec::new();
        self.0.references(&mut links).unwrap();
        links.into_iter()
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        println!("Usage: import <block store sql> <index sql>");
        std::process::exit(1);
    }
    info!("opening roots db {}", args[0]);
    info!("opening blocks db {}", args[1]);
    let roots = Path::new(&args[1]);
    let blocks = Path::new(&args[2]);
    let output = Path::new("out.sqlite");
    let mut store = BlockStore::open(output, Config::default())?;

    let blocks = Connection::open_with_flags(blocks, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let len: u32 = blocks.query_row("SELECT COUNT(1) FROM blocks", params![], |row| row.get(0))?;
    info!("importing {} blocks", len);

    let mut stmt = blocks.prepare("SELECT * FROM blocks")?;
    let block_iter = stmt.query_map(params![], |row| {
        Ok(OldBlock {
            key: row.get(0)?,
            //pinned: row.get(1)?,
            cid: row.get(2)?,
            data: row.get(3)?,
        })
    })?;

    let block_iter = block_iter.map(|block| {
        block.map_err(anyhow::Error::from).and_then(|block| {
            let key = Cid::try_from(String::from_utf8(block.key)?)?;
            let cid = Cid::try_from(block.cid)?;
            assert_eq!(key.hash(), cid.hash());
            //println!("{} {} {}", cid, block.pinned, block.data.len());
            let block = libipld::Block::<DefaultParams>::new(cid, block.data)?;
            let mut refs = Vec::new();
            block.references(&mut refs)?;
            let (cid, data) = block.into_inner();
            Ok(OwnedBlock::new(cid, data, refs))
        })
    });

    for block in &block_iter.chunks(1000) {
        info!("adding 1000 block chunk");
        let blocks = block.collect::<anyhow::Result<Vec<_>>>()?;
        store.add_blocks(blocks, None)?;
    }

    for (alias, cid) in query_roots(roots)?.into_iter() {
        info!("aliasing {} to {}", alias, cid);
        let now = std::time::Instant::now();
        store.alias(alias.as_bytes(), Some(&cid))?;
        info!("{}ms", now.elapsed().as_millis());
        let missing = store.get_missing_blocks::<Vec<_>>(&cid)?;
        info!("{} blocks missing", missing.len());
    }

    store.gc()?;

    let now = std::time::Instant::now();
    let mut len = 0usize;
    for (i, cid) in store.get_block_cids::<Vec<_>>()?.iter().enumerate() {
        if i % 1000 == 0 {
            info!("iterating {} {}", cid, i);
        }
        len += store.get_block(cid)?.map(|x| x.len()).unwrap_or(0)
    }
    let dt = now.elapsed().as_secs_f64();
    info!("iterating over all blocks: {}s", dt);
    info!("len = {}", len);
    info!("rate = {} bytes/s", (len as f64) / dt);

    let now = std::time::Instant::now();
    let mut len = 0usize;
    for (i, cid) in store.get_block_cids::<Vec<_>>()?.iter().enumerate() {
        if i % 1000 == 0 {
            info!("iterating {} {}", cid, i);
        }
        len += store.get_block(cid)?.map(|x| x.len()).unwrap_or(0)
    }
    let dt = now.elapsed().as_secs_f64();
    info!("iterating over all blocks: {}s", dt);
    info!("len = {}", len);
    info!("rate = {} bytes/s", (len as f64) / dt);
    Ok(())
}

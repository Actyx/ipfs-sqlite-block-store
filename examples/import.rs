use libipld::cid::Cid;
use libipld::store::DefaultParams;
use rusqlite::{params, Connection, OpenFlags};
use sqlite_block_store::BlockStore;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::path::Path;

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

fn main() -> anyhow::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        println!("Usage: import <block store sql> <index sql>");
        anyhow::bail!("");
    } else {
        println!("opening roots db {}", args[0]);
        println!("opening blocks db {}", args[1]);
    }
    let roots = Path::new(&args[1]);
    let blocks = Path::new(&args[2]);
    let output = Path::new("out.sqlite");
    let mut store = BlockStore::open(output)?;

    let blocks = Connection::open_with_flags(blocks, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let len: u32 = blocks.query_row("SELECT COUNT(1) FROM blocks", params![], |row| row.get(0))?;
    println!("{} blocks", len);

    let mut stmt = blocks.prepare("SELECT * FROM blocks")?;
    let block_iter = stmt.query_map(params![], |row| {
        Ok(OldBlock {
            key: row.get(0)?,
            //pinned: row.get(1)?,
            cid: row.get(2)?,
            data: row.get(3)?,
        })
    })?;

    for (i, block) in block_iter.enumerate() {
        let block = block?;
        let key = Cid::try_from(String::from_utf8(block.key)?)?;
        let cid = Cid::try_from(block.cid)?;
        assert_eq!(key.hash(), cid.hash());
        println!("key {} {}", key, i);
        //println!("{} {} {}", cid, block.pinned, block.data.len());
        let block = libipld::Block::<DefaultParams>::new(cid, block.data)?;
        let mut set = HashSet::new();
        block.references(&mut set)?;
        store.add_block(
            &block.cid().to_bytes(),
            block.data(),
            set.into_iter()
                .map(|cid| cid.to_bytes())
                .collect::<Vec<_>>(),
        )?;
    }

    for (alias, cid) in query_roots(roots)?.into_iter() {
        println!("aliasing {} to {}", alias, cid);
        let now = std::time::Instant::now();
        store.alias(alias.as_bytes(), Some(&cid.to_bytes()))?;
        println!("{}ms", now.elapsed().as_millis());
        let missing = store.get_missing_blocks(cid.to_bytes())?;
        println!("{} blocks missing", missing.len());
    }

    let now = std::time::Instant::now();
    let mut len = 0usize;
    for (i, cid) in store.get_cids()?.iter().enumerate() {
        if i % 1000 == 0 {
            println!("{} {}", cid.len(), i);
        }
        len += store.get_block(cid)?.map(|x| x.len()).unwrap_or(0)
    }
    let dt = now.elapsed().as_secs_f64();
    println!("iterating over all blocks: {}s", dt);
    println!("len = {}", len);
    println!("rate = {} bytes/s", (len as f64) / dt);

    let now = std::time::Instant::now();
    let mut len = 0usize;
    for (i, cid) in store.get_cids()?.iter().enumerate() {
        if i % 1000 == 0 {
            println!("{} {}", cid.len(), i);
        }
        len += store.get_block(cid)?.map(|x| x.len()).unwrap_or(0)
    }
    let dt = now.elapsed().as_secs_f64();
    println!("iterating over all blocks: {}s", dt);
    println!("len = {}", len);
    println!("rate = {} bytes/s", (len as f64) / dt);
    Ok(())
}

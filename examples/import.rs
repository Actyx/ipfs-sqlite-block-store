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
    let roots = Path::new("/home/dvc/actyx/_bag_prod_2020-03-25");
    let blocks = Path::new("/home/dvc/actyx/_bag_prod_2020-03-25-blocks.sqlite");
    let output = Path::new("/home/dvc/actyx/bag_prod_sqlite");
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

    for block in block_iter {
        let block = block?;
        let key = Cid::try_from(String::from_utf8(block.key)?)?;
        let cid = Cid::try_from(block.cid)?;
        assert_eq!(key.hash(), cid.hash());
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
    }

    Ok(())
}

mod cidbytes;
mod db;
mod error;
#[cfg(test)]
mod tests;

use crate::cidbytes::CidBytes;
use db::{add_block, in_txn, TempAlias};
pub use db::{Block, BlockStore};
pub use error::{BlockStoreError, Result};
use libipld::cid::{self, Cid};
use std::{
    convert::{TryFrom, TryInto},
    iter::FromIterator,
    path::Path,
    time::Instant,
};

pub struct Store {
    inner: BlockStore<CidBytes>,
}

pub struct CidBlock {
    cid: Cid,
    data: Vec<u8>,
    links: Vec<Cid>,
}

impl CidBlock {
    pub fn new(cid: Cid, data: Vec<u8>, links: Vec<Cid>) -> Self {
        Self { cid, data, links }
    }
}

impl Block<Cid> for CidBlock {
    type I = std::vec::IntoIter<Cid>;

    fn cid(&self) -> Cid {
        self.cid
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn links(&self) -> Self::I {
        self.links.clone().into_iter()
    }
}

struct CidBytesBlock {
    cid: CidBytes,
    data: Vec<u8>,
    links: Vec<CidBytes>,
}

impl Block<CidBytes> for CidBytesBlock {
    type I = std::vec::IntoIter<CidBytes>;

    fn cid(&self) -> CidBytes {
        self.cid
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn links(&self) -> Self::I {
        self.links.clone().into_iter()
    }
}

impl TryFrom<CidBlock> for CidBytesBlock {
    type Error = cid::Error;

    fn try_from(value: CidBlock) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            cid: (&value.cid).try_into()?,
            data: value.data,
            links: value
                .links
                .iter()
                .map(CidBytes::try_from)
                .collect::<std::result::Result<Vec<_>, cid::Error>>()?,
        })
    }
}

impl Store {
    pub fn memory() -> Result<Self> {
        Ok(Self {
            inner: BlockStore::memory()?,
        })
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: BlockStore::open(path)?,
        })
    }
    pub fn temp_alias(&self) -> crate::Result<TempAlias> {
        Ok(self.inner.create_temp_alias()?)
    }
    pub fn alias(&mut self, name: impl AsRef<[u8]>, link: Option<&Cid>) -> crate::Result<()> {
        let link: Option<CidBytes> = link.map(|x| CidBytes::try_from(x)).transpose()?;
        Ok(self.inner.alias(name.as_ref(), link.as_ref())?)
    }
    pub fn has_cid(&self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        Ok(self.inner.has_cid(&cid)?)
    }
    pub fn has_block(&mut self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        Ok(self.inner.has_block(&cid)?)
    }
    pub fn get_block_count(&mut self) -> Result<u64> {
        Ok(self.inner.get_block_count()?)
    }
    pub fn get_block_size(&mut self) -> Result<u64> {
        Ok(self.inner.get_block_size()?)
    }
    pub fn get_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        let result = self.inner.get_cids()?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<C>>()?;
        Ok(res)
    }
    pub fn get_descendants(&mut self, cid: &Cid) -> Result<Vec<Cid>> {
        let cid = CidBytes::try_from(cid)?;
        let result = self.inner.get_descendants(cid)?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<Vec<_>>>()?;
        Ok(res)
    }
    pub fn get_missing_blocks<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let result = self.inner.get_missing_blocks(cid)?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<C>>()?;
        Ok(res)
    }
    pub fn gc(&mut self) -> Result<()> {
        let t0 = Instant::now();
        self.inner.gc()?;
        println!(
            "deleting ids and most metadata {}",
            (Instant::now() - t0).as_secs_f64()
        );
        while self.inner.count_orphaned()? > 0 {
            let t0 = Instant::now();
            self.inner.delete_orphaned()?;
            println!(
                "deleting 10000 blocks {}",
                (Instant::now() - t0).as_secs_f64()
            );
        }
        Ok(())
    }
    pub fn add_blocks(
        &mut self,
        blocks: impl IntoIterator<Item = CidBlock>,
        alias: Option<&TempAlias>,
    ) -> Result<()> {
        let blocks = blocks
            .into_iter()
            .map(CidBytesBlock::try_from)
            .collect::<cid::Result<Vec<_>>>()?;
        self.inner.add_blocks(blocks, alias)?;
        Ok(())
    }
    pub fn add_block(
        &mut self,
        cid: &Cid,
        data: &[u8],
        links: impl IntoIterator<Item = Cid>,
        alias: Option<&TempAlias>,
    ) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        let links = links
            .into_iter()
            .map(|x| CidBytes::try_from(&x))
            .collect::<cid::Result<Vec<_>>>()?;
        Ok(self.inner.add_block(&cid, data, links, alias)?)
    }
    pub fn get_block(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let cid = CidBytes::try_from(cid)?;
        Ok(self.inner.get_block(&cid)?)
    }
}

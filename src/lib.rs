mod cidbytes;
mod db;
#[cfg(test)]
mod tests;

use std::{
    convert::{TryFrom, TryInto},
    path::Path,
};

use cid::Cid;
use cidbytes::CidBytes;
pub use db::{Block, BlockStore};

pub struct Store {
    inner: BlockStore<CidBytes>,
}

pub struct CidBlock {
    cid: Cid,
    data: Vec<u8>,
    links: Vec<Cid>,
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

    fn try_from(value: CidBlock) -> Result<Self, Self::Error> {
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
    pub fn memory() -> anyhow::Result<Self> {
        Ok(Self {
            inner: BlockStore::memory()?,
        })
    }
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            inner: BlockStore::open(path)?,
        })
    }
    pub fn alias(&mut self, name: impl AsRef<[u8]>, link: Option<&Cid>) -> anyhow::Result<()> {
        let link: Option<CidBytes> = link.map(|x| CidBytes::try_from(x)).transpose()?;
        Ok(self.inner.alias(name.as_ref(), link.as_ref())?)
    }
    pub fn has_cid(&mut self, cid: &Cid) -> anyhow::Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        Ok(self.inner.has_cid(&cid)?)
    }
    pub fn has_block(&mut self, cid: &Cid) -> anyhow::Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        Ok(self.inner.has_block(&cid)?)
    }
    pub fn get_descendants(&mut self, cid: &Cid) -> anyhow::Result<Vec<Cid>> {
        let cid = CidBytes::try_from(cid)?;
        let result = self.inner.get_descendants(cid)?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<Vec<_>>>()?;
        Ok(res)
    }
    pub fn get_missing_blocks(&mut self, cid: &Cid) -> anyhow::Result<Vec<Cid>> {
        let cid = CidBytes::try_from(cid)?;
        let result = self.inner.get_missing_blocks(cid)?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<Vec<_>>>()?;
        Ok(res)
    }
    pub fn gc(&mut self, grace_atime: i64) -> anyhow::Result<Option<i64>> {
        Ok(self.inner.gc(grace_atime)?)
    }
    pub fn add_blocks(&mut self, blocks: impl IntoIterator<Item = CidBlock>) -> anyhow::Result<()> {
        let blocks = blocks
            .into_iter()
            .map(CidBytesBlock::try_from)
            .collect::<cid::Result<Vec<_>>>()?;
        self.inner.add_blocks(blocks)?;
        Ok(())
    }
    pub fn add_block(
        &mut self,
        cid: &Cid,
        data: &[u8],
        links: impl IntoIterator<Item = Cid>,
    ) -> anyhow::Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        let links = links
            .into_iter()
            .map(|x| CidBytes::try_from(&x))
            .collect::<cid::Result<Vec<_>>>()?;
        Ok(self.inner.add_block(&cid, data, links)?)
    }
    pub fn get_block(&mut self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        let cid = CidBytes::try_from(cid)?;
        Ok(self.inner.get_block(&cid)?)
    }
}

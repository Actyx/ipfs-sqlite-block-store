use std::{convert::TryInto, sync::Arc};
use libipld::Cid;
use weight_cache::{Weighable, WeightCache};

#[derive(Debug)]
struct MemBlock(Arc<[u8]>);

impl Weighable for MemBlock {
    fn measure(value: &Self) -> usize {
        value.0.len()
    }
}

#[derive(Debug)]
pub(crate) struct MemCache<'a> {
    parent: Option<&'a mut MemCache<'a>>,
    /// the actual cache, disabled if a capacity of 0 was configured
    inner: Option<WeightCache<Cid, MemBlock>>,
    /// maximum size of blocks to cache
    /// we want this to remain very small, so we only cache tiny blocks
    max_size: usize,
}

impl<'a> MemCache<'a> {
    pub fn new(max_size: usize, capacity: usize, parent: Option<&'a mut MemCache<'a>>) -> Self {
        let capacity = capacity.try_into().ok();
        Self {
            parent,
            max_size,
            inner: capacity.map(WeightCache::new),
        }
    }

    pub fn offer(&mut self, key: &Cid, data: &[u8]) {
        if let Some(cache) = self.inner.as_mut() {
            if data.len() <= self.max_size {
                let _ = cache.put(*key, MemBlock(data.into()));
            }
        }
    }

    pub fn get(&mut self, key: &Cid) -> Option<&[u8]> {
        self.get0(key).map(|x| x.0.as_ref())
    }

    pub fn has(&mut self, key: &Cid) -> bool {
        self.get0(key).is_some()
    }

    /// get the value, just from ourselves, as a MemBlock
    fn get0(&mut self, key: &Cid) -> Option<&MemBlock> {
        self.inner.as_mut().and_then(|cache| cache.get(key))
    }
}

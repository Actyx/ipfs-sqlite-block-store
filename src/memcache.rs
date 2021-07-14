use libipld::Cid;
use std::{convert::TryInto, sync::Arc};
use weight_cache::{Weighable, WeightCache};

#[derive(Debug)]
struct MemBlock(i64, Arc<[u8]>);

impl Weighable for MemBlock {
    fn measure(value: &Self) -> usize {
        value.1.len()
    }
}

#[derive(Debug)]
pub(crate) struct MemCache {
    /// the actual cache, disabled if a capacity of 0 was configured
    inner: Option<WeightCache<Cid, MemBlock>>,
    /// maximum size of blocks to cache
    /// we want this to remain very small, so we only cache tiny blocks
    max_size: usize,
}

impl MemCache {
    pub fn new(max_size: usize, capacity: usize) -> Self {
        let capacity = capacity.try_into().ok();
        Self {
            max_size,
            inner: capacity.map(WeightCache::new),
        }
    }

    pub fn offer(&mut self, id: i64, key: &Cid, data: &[u8]) {
        if let Some(cache) = self.inner.as_mut() {
            if data.len() <= self.max_size {
                let _ = cache.put(*key, MemBlock(id, data.into()));
            }
        }
    }

    pub fn get(&mut self, key: &Cid) -> Option<(i64, Vec<u8>)> {
        self.get0(key).map(|x| (x.0, x.1.to_vec()))
    }

    pub fn has(&mut self, key: &Cid) -> bool {
        self.get0(key).is_some()
    }

    /// get the value, just from ourselves, as a MemBlock
    fn get0(&mut self, key: &Cid) -> Option<&MemBlock> {
        self.inner.as_mut().and_then(|cache| cache.get(key))
    }

    pub fn clear(&mut self) {
        self.inner = Some(WeightCache::new((1024 * 1024 * 4).try_into().unwrap()));
    }
}

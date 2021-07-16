use libipld::Cid;
use std::{convert::TryInto, sync::Arc};
use weight_cache::{Weighable, WeightCache};

#[derive(Debug)]
struct MemBlock(i64, Arc<[u8]>);

const CAPACITY: usize = 1024 * 1024 * 64;
const MAX_SIZE: usize = 1024 * 4;

impl Weighable for MemBlock {
    fn measure(value: &Self) -> usize {
        value.1.len()
    }
}

impl Default for MemCache {
    fn default() -> Self {
        Self::new(MAX_SIZE, CAPACITY)
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
    /// create a new MemCache
    /// `max_size` the maximum size for a block to be considered for caching
    /// `capacity` the total capacity of the cache, 0 to disable
    pub fn new(max_size: usize, capacity: usize) -> Self {
        let capacity = capacity.try_into().ok();
        Self {
            max_size,
            inner: capacity.map(WeightCache::new),
        }
    }

    /// offer a block to the cache, it will only consider it for caching if it is <= max_size
    pub fn offer(&mut self, id: i64, key: &Cid, data: &[u8]) {
        if let Some(cache) = self.inner.as_mut() {
            if data.len() <= self.max_size {
                let _ = cache.put(*key, MemBlock(id, data.into()));
            }
        }
    }

    /// get a block out of the cache
    pub fn get(&mut self, key: &Cid) -> Option<(i64, Vec<u8>)> {
        self.get0(key).map(|x| (x.0, x.1.to_vec()))
    }

    /// check if the cache has a block
    pub fn has(&mut self, key: &Cid) -> bool {
        self.get0(key).is_some()
    }

    /// get the value, just from ourselves, as a MemBlock
    fn get0(&mut self, key: &Cid) -> Option<&MemBlock> {
        match self.inner.as_mut().and_then(|cache| cache.get(key)) {
            Some(block) => {
                tracing::info!("CACHE HIT {}", block.1.len());
                Some(block)
            },
            None => {
                tracing::info!("CACHE MISS");
                None
            }
        }
    }

    /// clear the cache
    pub fn clear(&mut self) {
        self.inner = Some(WeightCache::new(CAPACITY.try_into().unwrap()));
    }

    /// remove a single cid
    pub fn remove(&mut self, _cid: &Cid) {
        // well, it works
        self.clear()
    }
}

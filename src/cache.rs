use libipld::Cid;
use std::fmt::Debug;

/// tracks block reads and writes to provide info about which blocks to evict from the LRU cache
#[allow(unused_variables)]
pub trait CacheTracker: Debug {
    /// called whenever blocks were accessed
    fn blocks_accessed(&mut self, blocks: &[(i64, &Cid, &[u8])]) {}
    /// called whenever blocks were written
    fn blocks_written(&mut self, blocks: &[(i64, &Cid, &[u8])]) {}
    /// notification that these ids no longer have to be tracked
    fn delete_ids(&mut self, ids: &[i64]) {}
    /// notification that only these ids should be retained
    fn retain_ids(&mut self, ids: &[i64]) {}
    /// sort ids by importance. More important ids should go to the end.
    fn sort_ids(&self, ids: &mut [i64]) {}
}

impl CacheTracker for Box<dyn CacheTracker> {
    fn blocks_accessed(&mut self, blocks: &[(i64, &Cid, &[u8])]) {
        self.as_mut().blocks_accessed(blocks)
    }

    fn blocks_written(&mut self, blocks: &[(i64, &Cid, &[u8])]) {
        self.as_mut().blocks_written(blocks)
    }

    fn sort_ids(&self, ids: &mut [i64]) {
        self.as_ref().sort_ids(ids)
    }
}

/// a cache tracker that does nothing whatsoever, but is extremely fast
#[derive(Debug)]
pub struct NoopCacheTracker;

impl CacheTracker for NoopCacheTracker {}

/// a cache tracker that just sorts by id, which is the time of first addition of a block
#[derive(Debug)]
pub struct SortByIdCacheTracker;

impl CacheTracker for SortByIdCacheTracker {
    fn sort_ids(&self, ids: &mut [i64]) {
        // a bit faster than stable sort, and obviously for ids it does not matter
        ids.sort_unstable();
    }
}

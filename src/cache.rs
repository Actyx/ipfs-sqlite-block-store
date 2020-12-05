use std::fmt::Debug;

use libipld::Cid;

/// tracks block reads and writes to provide info about which blocks to evict from the LRU cache
pub trait CacheTracker: Debug {
    /// called whenever a block is accessed
    fn block_accessed(&mut self, id: i64, cid: &Cid, data: &[u8]);
    /// called whenever a block is written, even if it already exists
    fn block_written(&mut self, id: i64, cid: &Cid, data: &[u8]);
    /// sort ids by importance. Ids that should be kept should go to the end.
    fn sort_ids(&self, ids: &mut [i64]);
}

/// a cache tracker that does nothing whatsoever, but is extremely fast
#[derive(Debug)]
pub struct NoopCacheTracker;

impl CacheTracker for NoopCacheTracker {
    fn block_accessed(&mut self, _id: i64, _cid: &Cid, _data: &[u8]) {}
    fn block_written(&mut self, _id: i64, _cid: &Cid, _data: &[u8]) {}
    fn sort_ids(&self, _ids: &mut [i64]) {}
}

impl CacheTracker for Box<dyn CacheTracker> {
    fn block_accessed(&mut self, id: i64, cid: &Cid, data: &[u8]) {
        self.as_mut().block_accessed(id, cid, data)
    }

    fn block_written(&mut self, id: i64, cid: &Cid, data: &[u8]) {
        self.as_mut().block_written(id, cid, data)
    }

    fn sort_ids(&self, ids: &mut [i64]) {
        self.as_ref().sort_ids(ids)
    }
}

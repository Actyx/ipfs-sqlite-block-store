use fnv::{FnvHashMap, FnvHashSet};
use libipld::Cid;
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};
mod async_tracker;
mod sqlite_tracker;
pub use async_tracker::{AsyncCacheTracker, Spawner};
use parking_lot::Mutex;
pub use sqlite_tracker::SqliteCacheTracker;

#[cfg(test)]
mod tests;

/// Information about a block that is quick to gather
///
/// This is what is available for making decisions about whether to cache a block
#[derive(Debug, Clone, Copy)]
pub struct BlockInfo {
    /// id of the block in the block store
    id: i64,
    /// cid
    cid: Cid,
    /// size of the block
    len: usize,
}

impl BlockInfo {
    pub fn new(id: i64, cid: &Cid, len: usize) -> Self {
        Self { id, cid: *cid, len }
    }
    pub fn id(&self) -> i64 {
        self.id
    }
    pub fn cid(&self) -> &Cid {
        &self.cid
    }
    pub fn block_len(&self) -> usize {
        self.len
    }
}

/// Information about a write operation that is cheap to gather
#[derive(Debug, Clone, Copy)]
pub struct WriteInfo {
    block: BlockInfo,
    block_exists: bool,
}

impl WriteInfo {
    pub fn new(block: BlockInfo, block_exists: bool) -> Self {
        Self {
            block,
            block_exists,
        }
    }
    /// true if we had the block already.
    pub fn block_exists(&self) -> bool {
        self.block_exists
    }
}

impl Deref for WriteInfo {
    type Target = BlockInfo;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

/// tracks block reads and writes to provide info about which blocks to evict from the LRU cache
#[allow(unused_variables)]
pub trait CacheTracker: Debug + Send + Sync {
    /// called whenever blocks were accessed
    ///
    /// note that this method will be called very frequently, on every block access.
    /// it is fire and forget, so it is perfectly ok to offload the writing to another thread.
    fn blocks_accessed(&self, blocks: Vec<BlockInfo>) {}

    /// called whenever blocks were written
    ///
    /// note that this method will be called frequently, on every block write.
    /// it is fire and forget, so it is perfectly ok to offload the writing to another thread.
    fn blocks_written(&self, blocks: Vec<WriteInfo>) {}

    /// called whenever blocks have been deleted by gc.
    fn blocks_deleted(&self, blocks: Vec<BlockInfo>) {}

    /// sort ids by importance. More important ids should go to the end.
    ///
    /// this will be called from inside gc
    fn sort_ids(&self, ids: &mut [i64]) {}

    /// indicate whether `retain_ids` should be called on startup
    fn has_persistent_state(&self) -> bool;

    /// notification that only these ids should be retained
    ///
    /// this will be called once during startup
    fn retain_ids(&self, ids: &[i64]) {}
}

impl CacheTracker for Arc<dyn CacheTracker> {
    fn blocks_accessed(&self, blocks: Vec<BlockInfo>) {
        self.as_ref().blocks_accessed(blocks)
    }

    fn blocks_written(&self, blocks: Vec<WriteInfo>) {
        self.as_ref().blocks_written(blocks)
    }

    fn sort_ids(&self, ids: &mut [i64]) {
        self.as_ref().sort_ids(ids)
    }

    fn blocks_deleted(&self, blocks: Vec<BlockInfo>) {
        self.as_ref().blocks_deleted(blocks)
    }

    fn has_persistent_state(&self) -> bool {
        self.as_ref().has_persistent_state()
    }

    fn retain_ids(&self, ids: &[i64]) {
        self.as_ref().retain_ids(ids)
    }
}

/// a cache tracker that does nothing whatsoever, but is extremely fast
#[derive(Debug)]
pub struct NoopCacheTracker;

impl CacheTracker for NoopCacheTracker {
    fn has_persistent_state(&self) -> bool {
        false
    }
}

/// a cache tracker that just sorts by id, which is the time of first addition of a block
#[derive(Debug)]
pub struct SortByIdCacheTracker;

impl CacheTracker for SortByIdCacheTracker {
    fn sort_ids(&self, ids: &mut [i64]) {
        // a bit faster than stable sort, and obviously for ids it does not matter
        ids.sort_unstable();
    }
    fn has_persistent_state(&self) -> bool {
        false
    }
}

/// keep track of block accesses in memory
pub struct InMemCacheTracker<T, F> {
    cache: Arc<Mutex<FnvHashMap<i64, T>>>,
    mk_cache_entry: F,
    created: Instant,
}

impl<T, F> InMemCacheTracker<T, F>
where
    T: Ord + Clone + Debug,
    F: Fn(Duration, BlockInfo) -> Option<T>,
{
    /// mk_cache_entry will be called on each block access to create or update a cache entry.
    /// It allows to customize whether we are interested in an entry at all, and what
    /// entries we want to be preserved.
    ///
    /// E.g. to just sort entries by their access time, use `|access, _, _| Some(access)`.
    /// this will keep entries in the cache based on last access time.
    ///
    /// It is also possible to use more sophisticated strategies like only caching certain cid types
    /// or caching based on the data size.
    pub fn new(mk_cache_entry: F) -> Self {
        Self {
            cache: Arc::new(Mutex::new(FnvHashMap::default())),
            mk_cache_entry,
            created: Instant::now(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SortKey<T: Ord> {
    time: Option<T>,
    id: i64,
}

impl<T: Ord> SortKey<T> {
    pub fn new(time: Option<T>, id: i64) -> Self {
        Self { time, id }
    }
}

fn get_key<T: Ord + Clone>(
    cache: &mut impl DerefMut<Target = FnvHashMap<i64, T>>,
    id: i64,
) -> SortKey<T> {
    SortKey::new(cache.get(&id).cloned(), id)
}

impl<T, F> CacheTracker for InMemCacheTracker<T, F>
where
    T: Ord + Clone + Debug + Send + Sync,
    F: Fn(Duration, BlockInfo) -> Option<T> + Send + Sync,
{
    /// called whenever blocks were accessed
    fn blocks_accessed(&self, blocks: Vec<BlockInfo>) {
        let now = Instant::now().checked_duration_since(self.created).unwrap();
        let mut cache = self.cache.lock();
        for block in blocks {
            if let Some(value) = (self.mk_cache_entry)(now, block) {
                cache.insert(block.id, value);
            } else {
                cache.remove(&block.id);
            }
        }
    }

    /// notification that these ids no longer have to be tracked
    fn blocks_deleted(&self, blocks: Vec<BlockInfo>) {
        let mut cache = self.cache.lock();
        for block in blocks {
            cache.remove(&block.id);
        }
    }

    /// notification that only these ids should be retained
    fn retain_ids(&self, ids: &[i64]) {
        let ids = ids.iter().cloned().collect::<FnvHashSet<_>>();
        let mut cache = self.cache.lock();
        cache.retain(|id, _| ids.contains(id));
    }

    /// sort ids by importance. More important ids should go to the end.
    fn sort_ids(&self, ids: &mut [i64]) {
        let mut cache = self.cache.lock();
        ids.sort_unstable_by_key(move |id| get_key(&mut cache, *id));
    }

    fn has_persistent_state(&self) -> bool {
        false
    }
}

impl<T: Debug, F> std::fmt::Debug for InMemCacheTracker<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemLruCacheTracker")
            .field("cache", &self.cache.lock())
            .finish()
    }
}

#[cfg(test)]
#[test]
fn sort_key_sort_order() {
    assert!(
        SortKey::new(None, i64::max_value())
            < SortKey::new(Some(Duration::default()), i64::min_value())
    );
}

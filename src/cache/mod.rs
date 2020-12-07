use fnv::{FnvHashMap, FnvHashSet};
use libipld::Cid;
use std::{
    fmt::Debug,
    ops::DerefMut,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
mod db;
pub use db::SqliteCacheTracker;
#[cfg(test)]
mod tests;

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

    fn delete_ids(&mut self, ids: &[i64]) {
        self.as_mut().delete_ids(ids)
    }

    fn retain_ids(&mut self, ids: &[i64]) {
        self.as_mut().retain_ids(ids)
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

/// keep track of block accesses in memory
pub struct InMemCacheTracker<T, F> {
    cache: Arc<Mutex<FnvHashMap<i64, T>>>,
    mk_cache_entry: F,
    created: Instant,
}

impl<T, F> InMemCacheTracker<T, F>
where
    T: Ord + Clone + Debug,
    F: Fn(Duration, &Cid, &[u8]) -> Option<T>,
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
    T: Ord + Clone + Debug,
    F: Fn(Duration, &Cid, &[u8]) -> Option<T>,
{
    /// called whenever blocks were accessed
    fn blocks_accessed(&mut self, blocks: &[(i64, &Cid, &[u8])]) {
        let now = Instant::now().checked_duration_since(self.created).unwrap();
        let mut cache = self.cache.lock().unwrap();
        for (id, cid, data) in blocks {
            if let Some(value) = (self.mk_cache_entry)(now, cid, data) {
                cache.insert(*id, value);
            } else {
                cache.remove(id);
            }
        }
    }

    /// notification that these ids no longer have to be tracked
    fn delete_ids(&mut self, ids: &[i64]) {
        let mut cache = self.cache.lock().unwrap();
        for id in ids {
            cache.remove(id);
        }
    }

    /// notification that only these ids should be retained
    fn retain_ids(&mut self, ids: &[i64]) {
        let ids = ids.iter().cloned().collect::<FnvHashSet<_>>();
        let mut cache = self.cache.lock().unwrap();
        cache.retain(|id, _| ids.contains(id));
    }

    /// sort ids by importance. More important ids should go to the end.
    fn sort_ids(&self, ids: &mut [i64]) {
        let mut cache = self.cache.lock().unwrap();
        ids.sort_unstable_by_key(move |id| get_key(&mut cache, *id));
    }
}

impl<T: Debug, F> std::fmt::Debug for InMemCacheTracker<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemLruCacheTracker")
            .field("cache", &self.cache.lock().unwrap())
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

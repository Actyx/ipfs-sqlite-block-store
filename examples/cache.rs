use fnv::{FnvHashMap, FnvHashSet};
use libipld::Cid;
use sqlite_block_store::{CacheTracker, Config, OwnedBlock, SizeTargets, Store};
use std::{cmp::Ord, fmt::Debug, ops::DerefMut, sync::{Arc, RwLock}, time::Duration, time::Instant, sync::Mutex};

/// keeps track of the last access time of blocks in memory
#[derive(Debug)]
pub struct InMemLruCacheTracker {
    cache: Arc<Mutex<FnvHashMap<i64, Duration>>>,
}

impl InMemLruCacheTracker {
    fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(FnvHashMap::default()))
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SortKey {
    time: Option<Duration>,
    id: i64,
}

impl SortKey {
    fn new(time: Option<Duration>, id: i64) -> Self {
        Self { time, id }
    }
}

fn get_key(cache: &mut impl DerefMut<Target = FnvHashMap<i64, Duration>>, id: i64) -> SortKey {
    SortKey::new(cache.get(&id).cloned(), id)
}

impl CacheTracker for InMemLruCacheTracker {
    /// called whenever blocks were accessed
    fn blocks_accessed(&mut self, blocks: &[(i64, &Cid, &[u8])]) {
        let now = Instant::now().elapsed();
        let mut cache = self.cache.lock().unwrap();
        for (id, _, _) in blocks {
            *cache.entry(*id).or_default() = now;
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

fn main() -> anyhow::Result<()> {
    assert!(
        SortKey::new(None, i64::max_value())
            < SortKey::new(Some(Duration::default()), i64::min_value())
    );
    let mut store =
        Store::memory(Config::default()
            .with_size_targets(SizeTargets::new(1000, 1000000))
            .with_cache_tracker(InMemLruCacheTracker::new())
        )?;
    Ok(())
}

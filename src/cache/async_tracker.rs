use super::{BlockInfo, CacheTracker};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

pub trait Spawner {
    fn spawn_blocking(&self, f: impl FnOnce() + Send + 'static);
}

pub struct AsyncCacheTracker<S, T> {
    spawner: S,
    inner: Arc<Mutex<T>>,
}

impl<S, T> Debug for AsyncCacheTracker<S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncCacheTracker").finish()
    }
}

impl<S: Spawner, T: CacheTracker> AsyncCacheTracker<S, T> {
    pub fn new(spawner: S, inner: T) -> Self {
        Self {
            spawner,
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl<S, T> CacheTracker for AsyncCacheTracker<S, T>
where
    S: Spawner,
    T: CacheTracker + Send + 'static,
{
    /// called whenever blocks were accessed
    fn blocks_accessed(&mut self, blocks: Vec<BlockInfo>) {
        let inner = self.inner.clone();
        self.spawner.spawn_blocking(move || {
            inner.lock().unwrap().blocks_accessed(blocks);
        });
    }
    /// called whenever blocks were written
    fn blocks_written(&mut self, blocks: Vec<BlockInfo>) {
        let inner = self.inner.clone();
        self.spawner.spawn_blocking(move || {
            inner.lock().unwrap().blocks_written(blocks);
        });
    }
    /// notification that these ids no longer have to be tracked
    fn delete_ids(&mut self, ids: &[i64]) {
        self.inner.lock().unwrap().delete_ids(ids);
    }
    /// notification that only these ids should be retained
    fn retain_ids(&mut self, ids: &[i64]) {
        self.inner.lock().unwrap().retain_ids(ids);
    }
    /// sort ids by importance. More important ids should go to the end.
    fn sort_ids(&self, ids: &mut [i64]) {
        self.inner.lock().unwrap().sort_ids(ids);
    }
}

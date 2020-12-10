use crate::{Block, BlockStore, StoreStats, TempPin};
use futures::future::BoxFuture;
use libipld::Cid;
use std::{
    iter::FromIterator,
    ops::DerefMut,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::*;

#[derive(Clone)]
pub struct AsyncBlockStore<R> {
    inner: Arc<Mutex<BlockStore>>,
    runtime: R,
}

impl AsyncTempPin {
    fn new(alias: TempPin) -> Self {
        Self(Arc::new(alias))
    }
}

/// a temp pin that can be freely cloned and shared
#[derive(Debug, Clone)]
pub struct AsyncTempPin(Arc<TempPin>);

/// Adapter for a runtime such as tokio or async_std
pub trait RuntimeAdapter {
    /// run a blocking block of code, most likely involving IO, on a different thread.
    fn unblock<F, T>(&self, f: F) -> BoxFuture<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;

    /// sleep for the given duration
    fn sleep(&self, duration: Duration) -> BoxFuture<()>;
}

impl<R: RuntimeAdapter> AsyncBlockStore<R> {
    /// Wrap a block store in an asyc wrapper
    ///
    /// `runtime` A runtime adapter for your runtime of choice
    /// `inner` The BlockStore to wrap
    pub fn new(runtime: R, inner: BlockStore) -> Self {
        Self {
            runtime,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn temp_pin(&self) -> AsyncTempPin {
        self.unblock(|store| AsyncTempPin::new(store.temp_pin()))
            .await
    }

    pub async fn alias(&self, name: Vec<u8>, link: Option<Cid>) -> crate::Result<()> {
        self.unblock(move |store| store.alias(&name, link.as_ref()))
            .await
    }

    pub async fn gc(&self) -> crate::Result<()> {
        self.unblock(|store| store.gc()).await
    }

    pub async fn incremental_gc(
        &self,
        min_blocks: usize,
        max_duration: Duration,
    ) -> crate::Result<bool> {
        self.unblock(move |store| store.incremental_gc(min_blocks, max_duration))
            .await
    }

    pub async fn incremental_delete_orphaned(
        &self,
        min_blocks: usize,
        max_duration: Duration,
    ) -> crate::Result<bool> {
        self.unblock(move |store| store.incremental_delete_orphaned(min_blocks, max_duration))
            .await
    }

    pub async fn get_block(&self, cid: Cid) -> crate::Result<Option<Vec<u8>>> {
        self.unblock(move |store| store.get_block(&cid)).await
    }

    pub async fn has_block(&self, cid: Cid) -> crate::Result<bool> {
        self.unblock(move |store| store.has_block(&cid)).await
    }

    pub async fn has_cid(&self, cid: Cid) -> crate::Result<bool> {
        self.unblock(move |store| store.has_cid(&cid)).await
    }

    pub async fn get_missing_blocks<C: FromIterator<Cid> + Send + 'static>(
        &self,
        cid: Cid,
    ) -> crate::Result<C> {
        self.unblock(move |store| store.get_missing_blocks(&cid))
            .await
    }

    pub async fn get_descendants<C: FromIterator<Cid> + Send + 'static>(
        &self,
        cid: Cid,
    ) -> crate::Result<C> {
        self.unblock(move |store| store.get_descendants(&cid)).await
    }

    pub async fn reverse_alias(&self, cid: Cid) -> crate::Result<Vec<Vec<u8>>> {
        self.unblock(move |store| store.reverse_alias(&cid)).await
    }

    pub async fn get_known_cids<C: FromIterator<Cid> + Send + 'static>(&self) -> crate::Result<C> {
        self.unblock(move |store| store.get_known_cids()).await
    }

    pub async fn get_block_cids<C: FromIterator<Cid> + Send + 'static>(&self) -> crate::Result<C> {
        self.unblock(move |store| store.get_block_cids()).await
    }

    pub async fn get_store_stats(&self) -> crate::Result<StoreStats> {
        self.unblock(move |store| store.get_store_stats()).await
    }

    pub async fn add_blocks<B: Block + Send + 'static>(
        &self,
        blocks: Vec<B>,
        alias: Option<AsyncTempPin>,
    ) -> crate::Result<()> {
        self.unblock(move |store| {
            let alias = alias.as_ref().map(|x| x.0.as_ref());
            store.add_blocks(blocks, alias)
        })
        .await
    }

    pub async fn add_block(
        &self,
        cid: Cid,
        data: Vec<u8>,
        links: Vec<Cid>,
        alias: Option<AsyncTempPin>,
    ) -> crate::Result<()> {
        self.unblock(move |store| {
            let alias = alias.as_ref().map(|x| x.0.as_ref());
            store.add_block(&cid, data.as_ref(), links, alias)
        })
        .await
    }

    /// helper to give a piece of code mutable, blocking access on the store
    fn unblock<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut BlockStore) -> T + Send + 'static,
    ) -> BoxFuture<T> {
        let inner = self.inner.clone();
        self.runtime
            .unblock(move || f(DerefMut::deref_mut(&mut inner.lock().unwrap())))
    }

    /// A gc loop that runs incremental gc in regular intervals
    ///
    /// Gc will run as long as this future is polled. GC is a two step process. First, the
    /// metadata of expendable non-pinned blocks will be deleted, then the actual data will
    /// be removed. This will run the first step and the second step interleaved to minimize
    /// gc interruptions.
    pub async fn gc_loop(self, config: GcConfig) -> crate::Result<()> {
        // initial delay so we don't start gc directly on startup
        self.runtime.sleep(config.interval / 2).await;
        loop {
            debug!("gc_loop running incremental gc");
            self.incremental_gc(config.min_blocks, config.target_duration)
                .await?;
            self.runtime.sleep(config.interval / 2).await;
            debug!("gc_loop running incremental delete orphaned");
            self.incremental_delete_orphaned(config.min_blocks, config.target_duration)
                .await?;
            self.runtime.sleep(config.interval / 2).await;
        }
    }
}

/// Configuration for the gc loop
///
/// This is done as a config struct since we might have additional parameters here in the future,
/// such as limits at which to do a full gc.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct GcConfig {
    /// interval at which gc runs
    ///
    /// note that this is implemented as delays between gcs, so it will not run exactly
    /// at this interval, but there will be some drift if gc takes long.
    pub interval: Duration,

    /// minimum number of blocks to collect in any case
    ///
    /// Using this parameter, it is possible to guarantee a minimum rate with which gc will be
    /// able to keep up. It is min_blocks / interval.
    pub min_blocks: usize,

    /// The target maximum gc duration of a single gc.
    ///
    /// This can not be guaranteed, since we guarantee to collect at least `min_blocks`.
    /// But as soon as this duration is exceeded, the incremental gc will stop doing additional
    /// work.
    pub target_duration: Duration,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            min_blocks: 10000,
            target_duration: Duration::from_secs(1),
        }
    }
}

use crate::{Block, BlockStore, StoreStats, TempAlias};
use futures::future::BoxFuture;
use libipld::Cid;
use std::{
    iter::FromIterator,
    ops::DerefMut,
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct AsyncBlockStore<U> {
    inner: Arc<Mutex<BlockStore>>,
    unblocker: U,
}

impl AsyncTempAlias {
    fn new(alias: TempAlias) -> Self {
        Self(Arc::new(alias))
    }
}

fn borrowed<'a, T>(x: &'a Option<T>) -> Option<&'a T> {
    match x {
        Some(v) => Some(v),
        None => None,
    }
}

/// a temp alias that can be freely cloned and shared
#[derive(Debug, Clone)]
pub struct AsyncTempAlias(Arc<TempAlias>);

pub trait Unblocker {
    fn unblock<F, T>(&self, f: F) -> BoxFuture<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;
}

impl<U: Unblocker> AsyncBlockStore<U> {
    pub fn new(unblocker: U, inner: BlockStore) -> Self {
        Self {
            unblocker,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn temp_alias(&self) -> AsyncTempAlias {
        self.unblock(|store| AsyncTempAlias::new(store.temp_alias()))
            .await
    }

    pub async fn alias(&self, name: Vec<u8>, link: Option<Cid>) -> crate::Result<()> {
        self.unblock(move |store| store.alias(&name, borrowed(&link)))
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
        alias: Option<AsyncTempAlias>,
    ) -> crate::Result<()> {
        self.unblock(move |store| {
            let alias = borrowed(&alias).map(|x| x.0.as_ref());
            store.add_blocks(blocks, alias)
        })
        .await
    }

    pub async fn add_block(
        &self,
        cid: Cid,
        data: Vec<u8>,
        links: Vec<Cid>,
        alias: Option<AsyncTempAlias>,
    ) -> crate::Result<()> {
        self.unblock(move |store| {
            let alias = borrowed(&alias).map(|x| x.0.as_ref());
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
        self.unblocker
            .unblock(move || f(DerefMut::deref_mut(&mut inner.lock().unwrap())))
    }
}

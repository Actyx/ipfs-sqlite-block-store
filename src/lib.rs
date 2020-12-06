mod cache;
mod cidbytes;
mod db;
mod error;
#[cfg(test)]
mod tests;

use crate::cidbytes::CidBytes;
pub use cache::{CacheTracker, NoopCacheTracker};
use db::*;
pub use error::{BlockStoreError, Result};
use libipld::cid::{self, Cid};
use rusqlite::{Connection, Transaction};
use std::{
    convert::TryFrom,
    fmt,
    iter::FromIterator,
    ops::DerefMut,
    path::Path,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct SizeTargets {
    /// target number of blocks.
    ///
    /// Up to this number, the store will retain everything even if not pinned.
    /// Once this number is exceeded, the store will run garbage collection of all
    /// unpinned blocks until the block criterion is met again.
    ///
    /// To completely disable storing of non-pinned blocks, set this to 0.
    /// Even then, the store will never delete pinned blocks.
    pub count: u64,

    /// target store size.
    ///
    /// Up to this size, the store will retain everything even if not pinned.
    /// Once this size is exceeded, the store will run garbage collection of all
    /// unpinned blocks until the size criterion is met again.
    ///
    /// The store will never delete pinned blocks.
    pub size: u64,
}

impl SizeTargets {
    pub fn new(count: u64, size: u64) -> Self {
        Self { count, size }
    }
}

#[derive(Debug)]
pub struct Config {
    size_targets: SizeTargets,
    cache_tracker: Box<dyn CacheTracker>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            size_targets: Default::default(),
            cache_tracker: Box::new(NoopCacheTracker),
        }
    }
}

impl Config {
    pub fn with_size_targets(mut self, size_targets: SizeTargets) -> Self {
        self.size_targets = size_targets;
        self
    }
    pub fn with_cache_tracker<T: CacheTracker + 'static>(mut self, cache_tracker: T) -> Self {
        self.cache_tracker = Box::new(cache_tracker);
        self
    }
}

pub struct Store {
    conn: Connection,
    expired_temp_aliases: Arc<Mutex<Vec<i64>>>,
    config: Config,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// number of blocks, excluding orphaned blocks
    count: u64,
    /// total size of blocks, excluding orphaned blocks
    size: u64,
}

/// a handle that contains a temporary alias
///
/// dropping this handle enqueue the alias for dropping before the next gc.
///
/// Note that implementing Clone for this would be a mistake.
pub struct TempAlias {
    id: AtomicI64,
    expired_temp_aliases: Arc<Mutex<Vec<i64>>>,
}

/// dump the temp alias id so you can find it in the database
impl fmt::Debug for TempAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.id.load(Ordering::SeqCst);
        let mut builder = f.debug_struct("TempAlias");
        if id > 0 {
            builder.field("id", &id);
        }
        builder.finish()
    }
}

impl Drop for TempAlias {
    fn drop(&mut self) {
        let id = self.id.get_mut();
        let alias = *id;
        if alias > 0 {
            // not sure if we have to guard against double drop, but it certainly does not hurt.
            *id = 0;
            self.expired_temp_aliases.lock().unwrap().push(alias);
        }
    }
}

/// execute a statement in a write transaction
fn in_txn<T>(conn: &mut Connection, f: impl FnOnce(&Transaction) -> Result<T>) -> Result<T> {
    let txn = conn.transaction()?;
    let result = f(&txn);
    if result.is_ok() {
        txn.commit()?;
    }
    result
}

/// execute a statement in a readonly transaction
/// nested transactions are not allowed here.
fn in_ro_txn<T>(conn: &Connection, f: impl FnOnce(&Transaction) -> Result<T>) -> Result<T> {
    let txn = conn.unchecked_transaction()?;
    f(&txn)
}

pub trait Block {
    type I: Iterator<Item = Cid>;
    fn cid(&self) -> &Cid;
    fn data(&self) -> &[u8];
    fn links(&self) -> Self::I;
}

pub struct OwnedBlock {
    cid: Cid,
    data: Vec<u8>,
    links: Vec<Cid>,
}

impl OwnedBlock {
    pub fn new(cid: Cid, data: Vec<u8>, links: Vec<Cid>) -> Self {
        Self { cid, data, links }
    }
}

impl Block for OwnedBlock {
    type I = std::vec::IntoIter<Cid>;

    fn cid(&self) -> &Cid {
        &self.cid
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn links(&self) -> Self::I {
        self.links.clone().into_iter()
    }
}

struct BorrowedBlock<'a, F> {
    cid: Cid,
    data: &'a [u8],
    links: F,
}

impl<'a, F, I> BorrowedBlock<'a, F>
where
    F: Fn() -> I,
    I: Iterator<Item = Cid>,
{
    fn new(cid: Cid, data: &'a [u8], links: F) -> Self {
        Self { cid, data, links }
    }
}

impl<'a, F, I> Block for BorrowedBlock<'a, F>
where
    F: Fn() -> I,
    I: Iterator<Item = Cid>,
{
    type I = I;

    fn cid(&self) -> &Cid {
        &self.cid
    }

    fn data(&self) -> &[u8] {
        self.data
    }

    fn links(&self) -> Self::I {
        (self.links)()
    }
}

impl Store {
    pub fn memory(config: Config) -> crate::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
            config,
        })
    }

    pub fn open(path: impl AsRef<Path>, config: Config) -> crate::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
            config,
        })
    }

    pub fn temp_alias(&self) -> TempAlias {
        TempAlias {
            id: AtomicI64::new(0),
            expired_temp_aliases: self.expired_temp_aliases.clone(),
        }
    }

    pub fn alias(&mut self, name: impl AsRef<[u8]>, link: Option<&Cid>) -> crate::Result<()> {
        let link: Option<CidBytes> = link.map(CidBytes::try_from).transpose()?;
        in_txn(&mut self.conn, |txn| {
            alias(txn, name.as_ref(), link.as_ref())
        })
    }

    pub fn has_cid(&self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        in_ro_txn(&self.conn, |txn| has_cid(txn, cid))
    }

    pub fn has_block(&mut self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        in_ro_txn(&self.conn, |txn| has_block(txn, cid))
    }

    pub fn get_store_stats(&self) -> Result<StoreStats> {
        in_ro_txn(&self.conn, get_store_stats)
    }

    pub fn get_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        let res = in_ro_txn(&self.conn, |txn| Ok(get_cids::<CidBytes>(txn)?))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    pub fn get_descendants<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let res = in_ro_txn(&self.conn, move |txn| get_descendants(txn, cid))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    pub fn get_missing_blocks<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let result = log_execution_time("get_missing_blocks", Duration::from_millis(10), || {
            in_ro_txn(&self.conn, move |txn| get_missing_blocks(txn, cid))
        })?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<C>>()?;
        Ok(res)
    }

    pub fn gc(&mut self) -> Result<()> {
        loop {
            let complete = self.incremental_gc(20000, Duration::from_secs(1))?;
            while !self.incremental_delete_orphaned(20000, Duration::from_secs(1))? {}
            if complete {
                break;
            }
        }
        Ok(())
    }
    pub fn incremental_gc(&mut self, min_blocks: usize, max_duration: Duration) -> Result<bool> {
        // atomically grab the expired_temp_aliases until now
        let expired_temp_aliases = {
            let mut result = Vec::new();
            std::mem::swap(
                self.expired_temp_aliases.lock().unwrap().deref_mut(),
                &mut result,
            );
            result
        };
        Ok(log_execution_time("gc", Duration::from_secs(1), || {
            let size_targets = self.config.size_targets;
            let cache_tracker = &mut self.config.cache_tracker;
            in_txn(&mut self.conn, move |txn| {
                // get rid of dropped temp aliases, this should be fast
                for id in expired_temp_aliases {
                    delete_temp_alias(txn, id)?;
                }
                Ok(incremental_gc(
                    &txn,
                    min_blocks,
                    max_duration,
                    size_targets,
                    cache_tracker,
                )?)
            })
        })?)
    }
    pub fn incremental_delete_orphaned(
        &mut self,
        min_blocks: usize,
        max_duration: Duration,
    ) -> Result<bool> {
        Ok(log_execution_time(
            "delete_orphaned",
            Duration::from_millis(100),
            || {
                in_txn(&mut self.conn, move |txn| {
                    Ok(incremental_delete_orphaned(txn, min_blocks, max_duration)?)
                })
            },
        )?)
    }
    pub fn add_blocks<B: Block>(
        &mut self,
        blocks: impl IntoIterator<Item = B>,
        alias: Option<&TempAlias>,
    ) -> Result<()> {
        let blocks = blocks.into_iter().collect::<Vec<_>>();
        let written = in_txn(&mut self.conn, |txn| {
            let alias = alias.map(|alias| &alias.id);
            Ok(blocks
                .iter()
                .map(|block| {
                    let cid_bytes = CidBytes::try_from(block.cid())?;
                    let links = block
                        .links()
                        .map(|x| CidBytes::try_from(&x))
                        .collect::<std::result::Result<Vec<_>, cid::Error>>()?;
                    let id = add_block(txn, &cid_bytes, &block.data(), links, alias)?;
                    Ok((id, block.cid(), block.data()))
                })
                .collect::<Result<Vec<_>>>()?)
        })?;
        self.config.cache_tracker.blocks_written(&written);
        Ok(())
    }
    pub fn add_block<I>(
        &mut self,
        cid: &Cid,
        data: &[u8],
        links: I,
        alias: Option<&TempAlias>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = Cid> + Clone,
    {
        let block = BorrowedBlock::new(cid.clone(), data, move || links.clone().into_iter());
        self.add_blocks(Some(block), alias)?;
        Ok(())
    }
    pub fn get_block(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let cid_bytes = CidBytes::try_from(cid)?;
        let result = in_ro_txn(&self.conn, |txn| get_block(txn, cid_bytes))?;
        Ok(result.map(|(id, block)| {
            // track the cache access
            self.config
                .cache_tracker
                .blocks_accessed(&[(id, cid, block.as_ref())]);
            block
        }))
    }
}

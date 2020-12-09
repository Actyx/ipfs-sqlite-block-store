pub mod async_block_store;
pub mod cache;
mod cidbytes;
mod db;
mod error;
#[cfg(test)]
mod tests;

use crate::cidbytes::CidBytes;
use cache::{BlockInfo, CacheTracker, NoopCacheTracker};
use db::*;
pub use error::{BlockStoreError, Result};
use libipld::cid::{self, Cid};
use rusqlite::{Connection, DatabaseName, Transaction};
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
use tracing::*;

/// Size targets for a store. Gc of non-pinned blocks will start once one of the size targets is exceeded.
///
/// There are targets for both block count and block size. The reason for this is that a store that has
/// a very large number of tiny blocks will become sluggish despite not having a large total size.
///
/// Size targets only apply to non-pinned blocks. Pinned blocks will never be gced even if exceeding one of the
/// size targets.
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

    pub fn exceeded(&self, stats: &StoreStats) -> bool {
        stats.count > self.count || stats.size > self.size
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
    /// Set size targets for the store
    pub fn with_size_targets(mut self, size_targets: SizeTargets) -> Self {
        self.size_targets = size_targets;
        self
    }
    /// Set strategy for which non-pinned blocks to keep in case one of the size targets is exceeded.
    pub fn with_cache_tracker<T: CacheTracker + 'static>(mut self, cache_tracker: T) -> Self {
        self.cache_tracker = Box::new(cache_tracker);
        self
    }
}

pub struct BlockStore {
    conn: Connection,
    expired_temp_aliases: Arc<Mutex<Vec<i64>>>,
    config: Config,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreStats {
    count: u64,
    size: u64,
}

impl StoreStats {
    /// Total number of blocks in the store
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Total size of blocks in the store
    pub fn size(&self) -> u64 {
        self.size
    }
}

// do not implement Clone for this!
/// a handle that contains a temporary alias
///
/// dropping this handle enqueue the alias for dropping before the next gc.
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

/// An ipfs block
pub trait Block {
    type I: Iterator<Item = Cid>;
    fn cid(&self) -> &Cid;
    fn data(&self) -> &[u8];
    fn links(&self) -> Self::I;
}

/// Block that owns its data
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

impl BlockStore {
    /// Create an in memory block store with the given config
    pub fn memory(config: Config) -> anyhow::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        init_db(&mut conn, true)?;
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
            config,
        })
    }

    /// Create a persistent block store with the given config
    pub fn open(path: impl AsRef<Path>, mut config: Config) -> anyhow::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(&mut conn, false)?;
        let ids = in_txn(&mut conn, |txn| get_ids(txn))?;
        config.cache_tracker.retain_ids(&ids);
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
            config,
        })
    }

    pub fn open_test(path: impl AsRef<Path>, mut config: Config) -> anyhow::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        debug!(
            "Restoring in memory database from {}",
            path.as_ref().display()
        );
        conn.restore(
            DatabaseName::Main,
            path,
            Some(|p: rusqlite::backup::Progress| {
                let percent = (p.pagecount - p.remaining) * 100 / p.pagecount;
                if percent % 10 == 0 {
                    debug!("Restoring: {} %", percent);
                }
            }),
        )?;
        let ids = in_txn(&mut conn, |txn| get_ids(txn))?;
        config.cache_tracker.retain_ids(&ids);
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
            config,
        })
    }

    pub fn integrity_check(&self) -> crate::Result<()> {
        let result = integrity_check(&self.conn)?;
        if result == vec!["ok".to_owned()] {
            Ok(())
        } else {
            let error_text = result.join(";");
            Err(crate::error::BlockStoreError::SqliteError(
                rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(11), Some(error_text)),
            ))
        }
    }

    /// Get a temporary alias for safely adding blocks to the store
    pub fn temp_alias(&self) -> TempAlias {
        TempAlias {
            id: AtomicI64::new(0),
            expired_temp_aliases: self.expired_temp_aliases.clone(),
        }
    }

    /// Add a permanent named alias/pin for a root
    pub fn alias(&mut self, name: impl AsRef<[u8]>, link: Option<&Cid>) -> crate::Result<()> {
        let link: Option<CidBytes> = link.map(CidBytes::try_from).transpose()?;
        in_txn(&mut self.conn, |txn| {
            alias(txn, name.as_ref(), link.as_ref())
        })
    }

    /// Returns the aliases referencing a block.
    pub fn reverse_alias(&mut self, cid: &Cid) -> crate::Result<Vec<Vec<u8>>> {
        let cid = CidBytes::try_from(cid)?;
        in_txn(&mut self.conn, |txn| reverse_alias(txn, cid.as_ref()))
    }

    /// Checks if the store knows about the cid.
    /// Note that this does not necessarily mean that the store has the data for the cid.
    pub fn has_cid(&self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        in_ro_txn(&self.conn, |txn| has_cid(txn, cid))
    }

    /// Checks if the store has the data for a cid
    pub fn has_block(&mut self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        in_ro_txn(&self.conn, |txn| has_block(txn, cid))
    }

    /// Get the stats for the store.
    ///
    /// The stats are kept up to date, so this is fast.
    pub fn get_store_stats(&self) -> Result<StoreStats> {
        in_ro_txn(&self.conn, get_store_stats)
    }

    /// Get all cids that the store knows about
    pub fn get_known_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        let res = in_ro_txn(&self.conn, |txn| Ok(get_known_cids::<CidBytes>(txn)?))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Get all cids for which the store has blocks
    pub fn get_block_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        let res = in_ro_txn(&self.conn, |txn| Ok(get_block_cids::<CidBytes>(txn)?))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Get descendants of a cid
    pub fn get_descendants<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let res = in_ro_txn(&self.conn, move |txn| get_descendants(txn, cid))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Given a root of a dag, gives all cids which we do not have data for.
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

    /// do a full garbage collection
    ///
    /// for a large block store, this can take several seconds to minutes. If that is not acceptable,
    /// consider using incremental gc.
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
    /// Perform an incremental garbage collection.
    ///
    /// Will collect unpinned blocks until either the size targets are met again, or at minimum
    /// `min_blocks` blocks are collected. Then it will continue connecting blocks until `max_duration`
    /// is elapsed.
    ///
    /// Note that this might significantly exceed `max_duration` for various reasons. Also note that
    /// when doing incremental gc, the actual blocks are not yet deleted. So a call to this method
    /// should usually be followed by a call to incremental_delete_orphaned.
    ///
    /// - `min_blocks` the minium number of blocks to collect in any case
    /// - `max_duration` the maximum duration that should be spent on gc
    ///
    /// Returns true if either size targets are met or there are no unpinned blocks left.
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
    /// Incrementally delete orphaned blocks
    ///
    /// Orphaned blocks are blocks for which we have deleted the metadata in `incremental_gc`.
    ///
    /// Will delete orphaned blocks until either all orphaned blocks are deleted, or at minimum
    /// `min_blocks` blocks are deleted. Then it will continue deleting blocks until `max_duration`
    /// is elapsed.
    ///
    /// Note that this might significantly exceed `max_duration` for various reasons.
    ///
    /// - `min_blocks` the minium number of blocks to delete in any case
    /// - `max_duration` the maximum duration that should be spent on gc
    ///
    /// Returns true if all orphaned blocks are deleted
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
    /// Add a number of blocks to the store
    ///
    /// It is up to the caller to extract links from blocks. Also, the store does not know
    /// anything about content-addressing and will not validate that the cid of a block is the
    /// actual hash of the content.
    ///
    /// - `blocks` the blocks to add.
    ///   Even we already have these blocks, the alias will be set. However, it will not be checked
    ///   that the links or data are the same as last time the block was added. That is responsibility
    ///   of the caller.
    /// - `alias` an optional temporary alias.
    ///   This can be used to incrementally add blocks without having to worry about them being garbage
    ///   collected before they can be pinned with a permanent alias.
    pub fn add_blocks<B: Block>(
        &mut self,
        blocks: impl IntoIterator<Item = B>,
        alias: Option<&TempAlias>,
    ) -> Result<()> {
        let infos = in_txn(&mut self.conn, |txn| {
            let alias = alias.map(|alias| &alias.id);
            Ok(blocks
                .into_iter()
                .map(|block| {
                    let cid_bytes = CidBytes::try_from(block.cid())?;
                    let links = block
                        .links()
                        .map(|x| CidBytes::try_from(&x))
                        .collect::<std::result::Result<Vec<_>, cid::Error>>()?;
                    let id = add_block(txn, &cid_bytes, &block.data(), links, alias)?;
                    Ok(BlockInfo::new(id, block.cid(), block.data()))
                })
                .collect::<Result<Vec<_>>>()?)
        })?;
        self.config.cache_tracker.blocks_written(infos);
        Ok(())
    }
    /// Add a single block
    ///
    /// this is just a convenience method that calls add_blocks internally.
    ///
    /// - `cid` the cid
    ///   This should be a hash of the data, with some format specifier.
    /// - `data` a blob
    /// - `links` links extracted from the data
    /// - `alias` an optional temporary alias
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
        let block = BorrowedBlock::new(*cid, data, move || links.clone().into_iter());
        self.add_blocks(Some(block), alias)?;
        Ok(())
    }
    /// Get data for a block
    ///
    /// Will return None if we don't have the data
    pub fn get_block(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let cid_bytes = CidBytes::try_from(cid)?;
        let result = in_ro_txn(&self.conn, |txn| get_block(txn, cid_bytes))?;
        Ok(result.map(|(id, block)| {
            // track the cache access
            self.config
                .cache_tracker
                .blocks_accessed(vec![BlockInfo::new(id, cid, block.as_ref())]);
            block
        }))
    }
}

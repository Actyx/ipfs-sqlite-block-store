//! # IPFS sqlite block store
//!
//! A block store for a rust implementation of [ipfs](https://ipfs.io/).
//!
//! # Concepts
//!
//! ## Aliases
//!
//! An alias is a named pin of a root. When a root is aliased, none of the leaves of the dag pointed
//! to by the root will be collected by gc. However, a root being aliased does not mean that the dag
//! must be complete.
//!
//! ## Temporary aliases
//!
//! A temporary alias is an unnamed alias that is just for the purpose of protecting blocks from gc
//! while a large tree is being constructed. While an alias maps a single name to a single root, a
//! temporary alias can be assigned to an arbitary number of blocks before the dag is finished.
//!
//! A temporary alias will be deleted as soon as the handle goes out of scope.
//!
//! ## Garbage Collection (GC)
//!
//! GC refers to the process of removing unpinned blocks. It runs only when the configured size
//! targets are exceeded. [Size targets](SizeTargets) contain both the total size of the store
//! and the number of blocks.
//!
//! GC will run incrementally, deleting blocks until the size targets are no longer exceeded. The
//! order in which unpinned blocks will be deleted can be customized.
//!
//! ## Caching
//!
//! For unpinned blocks, it is possible to customize which blocks have the highest value using a
//! [CacheTracker](cache::CacheTracker). The default is to [do nothing](cache::NoopCacheTracker)
//! and has no performance overhead.
//!
//! The most elaborate implemented strategy is to keep track of access times in a separate database,
//! via the [SqliteCacheTracker](cache::SqliteCacheTracker), which has a slight performance overhead.
//!
//! The performance overhead of writing to an access tracking database on each block read can be
//! mitigated by using the [AsyncCacheTracker](cache::AsyncCacheTracker) wrapper to perform the database
//! writes on a different thread.
//!
//! # Usage
//!
//! ## Blocking
//!
//! For blocking usage, use [BlockStore](BlockStore). This is the most low level interface.
//!
//! ## Non-blocking
//!
//! For non-blocking usage, use [AsyncBlockStore](async_block_store::AsyncBlockStore). This is a
//! wrapper that is meant to be used from async rust. In addition to wrapping most methods of
//! [BlockStore], it provides a method [gc_loop](async_block_store::AsyncBlockStore::gc_loop) to
//! run gc continuously.
//!
//! # Major differences to the go-ipfs pinning concept
//!
//! - Pinning/aliasing a root does not require that the dag is complete
//! - Aliases/named pins as opposed to unnamed and non-reference-counted pins
//! - Temporary pins as a mechanism to keep blocks safe from gc while a tree is being constructed
pub mod async_block_store;
pub mod cache;
mod cidbytes;
mod db;
mod error;
#[cfg(test)]
mod tests;

use crate::cidbytes::CidBytes;
use cache::{BlockInfo, CacheTracker, NoopCacheTracker, WriteInfo};
use db::*;
pub use error::{BlockStoreError, Result};
use libipld::{
    cid::{self, Cid},
    codec::Codec,
    store::StoreParams,
    Ipld, IpldCodec,
};
use parking_lot::Mutex;
use rusqlite::{Connection, DatabaseName};
use std::{
    convert::TryFrom,
    fmt,
    iter::FromIterator,
    ops::DerefMut,
    path::Path,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
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

    /// Size targets that can not be reached. This can be used to disable gc.
    pub fn max_value() -> Self {
        Self {
            count: u64::max_value(),
            size: u64::max_value(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Synchronous {
    // this is the most conservative mode. This only works if we have few, large transactions
    Full,
    Normal,
    Off,
}

impl fmt::Display for Synchronous {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Synchronous::Full => "FULL",
            Synchronous::Normal => "NORMAL",
            Synchronous::Off => "OFF",
        })
    }
}

#[derive(Debug)]
pub struct Config {
    size_targets: SizeTargets,
    cache_tracker: Box<dyn CacheTracker>,
    pragma_synchronous: Synchronous,
    pragma_cache_pages: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            size_targets: Default::default(),
            cache_tracker: Box::new(NoopCacheTracker),
            pragma_synchronous: Synchronous::Full, // most conservative setting
            pragma_cache_pages: 8192, // 32 megabytes with the default page size of 4096
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
    pub fn with_pragma_synchronous(mut self, value: Synchronous) -> Self {
        self.pragma_synchronous = value;
        self
    }
    pub fn with_pragma_cache_pages(mut self, value: u64) -> Self {
        self.pragma_cache_pages = value;
        self
    }
}

pub struct BlockStore {
    conn: Connection,
    expired_temp_pins: Arc<Mutex<Vec<i64>>>,
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
/// a handle that contains a temporary pin
///
/// dropping this handle enqueue the pin for dropping before the next gc.
pub struct TempPin {
    id: AtomicI64,
    expired_temp_pins: Arc<Mutex<Vec<i64>>>,
}

/// dump the temp alias id so you can find it in the database
impl fmt::Debug for TempPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.id.load(Ordering::SeqCst);
        let mut builder = f.debug_struct("TempAlias");
        if id > 0 {
            builder.field("id", &id);
        }
        builder.finish()
    }
}

impl Drop for TempPin {
    fn drop(&mut self) {
        let id = self.id.get_mut();
        let alias = *id;
        if alias > 0 {
            // not sure if we have to guard against double drop, but it certainly does not hurt.
            *id = 0;
            self.expired_temp_pins.lock().push(alias);
        }
    }
}

/// An ipfs block
pub trait Block {
    fn cid(&self) -> &Cid;
    fn data(&self) -> &[u8];
}

impl<S: StoreParams> Block for libipld::Block<S> {
    fn cid(&self) -> &Cid {
        libipld::Block::cid(&self)
    }

    fn data(&self) -> &[u8] {
        libipld::Block::data(&self)
    }
}

/// Block that owns its data
#[derive(Debug, Clone)]
pub struct OwnedBlock {
    cid: Cid,
    data: Box<[u8]>,
}

impl OwnedBlock {
    pub fn new(cid: Cid, data: impl Into<Box<[u8]>>) -> Self {
        Self {
            cid,
            data: data.into(),
        }
    }
}

impl Block for OwnedBlock {
    fn cid(&self) -> &Cid {
        &self.cid
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
}

struct BorrowedBlock<'a> {
    cid: Cid,
    data: &'a [u8],
}

impl<'a> BorrowedBlock<'a> {
    fn new(cid: Cid, data: &'a [u8]) -> Self {
        Self { cid, data }
    }
}

impl<'a> Block for BorrowedBlock<'a> {
    fn cid(&self) -> &Cid {
        &self.cid
    }

    fn data(&self) -> &[u8] {
        self.data
    }
}

fn links(block: &impl Block) -> anyhow::Result<Vec<Cid>> {
    let mut links = Vec::new();
    IpldCodec::try_from(block.cid().codec())?
        .references::<Ipld, Vec<_>>(&block.data(), &mut links)?;
    Ok(links)
}

impl BlockStore {
    /// Create an in memory block store with the given config
    pub fn memory(config: Config) -> crate::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        init_db(
            &mut conn,
            true,
            config.pragma_cache_pages as i64,
            config.pragma_synchronous,
        )?;
        Ok(Self {
            conn,
            expired_temp_pins: Arc::new(Mutex::new(Vec::new())),
            config,
        })
    }

    /// Create a persistent block store with the given config
    pub fn open(path: impl AsRef<Path>, config: Config) -> crate::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(
            &mut conn,
            false,
            config.pragma_cache_pages as i64,
            config.pragma_synchronous,
        )?;
        let ids = in_txn(&mut conn, |txn| get_ids(txn))?;
        config.cache_tracker.retain_ids(&ids);
        Ok(Self {
            conn,
            expired_temp_pins: Arc::new(Mutex::new(Vec::new())),
            config,
        })
    }

    /// Open the file at the given path for testing.
    ///
    /// This will create a writeable in-memory database that is initialized with the content
    /// of the file at the given path.
    pub fn open_test(path: impl AsRef<Path>, config: Config) -> crate::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        debug!(
            "Restoring in memory database from {}",
            path.as_ref().display()
        );
        conn.restore(
            DatabaseName::Main,
            path,
            Some(|p: rusqlite::backup::Progress| {
                let percent = if p.pagecount == 0 {
                    100
                } else {
                    (p.pagecount - p.remaining) * 100 / p.pagecount
                };
                if percent % 10 == 0 {
                    debug!("Restoring: {} %", percent);
                }
            }),
        )?;
        let ids = in_txn(&mut conn, |txn| get_ids(txn))?;
        config.cache_tracker.retain_ids(&ids);
        Ok(Self {
            conn,
            expired_temp_pins: Arc::new(Mutex::new(Vec::new())),
            config,
        })
    }

    pub fn flush(&self) -> crate::Result<()> {
        // TODO: check if this works! We are always in WAL mode.
        // https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
        Ok(self.conn.pragma_update(None, "wal_checkpoint", &"FULL")?)
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
    pub fn temp_pin(&self) -> TempPin {
        TempPin {
            id: AtomicI64::new(0),
            expired_temp_pins: self.expired_temp_pins.clone(),
        }
    }

    /// Add a permanent named alias/pin for a root
    pub fn alias(&mut self, name: impl AsRef<[u8]>, link: Option<&Cid>) -> crate::Result<()> {
        self.alias_many(std::iter::once((name, link.cloned())))
    }

    /// Resolves an alias to a cid.
    pub fn resolve(&self, name: impl AsRef<[u8]>) -> crate::Result<Option<Cid>> {
        in_ro_txn(&self.conn, |txn| {
            Ok(resolve::<CidBytes>(txn, name.as_ref())?
                .map(|c| Cid::try_from(&c))
                .transpose()?)
        })
    }

    /// Add multiple permanent named aliases
    pub fn alias_many(
        &mut self,
        aliases: impl IntoIterator<Item = (impl AsRef<[u8]>, Option<Cid>)>,
    ) -> crate::Result<()> {
        in_txn(&mut self.conn, |txn| {
            for (name, link) in aliases.into_iter() {
                let link: Option<CidBytes> = link.map(|x| CidBytes::try_from(&x)).transpose()?;
                alias(txn, name.as_ref(), link.as_ref())?;
            }
            Ok(())
        })
    }

    pub fn assign_temp_pin(
        &mut self,
        pin: &TempPin,
        links: impl IntoIterator<Item = Cid>,
    ) -> crate::Result<()> {
        let pin0 = pin.id.load(Ordering::SeqCst);
        let pin0 = in_txn(&mut self.conn, |txn| {
            let links = links
                .into_iter()
                .map(|x| CidBytes::try_from(&x))
                .collect::<std::result::Result<Vec<_>, cid::Error>>()?;
            assign_temp_pin(txn, pin0, links)
        })?;
        pin.id.store(pin0, Ordering::SeqCst);
        Ok(())
    }

    /// Returns the aliases referencing a block.
    pub fn reverse_alias(&self, cid: &Cid) -> crate::Result<Option<Vec<Vec<u8>>>> {
        let cid = CidBytes::try_from(cid)?;
        in_ro_txn(&self.conn, |txn| reverse_alias(txn, cid.as_ref()))
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

    /// Look up multiple blocks in one read transaction
    pub fn has_blocks<I, O>(&self, cids: I) -> Result<O>
    where
        I: IntoIterator<Item = Cid>,
        O: FromIterator<(Cid, bool)>,
    {
        in_ro_txn(&self.conn, |txn| {
            cids.into_iter()
                .map(|cid| -> Result<(Cid, bool)> {
                    Ok((cid, has_block(txn, CidBytes::try_from(&cid)?)?))
                })
                .collect::<crate::Result<O>>()
        })
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
    pub fn get_block_cids<C: FromIterator<Cid>>(&self) -> Result<C> {
        let res = in_ro_txn(&self.conn, |txn| Ok(get_block_cids::<CidBytes>(txn)?))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Get descendants of a cid
    pub fn get_descendants<C: FromIterator<Cid>>(&self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let res = in_ro_txn(&self.conn, move |txn| get_descendants(txn, cid))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Given a root of a dag, gives all cids which we do not have data for.
    pub fn get_missing_blocks<C: FromIterator<Cid>>(&self, cid: &Cid) -> Result<C> {
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
        // atomically grab the expired_temp_pins until now
        let expired_temp_pins = {
            let mut result = Vec::new();
            std::mem::swap(self.expired_temp_pins.lock().deref_mut(), &mut result);
            result
        };
        let (deleted, complete) = log_execution_time("gc", Duration::from_secs(1), || {
            let size_targets = self.config.size_targets;
            let cache_tracker = &self.config.cache_tracker;
            in_txn(&mut self.conn, move |txn| {
                // get rid of dropped temp aliases, this should be fast
                for id in expired_temp_pins {
                    delete_temp_pin(txn, id)?;
                }
                Ok(incremental_gc(
                    &txn,
                    min_blocks,
                    max_duration,
                    size_targets,
                    cache_tracker,
                )?)
            })
        })?;
        self.config.cache_tracker.blocks_deleted(deleted);
        Ok(complete)
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
    pub fn put_blocks<B: Block>(
        &mut self,
        blocks: impl IntoIterator<Item = B>,
        pin: Option<&TempPin>,
    ) -> Result<()> {
        let mut pin0 = pin.map(|pin| pin.id.load(Ordering::SeqCst));
        let infos = in_txn(&mut self.conn, |txn| {
            Ok(blocks
                .into_iter()
                .map(|block| {
                    let cid_bytes = CidBytes::try_from(block.cid())?;
                    let links = links(&block)?
                        .iter()
                        .map(CidBytes::try_from)
                        .collect::<std::result::Result<Vec<_>, cid::Error>>()?;
                    let res = put_block(txn, &cid_bytes, &block.data(), links, &mut pin0)?;
                    Ok(WriteInfo::new(
                        BlockInfo::new(res.id, block.cid(), block.data().len()),
                        res.block_exists,
                    ))
                })
                .collect::<Result<Vec<_>>>()?)
        })?;
        if let (Some(pin), Some(p)) = (pin, pin0) {
            pin.id.store(p, Ordering::SeqCst);
        }
        self.config.cache_tracker.blocks_written(infos);
        Ok(())
    }
    /// Add a single block
    ///
    /// this is just a convenience method that calls put_blocks internally.
    ///
    /// - `cid` the cid
    ///   This should be a hash of the data, with some format specifier.
    /// - `data` a blob
    /// - `links` links extracted from the data
    /// - `alias` an optional temporary alias
    pub fn put_block(&mut self, block: &impl Block, alias: Option<&TempPin>) -> Result<()> {
        let bb = BorrowedBlock::new(*block.cid(), block.data());
        self.put_blocks(Some(bb), alias)?;
        Ok(())
    }
    /// Get multiple blocks in a single read transaction
    pub fn get_blocks<I>(&self, cids: I) -> Result<impl Iterator<Item = (Cid, Option<Vec<u8>>)>>
    where
        I: IntoIterator<Item = Cid>,
    {
        let res = in_ro_txn(&self.conn, |txn| {
            cids.into_iter()
                .map(|cid| Ok((cid, get_block(txn, &CidBytes::try_from(&cid)?)?)))
                .collect::<crate::Result<Vec<_>>>()
        })?;
        let infos = res
            .iter()
            .filter_map(|(cid, res)| {
                res.as_ref()
                    .map(|(id, data)| BlockInfo::new(*id, cid, data.len()))
            })
            .collect::<Vec<_>>();
        self.config.cache_tracker.blocks_accessed(infos);
        Ok(res
            .into_iter()
            .map(|(cid, res)| (cid, res.map(|(_, data)| data))))
    }
    /// Get data for a block
    ///
    /// Will return None if we don't have the data
    pub fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        Ok(self.get_blocks(std::iter::once(*cid))?.next().unwrap().1)
    }
}

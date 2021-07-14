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
//! # Major differences to the go-ipfs pinning concept
//!
//! - Pinning/aliasing a root does not require that the dag is complete
//! - Aliases/named pins as opposed to unnamed and non-reference-counted pins
//! - Temporary pins as a mechanism to keep blocks safe from gc while a tree is being constructed
pub mod cache;
mod cidbytes;
mod db;
mod error;
mod memcache;
#[cfg(test)]
mod tests;
mod transaction;

use cache::{CacheTracker, NoopCacheTracker};
use db::*;
pub use error::{BlockStoreError, Result};
use libipld::{cid::Cid, codec::References, store::StoreParams, Block, Ipld};
use memcache::MemCache;
use parking_lot::Mutex;
use rusqlite::{Connection, DatabaseName, OpenFlags};
use std::{
    fmt,
    iter::FromIterator,
    marker::PhantomData,
    mem,
    ops::DerefMut,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing::*;
pub use transaction::Transaction;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DbPath {
    File(PathBuf),
    Memory,
}

impl DbPath {
    fn is_memory(&self) -> bool {
        !matches!(self, DbPath::File(_))
    }
}

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
    cache_tracker: Arc<dyn CacheTracker>,
    pragma_synchronous: Synchronous,
    pragma_cache_pages: u64,
    // open in readonly mode
    read_only: bool,
    // create if it does not yet exist
    create: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            size_targets: Default::default(),
            cache_tracker: Arc::new(NoopCacheTracker),
            pragma_synchronous: Synchronous::Full, // most conservative setting
            pragma_cache_pages: 8192, // 32 megabytes with the default page size of 4096
            read_only: false,
            create: true,
        }
    }
}

impl Config {
    pub fn with_read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }
    /// Set size targets for the store
    pub fn with_size_targets(mut self, size_targets: SizeTargets) -> Self {
        self.size_targets = size_targets;
        self
    }
    /// Set strategy for which non-pinned blocks to keep in case one of the size targets is exceeded.
    pub fn with_cache_tracker<T: CacheTracker + 'static>(mut self, cache_tracker: T) -> Self {
        self.cache_tracker = Arc::new(cache_tracker);
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

pub struct BlockStore<S> {
    conn: Connection,
    mem_cache: MemCache,
    expired_temp_pins: Arc<Mutex<Vec<i64>>>,
    config: Config,
    _s: PhantomData<S>,
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

impl<S> BlockStore<S>
where
    S: StoreParams,
    Ipld: References<S::Codecs>,
{
    fn create_connection(db_path: DbPath, config: &Config) -> crate::Result<rusqlite::Connection> {
        let mut flags = OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI;
        flags |= if config.read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        };
        if config.create && !config.read_only {
            flags |= OpenFlags::SQLITE_OPEN_CREATE
        }
        let conn = match db_path {
            DbPath::Memory => Connection::open_in_memory()?,
            DbPath::File(path) => Connection::open_with_flags(path, flags)?,
        };
        Ok(conn)
    }

    pub fn open_path(db_path: DbPath, config: Config) -> crate::Result<Self> {
        let is_memory = db_path.is_memory();
        let mut conn = Self::create_connection(db_path, &config)?;
        init_db(
            &mut conn,
            is_memory,
            config.pragma_cache_pages as i64,
            config.pragma_synchronous,
        )?;
        let ids = in_txn(&mut conn, |txn| get_ids(txn))?;
        config.cache_tracker.retain_ids(&ids);
        Ok(Self {
            conn,
            mem_cache: MemCache::new(1024, 1024 * 1024 * 4),
            expired_temp_pins: Arc::new(Mutex::new(Vec::new())),
            config,
            _s: PhantomData,
        })
    }

    /// Create an in memory block store with the given config
    pub fn memory(config: Config) -> crate::Result<Self> {
        Self::open_path(DbPath::Memory, config)
    }

    /// Create a persistent block store with the given config
    pub fn open(path: impl AsRef<Path>, config: Config) -> crate::Result<Self> {
        let mut pb: PathBuf = PathBuf::new();
        pb.push(path);
        Self::open_path(DbPath::File(pb), config)
    }

    /// Open the file at the given path for testing.
    ///
    /// This will create a writeable in-memory database that is initialized with the content
    /// of the file at the given path.
    pub fn open_test(path: impl AsRef<Path>, config: Config) -> crate::Result<Self> {
        let mut conn = Self::create_connection(DbPath::Memory, &config)?;
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
            mem_cache: MemCache::new(1024, 1024 * 1024 * 4),
            expired_temp_pins: Arc::new(Mutex::new(Vec::new())),
            config,
            _s: PhantomData,
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

    pub fn transaction(&mut self) -> Result<Transaction<'_, S>> {
        Transaction::new(self)
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
        let txn = self.transaction()?;
        txn.alias(name, link)?;
        txn.commit()
    }

    /// Resolves an alias to a cid.
    pub fn resolve(&mut self, name: impl AsRef<[u8]>) -> crate::Result<Option<Cid>> {
        self.transaction()?.resolve(name)
    }

    pub fn extend_temp_pin(
        &mut self,
        pin: &TempPin,
        links: impl IntoIterator<Item = Cid>,
    ) -> crate::Result<()> {
        let txn = self.transaction()?;
        for link in links {
            txn.extend_temp_pin(pin, &link)?;
        }
        txn.commit()
    }

    /// Returns the aliases referencing a block.
    pub fn reverse_alias(&mut self, cid: &Cid) -> crate::Result<Option<Vec<Vec<u8>>>> {
        self.transaction()?.reverse_alias(cid)
    }

    /// Checks if the store knows about the cid.
    /// Note that this does not necessarily mean that the store has the data for the cid.
    pub fn has_cid(&mut self, cid: &Cid) -> Result<bool> {
        self.transaction()?.has_cid(cid)
    }

    /// Checks if the store has the data for a cid
    pub fn has_block(&mut self, cid: &Cid) -> Result<bool> {
        self.transaction()?.has_block(cid)
    }

    /// Get the stats for the store.
    ///
    /// The stats are kept up to date, so this is fast.
    pub fn get_store_stats(&mut self) -> Result<StoreStats> {
        self.transaction()?.get_store_stats()
    }

    /// Get all cids that the store knows about
    pub fn get_known_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        self.transaction()?.get_known_cids()
    }

    /// Get all cids for which the store has blocks
    pub fn get_block_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        self.transaction()?.get_block_cids()
    }

    /// Get descendants of a cid
    pub fn get_descendants<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        self.transaction()?.get_descendants(cid)
    }

    /// Given a root of a dag, gives all cids which we do not have data for.
    pub fn get_missing_blocks<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        self.transaction()?.get_missing_blocks(cid)
    }

    /// list all aliases
    pub fn aliases<C: FromIterator<(Vec<u8>, Cid)>>(&mut self) -> Result<C> {
        self.transaction()?.aliases()
    }

    /// Add a number of blocks to the store
    ///
    /// - `blocks` the blocks to add.
    /// - `alias` an optional temporary alias.
    ///   This can be used to incrementally add blocks without having to worry about them being garbage
    ///   collected before they can be pinned with a permanent alias.
    pub fn put_blocks(
        &mut self,
        blocks: impl IntoIterator<Item = Block<S>>,
        pin: Option<&TempPin>,
    ) -> Result<()> {
        let txn = self.transaction()?;
        for block in blocks {
            txn.put_block(&block, pin)?;
        }
        txn.commit()
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
    pub fn put_block(&mut self, block: &Block<S>, pin: Option<&TempPin>) -> Result<()> {
        let txn = self.transaction()?;
        txn.put_block(block, pin)?;
        txn.commit()
    }

    /// Get data for a block
    ///
    /// Will return None if we don't have the data
    pub fn get_block(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        self.transaction()?.get_block(cid)
    }

    pub fn vacuum(&mut self) -> Result<()> {
        vacuum(&self.conn)
    }

    /// do a full garbage collection
    ///
    /// for a large block store, this can take several seconds to minutes. If that is not acceptable,
    /// consider using incremental gc.
    pub fn gc(&mut self) -> Result<()> {
        // the brute force option of cache coherence
        self.mem_cache.clear();
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
            mem::swap(self.expired_temp_pins.lock().deref_mut(), &mut result);
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
                incremental_gc(&txn, min_blocks, max_duration, size_targets, cache_tracker)
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
        log_execution_time("delete_orphaned", Duration::from_millis(100), || {
            in_txn(&mut self.conn, move |txn| {
                Ok(incremental_delete_orphaned(txn, min_blocks, max_duration)?)
            })
        })
    }
}

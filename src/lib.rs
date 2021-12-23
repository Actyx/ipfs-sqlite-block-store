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
#[cfg(test)]
mod tests;
mod transaction;

use cache::{CacheTracker, NoopCacheTracker};
use db::*;
use error::Context;
pub use error::{BlockStoreError, Result};
use libipld::{codec::References, store::StoreParams, Block, Cid, Ipld};
use parking_lot::Mutex;
use rusqlite::{Connection, DatabaseName, OpenFlags};
use std::{
    collections::HashSet,
    fmt,
    iter::FromIterator,
    marker::PhantomData,
    mem,
    ops::DerefMut,
    path::{Path, PathBuf},
    sync::Arc,
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

#[derive(Debug, Clone)]
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
    expired_temp_pins: Arc<Mutex<Vec<i64>>>,
    config: Config,
    db_path: DbPath,
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

/// a handle that contains a temporary pin
///
/// Dropping this handle enqueues the pin for dropping before the next gc.
// do not implement Clone for this!
pub struct TempPin {
    id: i64,
    expired_temp_pins: Arc<Mutex<Vec<i64>>>,
}

impl TempPin {
    fn new(expired_temp_pins: Arc<Mutex<Vec<i64>>>) -> Self {
        Self {
            id: 0,
            expired_temp_pins,
        }
    }
}

/// dump the temp alias id so you can find it in the database
impl fmt::Debug for TempPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("TempAlias");
        if self.id > 0 {
            builder.field("id", &self.id);
        } else {
            builder.field("unused", &true);
        }
        builder.finish()
    }
}

impl Drop for TempPin {
    fn drop(&mut self) {
        if self.id > 0 {
            self.expired_temp_pins.lock().push(self.id);
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
            DbPath::Memory => Connection::open_in_memory().ctx("opening in-memory DB")?,
            DbPath::File(path) => Connection::open_with_flags(path, flags).ctx("opening DB")?,
        };
        Ok(conn)
    }

    pub fn open_path(db_path: DbPath, config: Config) -> crate::Result<Self> {
        let is_memory = db_path.is_memory();
        let mut conn = Self::create_connection(db_path.clone(), &config)?;
        // this needs to be done only once, and before the first transaction
        conn.execute_batch("PRAGMA journal_mode = WAL")
            .ctx("setting WAL mode")?;
        init_db(
            &mut conn,
            is_memory,
            config.pragma_cache_pages as i64,
            config.pragma_synchronous,
        )?;
        let mut this = Self {
            conn,
            expired_temp_pins: Arc::new(Mutex::new(Vec::new())),
            config,
            db_path,
            _s: PhantomData,
        };
        if !is_memory {
            let mut conn = this.additional_connection()?;
            std::thread::spawn(move || {
                if let Err(e) = recompute_store_stats(&mut conn.conn) {
                    tracing::error!("cannot recompute store stats: {}", e);
                }
            });
        }
        if this.config.cache_tracker.has_persistent_state() {
            let ids = in_txn(
                &mut this.conn,
                Some(("get IDs", Duration::from_secs(1))),
                get_ids,
            )?;
            this.config.cache_tracker.retain_ids(&ids);
        }
        Ok(this)
    }

    /// Create another connection to the underlying database
    ///
    /// This allows you to perform operations in parallel.
    pub fn additional_connection(&self) -> crate::Result<Self> {
        if self.db_path.is_memory() {
            return Err(BlockStoreError::NoAdditionalInMemory);
        }
        let mut conn = Self::create_connection(self.db_path.clone(), &self.config)?;
        init_pragmas(
            &mut conn,
            self.db_path.is_memory(),
            self.config.pragma_cache_pages as i64,
        )?;
        conn.pragma_update(
            None,
            "synchronous",
            &self.config.pragma_synchronous.to_string(),
        )
        .ctx("setting synchronous mode")?;
        Ok(Self {
            conn,
            expired_temp_pins: self.expired_temp_pins.clone(),
            config: self.config.clone(),
            db_path: self.db_path.clone(),
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
        )
        .ctx("restoring test DB from backup")?;
        let ids = in_txn(
            &mut conn,
            Some(("get ids", Duration::from_secs(1))),
            get_ids,
        )?;
        config.cache_tracker.retain_ids(&ids);
        Ok(Self {
            conn,
            expired_temp_pins: Arc::new(Mutex::new(Vec::new())),
            config,
            db_path: DbPath::Memory,
            _s: PhantomData,
        })
    }

    pub fn backup(&mut self, path: impl AsRef<Path>) -> Result<()> {
        in_txn(&mut self.conn, None, move |txn| {
            txn.backup(DatabaseName::Main, path.as_ref(), None)
                .ctx("backing up DB")
        })
    }

    pub fn flush(&mut self) -> crate::Result<()> {
        in_txn(&mut self.conn, None, |txn| {
            txn.pragma_update(None, "wal_checkpoint", &"TRUNCATE")
                .ctx("flushing WAL")
        })
    }

    pub fn integrity_check(&mut self) -> crate::Result<()> {
        let result = integrity_check(&mut self.conn)?;
        if result == vec!["ok".to_owned()] {
            Ok(())
        } else {
            let error_text = result.join(";");
            Err(crate::error::BlockStoreError::SqliteError(
                rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(11), Some(error_text)),
                "checking integrity",
            ))
        }
        // FIXME add actual integrity check on the stored blocks
    }

    pub fn transaction(&mut self) -> Transaction<'_, S> {
        Transaction::new(self)
    }

    /// Get a temporary alias for safely adding blocks to the store
    pub fn temp_pin(&self) -> TempPin {
        TempPin::new(self.expired_temp_pins.clone())
    }

    /// Run a full VACUUM on the SQLITE database
    ///
    /// This may take a while, blocking all other writes to the store.
    pub fn vacuum(&mut self) -> Result<()> {
        vacuum(&mut self.conn)
    }

    /// Perform maintenance on the TempPins
    ///
    /// This is done automatically upon every (incremental) GC, so you normally don’t need to call this.
    pub fn cleanup_temp_pins(&mut self) -> Result<()> {
        // atomically grab the expired_temp_pins until now
        let expired_temp_pins = mem::take(self.expired_temp_pins.lock().deref_mut());
        in_txn(
            &mut self.conn,
            Some(("dropping expired temp_pins", Duration::from_millis(100))),
            move |txn| {
                // get rid of dropped temp aliases, this should be fast
                for id in expired_temp_pins.iter() {
                    delete_temp_pin(txn, *id)?;
                }
                Ok(())
            },
        )
    }

    /// Perform full GC
    ///
    /// This is the same as running incremental GC without limits, plus a full SQLITE VACUUM.
    pub fn gc(&mut self) -> Result<()> {
        self.cleanup_temp_pins()?;
        incremental_gc(
            &mut self.conn,
            usize::MAX,
            Duration::from_secs(u32::MAX.into()),
            self.config.size_targets,
            &self.config.cache_tracker,
        )?;
        self.vacuum()?;
        Ok(())
    }

    /// Perform an incremental garbage collection.
    ///
    /// Will collect unpinned blocks until either the size targets are met again, or at minimum
    /// `min_blocks` blocks are collected. Then it will continue connecting blocks until `max_duration`
    /// is elapsed.
    ///
    /// Note that this might significantly exceed `max_duration` for various reasons.
    ///
    /// Returns true if either size targets are met or there are no unpinned blocks left.
    pub fn incremental_gc(&mut self, min_blocks: usize, max_duration: Duration) -> Result<bool> {
        self.cleanup_temp_pins()?;
        let ret = incremental_gc(
            &mut self.conn,
            min_blocks,
            max_duration,
            self.config.size_targets,
            &self.config.cache_tracker,
        )?;
        in_txn(&mut self.conn, None, |txn| {
            txn.execute_batch("PRAGMA incremental_vacuum")
                .ctx("incremental vacuum")
        })?;
        Ok(ret)
    }
}

macro_rules! delegate {
    ($($(#[$attr:meta])*$n:ident$(<$v:ident : $vt:path>)?($($arg:ident : $typ:ty),*) -> $ret:ty;)+) => {
        $(
            $(#[$attr])*
            pub fn $n$(<$v: $vt>)?(&mut self, $($arg: $typ),*) -> $ret {
                self.transaction().$n($($arg),*)
            }
        )+
    };
}

impl<S> BlockStore<S>
where
    S: StoreParams,
    Ipld: References<S::Codecs>,
{
    delegate! {
        /// Set or delete an alias
        alias(name: impl AsRef<[u8]>, link: Option<&Cid>) -> Result<()>;

        /// Returns the aliases referencing a cid
        reverse_alias(cid: &Cid) -> Result<Option<HashSet<Vec<u8>>>>;

        /// Resolves an alias to a cid
        resolve(name: impl AsRef<[u8]>) -> Result<Option<Cid>>;

        /// Extend temp pin with an additional cid
        extend_temp_pin(pin: &mut TempPin, link: &Cid) -> Result<()>;

        /// Checks if the store knows about the cid.
        ///
        /// Note that this does not necessarily mean that the store has the data for the cid.
        has_cid(cid: &Cid) -> Result<bool>;

        /// Checks if the store has the data for a cid
        has_block(cid: &Cid) -> Result<bool>;

        /// Get all cids that the store knows about
        get_known_cids<C: FromIterator<Cid>>() -> Result<C>;

        /// Get all cids for which the store has blocks
        get_block_cids<C: FromIterator<Cid>>() -> Result<C>;

        /// Get descendants of a cid
        get_descendants<C: FromIterator<Cid>>(cid: &Cid) -> Result<C>;

        /// Given a root of a dag, gives all cids which we do not have data for.
        get_missing_blocks<C: FromIterator<Cid>>(cid: &Cid) -> Result<C>;

        /// list all aliases
        aliases<C: FromIterator<(Vec<u8>, Cid)>>() -> Result<C>;

        /// Put a block
        ///
        /// This will only be completed once the transaction is successfully committed.
        put_block(block: &Block<S>, pin: Option<&mut TempPin>) -> Result<()>;

        /// Get a block
        get_block(cid: &Cid) -> Result<Option<Vec<u8>>>;

        /// Get the stats for the store
        ///
        /// The stats are kept up to date, so this is fast.
        get_store_stats() -> Result<StoreStats>;
    }

    pub fn put_blocks<I>(&mut self, blocks: I, mut pin: Option<&mut TempPin>) -> Result<()>
    where
        I: IntoIterator<Item = Block<S>>,
    {
        let mut txn = self.transaction();
        for block in blocks {
            txn.put_block(&block, pin.as_deref_mut())?;
        }
        txn.commit()
    }
}

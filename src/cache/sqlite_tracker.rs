use super::{BlockInfo, CacheTracker};
use fnv::{FnvHashMap, FnvHashSet};
use parking_lot::Mutex;
use rusqlite::{Connection, Transaction};
use std::{
    fmt::Debug,
    ops::DerefMut,
    path::Path,
    sync::Arc,
    time::{Instant, SystemTime},
};
use tracing::*;

/// A cache tracker that uses a sqlite database as persistent storage
pub struct SqliteCacheTracker<F> {
    conn: Arc<Mutex<Connection>>,
    mk_cache_entry: F,
}

impl<F> Debug for SqliteCacheTracker<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteCacheTracker").finish()
    }
}

const INIT: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = OFF;
CREATE TABLE IF NOT EXISTS accessed (
    id INTEGER PRIMARY KEY,
    time INTEGER
);
"#;

fn init_db(conn: &mut Connection) -> crate::Result<()> {
    conn.execute_batch(INIT)?;
    Ok(())
}

/// execute a statement in a readonly transaction
/// nested transactions are not allowed here.
pub(crate) fn in_ro_txn<T>(
    conn: &mut Connection,
    f: impl FnOnce(&Transaction) -> crate::Result<T>,
) -> crate::Result<T> {
    let txn = conn.transaction()?;
    f(&txn)
}

fn attempt_txn<T>(
    mut conn: impl DerefMut<Target = Connection>,
    f: impl FnOnce(&Transaction) -> crate::Result<T>,
) {
    let result = crate::in_txn(&mut conn, None, f);
    if let Err(cause) = result {
        tracing::warn!("Unable to execute transaction {}", cause);
    }
}

fn attempt_ro_txn<T>(
    mut conn: impl DerefMut<Target = Connection>,
    f: impl FnOnce(&Transaction) -> crate::Result<T>,
) {
    let result = in_ro_txn(&mut conn, f);
    if let Err(cause) = result {
        tracing::warn!("Unable to execute readonly transaction {}", cause);
    }
}

fn set_accessed(txn: &Transaction, id: i64, accessed: i64) -> crate::Result<()> {
    txn.prepare_cached("REPLACE INTO accessed (id, time) VALUES (?, ?)")?
        .execute([id, accessed])?;
    Ok(())
}

fn get_accessed_bulk(
    txn: &Transaction,
    result: &mut FnvHashMap<i64, Option<i64>>,
) -> crate::Result<()> {
    let mut stmt = txn.prepare_cached("SELECT id, time FROM accessed")?;
    let accessed = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let time: i64 = row.get(1)?;
        Ok((id, time))
    })?;
    // we have no choice but to run through all values in accessed.
    for row in accessed.flatten() {
        // only add if a row already exists
        let (id, time) = row;
        if let Some(value) = result.get_mut(&id) {
            *value = Some(time);
        }
    }
    Ok(())
}

fn delete_id(txn: &Transaction, id: i64) -> crate::Result<()> {
    txn.prepare_cached("DELETE FROM accessed WHERE id = ?")?
        .execute([id])?;
    Ok(())
}

fn get_ids(txn: &Transaction) -> crate::Result<Vec<i64>> {
    let ids = txn
        .prepare_cached("SELECT id FROM accessed")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<i64>>>()?;
    Ok(ids)
}

impl<F> SqliteCacheTracker<F>
where
    F: Fn(i64, BlockInfo) -> Option<i64>,
{
    pub fn memory(mk_cache_entry: F) -> crate::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        init_db(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            mk_cache_entry,
        })
    }

    pub fn open(path: impl AsRef<Path>, mk_cache_entry: F) -> crate::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(&mut conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            mk_cache_entry,
        })
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SortKey {
    time: Option<i64>,
    id: i64,
}

impl SortKey {
    fn new(time: Option<i64>, id: i64) -> Self {
        Self { time, id }
    }
}

impl<F> CacheTracker for SqliteCacheTracker<F>
where
    F: Fn(i64, BlockInfo) -> Option<i64> + Send + Sync,
{
    #[allow(clippy::needless_collect)]
    fn blocks_accessed(&self, blocks: Vec<BlockInfo>) {
        let accessed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let nanos = accessed.as_nanos() as i64;
        let items = blocks
            .iter()
            .filter_map(|block| (self.mk_cache_entry)(nanos, *block).map(|nanos| (block.id, nanos)))
            .collect::<Vec<_>>();
        if items.is_empty() {
            return;
        }
        attempt_txn(self.conn.lock(), |txn| {
            for (id, accessed) in items {
                set_accessed(txn, id, accessed as i64)?;
            }
            Ok(())
        });
    }

    fn blocks_deleted(&self, blocks: Vec<BlockInfo>) {
        attempt_txn(self.conn.lock(), |txn| {
            for block in blocks {
                delete_id(txn, block.id)?;
            }
            Ok(())
        });
    }

    fn retain_ids(&self, ids: &[i64]) {
        let ids = ids.iter().cloned().collect::<FnvHashSet<i64>>();
        attempt_txn(self.conn.lock(), move |txn| {
            for id in get_ids(txn)? {
                if !&ids.contains(&id) {
                    delete_id(txn, id)?;
                }
            }
            Ok(())
        });
    }

    fn sort_ids(&self, ids: &mut [i64]) {
        attempt_ro_txn(self.conn.lock(), |txn| {
            let t0 = Instant::now();
            let mut accessed = ids
                .iter()
                .map(|id| (*id, None))
                .collect::<FnvHashMap<i64, Option<i64>>>();
            get_accessed_bulk(txn, &mut accessed)?;
            debug!("getting access times took {}", t0.elapsed().as_micros());
            let t0 = Instant::now();
            ids.sort_by_cached_key(|id| SortKey::new(accessed.get(id).cloned().flatten(), *id));
            debug!("sorting ids took {}", t0.elapsed().as_micros());
            Ok(())
        });
    }

    fn has_persistent_state(&self) -> bool {
        true
    }
}

#[test]
fn sort_key_sort_order() {
    assert!(
        SortKey::new(None, i64::max_value())
            < SortKey::new(Some(i64::min_value()), i64::min_value())
    );
}

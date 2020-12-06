use fnv::FnvHashSet;
use libipld::Cid;
use rusqlite::{Connection, OptionalExtension, Transaction, NO_PARAMS};
use std::{
    fmt::Debug,
    path::Path,
    time::{Duration, SystemTime},
};

use super::CacheTracker;

pub struct SqliteCacheTracker<F> {
    conn: Connection,
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

fn attempt_txn<T>(conn: &mut Connection, f: impl FnOnce(&Transaction) -> crate::Result<T>) {
    let result = crate::in_txn(conn, f);
    if let Err(cause) = result {
        tracing::warn!("Unable to execute transaction {}", cause);
    }
}

fn attempt_ro_txn<T>(conn: &Connection, f: impl FnOnce(&Transaction) -> crate::Result<T>) {
    let result = crate::in_ro_txn(conn, f);
    if let Err(cause) = result {
        tracing::warn!("Unable to execute readonly transaction {}", cause);
    }
}

fn set_accessed(txn: &Transaction, id: i64, accessed: i64) -> crate::Result<()> {
    txn.prepare_cached("REPLACE INTO accessed (id, time) VALUES (?, ?)")?
        .execute(&[id, accessed])?;
    Ok(())
}

fn get_accessed(txn: &Transaction, id: i64) -> crate::Result<Option<i64>> {
    let accessed = txn
        .prepare_cached("SELECT FROM accessed WHERE id = ?")?
        .query_row(&[id], |row| row.get(0))
        .optional()?;
    Ok(accessed)
}

fn delete_id(txn: &Transaction, id: i64) -> crate::Result<()> {
    txn.prepare_cached("DELETE FROM accessed WHERE id = ?")?
        .execute(&[id])?;
    Ok(())
}

fn get_ids(txn: &Transaction) -> crate::Result<Vec<i64>> {
    let ids = txn
        .prepare_cached("SELECT id FROM accessed")?
        .query_map(NO_PARAMS, |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<i64>>>()?;
    Ok(ids)
}

impl<F> SqliteCacheTracker<F>
where
    F: Fn(Duration, &Cid, &[u8]) -> Option<Duration>,
{
    pub fn memory(mk_cache_entry: F) -> crate::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
            mk_cache_entry,
        })
    }

    pub fn open(path: impl AsRef<Path>, mk_cache_entry: F) -> crate::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
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
    F: Fn(Duration, &Cid, &[u8]) -> Option<Duration>,
{
    fn blocks_accessed(&mut self, blocks: &[(i64, &Cid, &[u8])]) {
        let accessed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let items = blocks
            .iter()
            .filter_map(|(id, cid, data)| {
                (self.mk_cache_entry)(accessed, cid, data).map(|accessed| (id, accessed))
            })
            .collect::<Vec<_>>();
        if items.is_empty() {
            return;
        }
        attempt_txn(&mut self.conn, |txn| {
            for (id, accessed) in items {
                set_accessed(txn, *id, accessed.as_secs() as i64)?;
            }
            Ok(())
        });
    }

    fn delete_ids(&mut self, ids: &[i64]) {
        attempt_txn(&mut self.conn, |txn| {
            for id in ids {
                delete_id(txn, *id)?;
            }
            Ok(())
        });
    }

    fn retain_ids(&mut self, ids: &[i64]) {
        let ids = ids.iter().cloned().collect::<FnvHashSet<i64>>();
        attempt_txn(&mut self.conn, move |txn| {
            for id in get_ids(txn)? {
                if !&ids.contains(&id) {
                    delete_id(txn, id)?;
                }
            }
            Ok(())
        });
    }

    fn sort_ids(&self, ids: &mut [i64]) {
        attempt_ro_txn(&self.conn, |txn| {
            let mut keys = Vec::new();
            for id in ids.iter() {
                keys.push(SortKey::new(get_accessed(txn, *id)?, *id));
            }
            keys.sort_unstable();
            for i in 0..ids.len() {
                ids[i] = keys[i].id;
            }
            Ok(())
        });
    }
}

#[test]
fn sort_key_sort_order() {
    assert!(
        SortKey::new(None, i64::max_value())
            < SortKey::new(Some(i64::min_value()), i64::min_value())
    );
}

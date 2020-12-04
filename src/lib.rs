mod cidbytes;
mod db;
mod error;
#[cfg(test)]
mod tests;

use crate::cidbytes::CidBytes;
use db::*;
pub use error::{BlockStoreError, Result};
use libipld::cid::{self, Cid};
use rusqlite::{Connection, Transaction};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    iter::FromIterator,
    ops::DerefMut,
    path::Path,
    sync::atomic::Ordering,
    sync::Arc,
    sync::{atomic::AtomicI64, Mutex},
    time::Duration,
};

pub struct Store {
    conn: Connection,
    expired_temp_aliases: Arc<Mutex<Vec<i64>>>,
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
fn in_txn<T>(
    conn: &mut Connection,
    f: impl FnOnce(&Transaction) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let txn = conn.transaction()?;
    let result = f(&txn);
    if result.is_ok() {
        txn.commit()?;
    }
    result
}

/// execute a statement in a readonly transaction
/// nested transactions are not allowed here.
fn in_ro_txn<T>(
    conn: &Connection,
    f: impl FnOnce(&Transaction) -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    let txn = conn.unchecked_transaction()?;
    let result = f(&txn);
    result
}

pub trait Block<C> {
    type I: Iterator<Item = C>;
    fn cid(&self) -> C;
    fn data(&self) -> &[u8];
    fn links(&self) -> Self::I;
}

pub struct CidBlock {
    cid: Cid,
    data: Vec<u8>,
    links: Vec<Cid>,
}

impl CidBlock {
    pub fn new(cid: Cid, data: Vec<u8>, links: Vec<Cid>) -> Self {
        Self { cid, data, links }
    }
}

impl Block<Cid> for CidBlock {
    type I = std::vec::IntoIter<Cid>;

    fn cid(&self) -> Cid {
        self.cid
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn links(&self) -> Self::I {
        self.links.clone().into_iter()
    }
}

struct CidBytesBlock {
    cid: CidBytes,
    data: Vec<u8>,
    links: Vec<CidBytes>,
}

impl Block<CidBytes> for CidBytesBlock {
    type I = std::vec::IntoIter<CidBytes>;

    fn cid(&self) -> CidBytes {
        self.cid
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn links(&self) -> Self::I {
        self.links.clone().into_iter()
    }
}

impl TryFrom<CidBlock> for CidBytesBlock {
    type Error = cid::Error;

    fn try_from(value: CidBlock) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            cid: (&value.cid).try_into()?,
            data: value.data,
            links: value
                .links
                .iter()
                .map(CidBytes::try_from)
                .collect::<std::result::Result<Vec<_>, cid::Error>>()?,
        })
    }
}

impl Store {
    pub fn memory() -> rusqlite::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let mut conn = Connection::open(path)?;
        init_db(&mut conn)?;
        Ok(Self {
            conn,
            expired_temp_aliases: Arc::new(Mutex::new(Vec::new())),
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
        Ok(in_txn(&mut self.conn, |txn| {
            alias(txn, name.as_ref(), link.as_ref())
        })?)
    }

    pub fn has_cid(&self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        Ok(in_ro_txn(&self.conn, |txn| has_cid(txn, cid))?)
    }

    pub fn has_block(&mut self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        Ok(in_ro_txn(&self.conn, |txn| has_block(txn, cid))?)
    }

    pub fn get_block_count(&mut self) -> Result<u64> {
        Ok(u64::try_from(in_ro_txn(&self.conn, |txn| get_block_count(txn))?).unwrap())
    }

    pub fn get_block_size(&mut self) -> Result<u64> {
        Ok(u64::try_from(in_ro_txn(&self.conn, |txn| get_block_size(txn))?).unwrap())
    }

    pub fn get_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        let result = in_ro_txn(&self.conn, |txn| get_cids::<CidBytes>(txn))?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<C>>()?;
        Ok(res)
    }

    pub fn get_descendants<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let result = in_ro_txn(&self.conn, move |txn| Ok(get_descendants(txn, cid)?))?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<C>>()?;
        Ok(res)
    }

    pub fn get_missing_blocks<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let result = log_execution_time("get_missing_blocks", Duration::from_millis(10), || {
            in_ro_txn(&self.conn, move |txn| {
                let result = get_missing_blocks(txn, cid)?;
                Ok(result)
            })
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
            in_txn(&mut self.conn, move |txn| {
                // get rid of dropped temp aliases, this should be fast
                for id in expired_temp_aliases {
                    delete_temp_alias(txn, id)?;
                }
                incremental_gc(&txn, min_blocks, max_duration)
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
    pub fn add_blocks(
        &mut self,
        blocks: impl IntoIterator<Item = CidBlock>,
        alias: Option<&TempAlias>,
    ) -> Result<()> {
        let blocks = blocks
            .into_iter()
            .map(CidBytesBlock::try_from)
            .collect::<cid::Result<Vec<_>>>()?;
        let alias = alias.map(|alias| &alias.id);
        in_txn(&mut self.conn, move |txn| {
            for block in blocks.into_iter() {
                add_block(txn, &block.cid(), block.data(), block.links(), alias)?;
            }
            Ok(())
        })?;
        Ok(())
    }
    pub fn add_block(
        &mut self,
        cid: &Cid,
        data: &[u8],
        links: impl IntoIterator<Item = Cid>,
        alias: Option<&TempAlias>,
    ) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        let links = links
            .into_iter()
            .map(|x| CidBytes::try_from(&x))
            .collect::<cid::Result<Vec<_>>>()?;
        let alias = alias.map(|alias| &alias.id);
        Ok(in_txn(&mut self.conn, |txn| {
            Ok(add_block(txn, &cid, data, links, alias)?)
        })?)
    }
    pub fn get_block(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let cid = CidBytes::try_from(cid)?;
        Ok(in_ro_txn(&self.conn, |txn| Ok(get_block(txn, cid)?))?)
    }
}

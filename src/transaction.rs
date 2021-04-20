use crate::{
    cache::{BlockInfo, CacheTracker, WriteInfo},
    cidbytes::CidBytes,
    db::*,
    links, Block, BlockStore, Result, StoreStats, TempPin,
};
use fnv::FnvHashSet;
use libipld::{cid, Cid};
use parking_lot::Mutex;
use std::{
    convert::TryFrom,
    iter::FromIterator,
    marker::PhantomData,
    mem,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

pub struct Transaction<'a, S> {
    inner: rusqlite::Transaction<'a>,
    info: Mutex<TransactionInfo>,
    expired_temp_pins: Arc<Mutex<Vec<i64>>>,
    _s: PhantomData<S>,
}

struct TransactionInfo {
    written: Vec<WriteInfo>,
    accessed: Vec<BlockInfo>,
    committed: bool,
    tracker: Arc<dyn CacheTracker>,
}

impl Drop for TransactionInfo {
    fn drop(&mut self) {
        if !self.accessed.is_empty() {
            let blocks = mem::replace(&mut self.accessed, Vec::new());
            self.tracker.blocks_accessed(blocks);
        }
        // if the transaction was not committed, we don't report blocks written!
        if self.committed && !self.written.is_empty() {
            let blocks = mem::replace(&mut self.written, Vec::new());
            self.tracker.blocks_written(blocks);
        }
    }
}

impl<'a, S> Transaction<'a, S> {
    pub(crate) fn new(owner: &'a mut BlockStore<S>) -> Result<Self> {
        Ok(Self {
            inner: owner.conn.transaction()?,
            info: Mutex::new(TransactionInfo {
                written: Vec::new(),
                accessed: Vec::new(),
                committed: false,
                tracker: owner.config.cache_tracker.clone(),
            }),
            expired_temp_pins: owner.expired_temp_pins.clone(),
            _s: PhantomData,
        })
    }

    fn txn(&self) -> &rusqlite::Transaction<'a> {
        &self.inner
    }

    /// Set or delete an alias
    pub fn alias(&self, name: impl AsRef<[u8]>, link: Option<&Cid>) -> Result<()> {
        let link: Option<CidBytes> = link.map(CidBytes::try_from).transpose()?;
        alias(self.txn(), name.as_ref(), link.as_ref())?;
        Ok(())
    }

    /// Returns the aliases referencing a cid.
    pub fn reverse_alias(&self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>> {
        let cid = CidBytes::try_from(cid)?;
        reverse_alias(self.txn(), cid.as_ref())
    }

    /// Resolves an alias to a cid.
    pub fn resolve(&self, name: impl AsRef<[u8]>) -> Result<Option<Cid>> {
        Ok(resolve::<CidBytes>(self.txn(), name.as_ref())?
            .map(|c| Cid::try_from(&c))
            .transpose()?)
    }

    /// Get a temporary pin for safely adding blocks to the store
    pub fn temp_pin(&self) -> TempPin {
        TempPin {
            id: AtomicI64::new(0),
            expired_temp_pins: self.expired_temp_pins.clone(),
        }
    }

    /// Extend temp pin with an additional cid
    pub fn extend_temp_pin(&self, pin: &TempPin, link: &Cid) -> Result<()> {
        let link = CidBytes::try_from(link)?;
        let pin0 = pin.id.load(Ordering::SeqCst);
        extend_temp_pin(self.txn(), pin0, vec![link])?;
        pin.id.store(pin0, Ordering::SeqCst);
        Ok(())
    }

    /// Checks if the store knows about the cid.
    ///
    /// Note that this does not necessarily mean that the store has the data for the cid.
    pub fn has_cid(&self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        has_cid(self.txn(), cid)
    }

    /// Checks if the store has the data for a cid
    pub fn has_block(&self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        has_block(self.txn(), cid)
    }

    /// Get all cids that the store knows about
    pub fn get_known_cids<C: FromIterator<Cid>>(&self) -> Result<C> {
        let res = get_known_cids::<CidBytes>(self.txn())?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Get all cids for which the store has blocks
    pub fn get_block_cids<C: FromIterator<Cid>>(&self) -> Result<C> {
        let res = get_block_cids::<CidBytes>(self.txn())?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Get descendants of a cid
    pub fn get_descendants<C: FromIterator<Cid>>(&self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let res = get_descendants(self.txn(), cid)?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Given a root of a dag, gives all cids which we do not have data for.
    pub fn get_missing_blocks<C: FromIterator<Cid>>(&self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let result = log_execution_time("get_missing_blocks", Duration::from_millis(10), || {
            get_missing_blocks(self.txn(), cid)
        })?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// list all aliases
    pub fn aliases<C: FromIterator<(Vec<u8>, Cid)>>(&self) -> Result<C> {
        let result: Vec<(Vec<u8>, CidBytes)> = aliases(self.txn())?;
        let res = result
            .into_iter()
            .map(|(alias, cid)| {
                let cid = Cid::try_from(&cid)?;
                Ok((alias, cid))
            })
            .collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Put a block. This will only be completed once the transaction is successfully committed
    pub fn put_block<B: Block<S>>(&self, block: B, pin: Option<&TempPin>) -> Result<()> {
        let mut pin0 = pin.map(|pin| pin.id.load(Ordering::SeqCst));
        let cid_bytes = CidBytes::try_from(block.cid())?;
        let links = links(&block)?
            .iter()
            .map(CidBytes::try_from)
            .collect::<std::result::Result<FnvHashSet<_>, cid::Error>>()?;
        let res = put_block(self.txn(), &cid_bytes, &block.data(), links, &mut pin0)?;
        let write_info = WriteInfo::new(
            BlockInfo::new(res.id, block.cid(), block.data().len()),
            res.block_exists,
        );
        if let (Some(pin), Some(p)) = (pin, pin0) {
            pin.id.store(p, Ordering::SeqCst);
        }
        self.info.lock().written.push(write_info);
        Ok(())
    }

    /// Get a block
    pub fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let response = get_block(self.txn(), &CidBytes::try_from(cid)?)?;
        if let Some(info) = response
            .as_ref()
            .map(|(id, data)| BlockInfo::new(*id, cid, data.len()))
        {
            self.info.lock().accessed.push(info);
        }
        Ok(response.map(|(_id, data)| data))
    }

    /// Get the stats for the store.
    ///
    /// The stats are kept up to date, so this is fast.
    pub fn get_store_stats(&self) -> Result<StoreStats> {
        get_store_stats(self.txn())
    }

    /// Commit and consume the transaction. Default is to not commit.
    pub fn commit(self) -> Result<()> {
        self.inner.commit()?;
        self.info.lock().committed = true;
        Ok(())
    }
}

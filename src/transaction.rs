use crate::{
    cache::{BlockInfo, CacheTracker, WriteInfo},
    cidbytes::CidBytes,
    db::*,
    Block, BlockStore, Result, StoreStats, TempPin,
};
use fnv::FnvHashSet;
use libipld::{cid, codec::References, store::StoreParams, Cid, Ipld};
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
    inner: &'a mut rusqlite::Connection,
    info: TransactionInfo,
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
            let blocks = mem::take(&mut self.accessed);
            self.tracker.blocks_accessed(blocks);
        }
        // if the transaction was not committed, we don't report blocks written!
        if self.committed && !self.written.is_empty() {
            let blocks = mem::take(&mut self.written);
            self.tracker.blocks_written(blocks);
        }
    }
}

impl<'a, S> Transaction<'a, S>
where
    S: StoreParams,
    Ipld: References<S::Codecs>,
{
    pub(crate) fn new(owner: &'a mut BlockStore<S>) -> Self {
        Self {
            inner: &mut owner.conn,
            info: TransactionInfo {
                written: Vec::new(),
                accessed: Vec::new(),
                committed: false,
                tracker: owner.config.cache_tracker.clone(),
            },
            expired_temp_pins: owner.expired_temp_pins.clone(),
            _s: PhantomData,
        }
    }

    /// Set or delete an alias
    pub fn alias(&mut self, name: impl AsRef<[u8]>, link: Option<&Cid>) -> Result<()> {
        let link: Option<CidBytes> = link.map(CidBytes::try_from).transpose()?;
        in_txn(self.inner, None, move |txn| {
            alias(txn, name.as_ref(), link.as_ref())
        })?;
        Ok(())
    }

    /// Returns the aliases referencing a cid.
    pub fn reverse_alias(&mut self, cid: &Cid) -> Result<Option<Vec<Vec<u8>>>> {
        let cid = CidBytes::try_from(cid)?;
        in_txn(self.inner, None, move |txn| {
            reverse_alias(txn, cid.as_ref())
        })
    }

    /// Resolves an alias to a cid.
    pub fn resolve(&mut self, name: impl AsRef<[u8]>) -> Result<Option<Cid>> {
        in_txn(self.inner, None, move |txn| {
            resolve::<CidBytes>(txn, name.as_ref())?
                .map(|c| Cid::try_from(&c))
                .transpose()
                .map_err(Into::into)
        })
    }

    /// Get a temporary pin for safely adding blocks to the store
    pub fn temp_pin(&mut self) -> TempPin {
        TempPin {
            id: AtomicI64::new(0),
            expired_temp_pins: self.expired_temp_pins.clone(),
        }
    }

    /// Extend temp pin with an additional cid
    pub fn extend_temp_pin(&mut self, pin: &TempPin, link: &Cid) -> Result<()> {
        let link = CidBytes::try_from(link)?;
        let pin0 = pin.id.load(Ordering::SeqCst);
        in_txn(self.inner, None, move |txn| {
            extend_temp_pin(txn, pin0, vec![link])
        })?;
        pin.id.store(pin0, Ordering::SeqCst); // FIXME WAT?
        Ok(())
    }

    /// Checks if the store knows about the cid.
    ///
    /// Note that this does not necessarily mean that the store has the data for the cid.
    pub fn has_cid(&mut self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        in_txn(self.inner, None, move |txn| has_cid(txn, cid))
    }

    /// Checks if the store has the data for a cid
    pub fn has_block(&mut self, cid: &Cid) -> Result<bool> {
        let cid = CidBytes::try_from(cid)?;
        in_txn(self.inner, None, move |txn| has_block(txn, cid))
    }

    /// Get all cids that the store knows about
    pub fn get_known_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        let res = in_txn(self.inner, None, move |txn| get_known_cids::<CidBytes>(txn))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Get all cids for which the store has blocks
    pub fn get_block_cids<C: FromIterator<Cid>>(&mut self) -> Result<C> {
        let res = in_txn(self.inner, None, move |txn| get_block_cids::<CidBytes>(txn))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Get descendants of a cid
    pub fn get_descendants<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let res = in_txn(self.inner, None, move |txn| get_descendants(txn, cid))?;
        let res = res.iter().map(Cid::try_from).collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// Given a root of a dag, gives all cids which we do not have data for.
    pub fn get_missing_blocks<C: FromIterator<Cid>>(&mut self, cid: &Cid) -> Result<C> {
        let cid = CidBytes::try_from(cid)?;
        let result = in_txn(
            self.inner,
            Some(("get_missing_blocks", Duration::from_millis(10))),
            |txn| get_missing_blocks(txn, cid),
        )?;
        let res = result
            .iter()
            .map(Cid::try_from)
            .collect::<cid::Result<C>>()?;
        Ok(res)
    }

    /// list all aliases
    pub fn aliases<C: FromIterator<(Vec<u8>, Cid)>>(&mut self) -> Result<C> {
        let result: Vec<(Vec<u8>, CidBytes)> = in_txn(self.inner, None, move |txn| aliases(txn))?;
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
    pub fn put_block(&mut self, block: &Block<S>, pin: Option<&TempPin>) -> Result<()> {
        let mut pin0 = pin.map(|pin| pin.id.load(Ordering::SeqCst));
        let pin1 = &mut pin0;
        let cid_bytes = CidBytes::try_from(block.cid())?;
        let mut links = Vec::new();
        block.references(&mut links)?;
        let links = links
            .iter()
            .map(CidBytes::try_from)
            .collect::<std::result::Result<FnvHashSet<_>, cid::Error>>()?;
        let res = in_txn(
            self.inner,
            Some(("put block", Duration::from_millis(100))),
            move |txn| put_block(txn, &cid_bytes, block.data(), links.iter().copied(), pin1),
        )?;
        let write_info = WriteInfo::new(
            BlockInfo::new(res.id, block.cid(), block.data().len()),
            res.block_exists,
        );
        if let (Some(pin), Some(p)) = (pin, pin0) {
            pin.id.store(p, Ordering::SeqCst);
        }
        self.info.written.push(write_info);
        Ok(())
    }

    /// Get a block
    pub fn get_block(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let response = in_txn(self.inner, None, move |txn| {
            get_block(txn, &CidBytes::try_from(cid)?)
        })?;
        if let Some(info) = response
            .as_ref()
            .map(|(id, data)| BlockInfo::new(*id, cid, data.len()))
        {
            self.info.accessed.push(info);
        }
        Ok(response.map(|(_id, data)| data))
    }

    /// Get the stats for the store.
    ///
    /// The stats are kept up to date, so this is fast.
    pub fn get_store_stats(&mut self) -> Result<StoreStats> {
        in_txn(self.inner, None, move |txn| get_store_stats(txn))
    }

    /// Commit and consume the transaction. Default is to not commit.
    pub fn commit(mut self) -> Result<()> {
        self.info.committed = true;
        Ok(())
    }
}

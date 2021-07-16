use lazy_static::lazy_static;
use prometheus::{Histogram, HistogramOpts, Registry};

lazy_static! {
    pub static ref BLOCK_GET_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("block_get_time", "Complete time to load blocks",)
            .namespace("ipfs_sqlite_block_store")
    )
    .unwrap();
    pub static ref BLOCK_PUT_HIST: Histogram = Histogram::with_opts(
        HistogramOpts::new("block_put_time", "Complete time store blocks",)
            .namespace("ipfs_sqlite_block_store")
    )
    .unwrap();
}

pub fn register(registry: &Registry) -> anyhow::Result<()> {
    registry.register(Box::new(BLOCK_GET_HIST.clone()))?;
    registry.register(Box::new(BLOCK_PUT_HIST.clone()))?;
    Ok(())
}

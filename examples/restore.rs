use ipfs_sqlite_block_store::{BlockStore, Config};
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let _ = BlockStore::open_test("test-data/mini.sqlite", Config::default())?;
    Ok(())
}

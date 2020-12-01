use derive_more::{Display, Error, From};
#[derive(Debug, Display, From, Error)]
pub enum BlockStoreError {
    SqliteError(rusqlite::Error),
    CidError(libipld::cid::Error),
}

pub type Result<T> = std::result::Result<T, BlockStoreError>;

use derive_more::{Display, Error, From};
#[derive(Debug, Display, From, Error)]
pub enum BlockStoreError {
    SqliteError(rusqlite::Error),
    CidError(libipld::cid::Error),
    TryFromIntError(std::num::TryFromIntError),
}

pub type Result<T> = std::result::Result<T, BlockStoreError>;

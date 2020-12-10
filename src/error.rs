use derive_more::{Display, From};

#[derive(Debug, Display, From)]
pub enum BlockStoreError {
    /// Error when interacting with the sqlite database
    SqliteError(rusqlite::Error),
    /// Error convering from a cid to a fixed sized representation.
    /// This can be caused by hashes with more than 32 bytes of size
    CidError(libipld::cid::Error),
    /// Error when converting i64 from sqlite to u64.
    /// This is unlikely to ever happen.
    TryFromIntError(std::num::TryFromIntError),
    /// Other error
    Other(anyhow::Error),
}

impl std::error::Error for BlockStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlockStoreError::SqliteError(e) => Some(e),
            BlockStoreError::CidError(e) => Some(e),
            BlockStoreError::TryFromIntError(e) => Some(e),
            BlockStoreError::Other(e) => Some(e.as_ref()),
        }
    }
}

pub type Result<T> = std::result::Result<T, BlockStoreError>;

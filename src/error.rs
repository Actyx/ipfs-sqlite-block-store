use derive_more::{Display, Error, From};
#[derive(Debug, Display, From, Error)]
pub enum BlockStoreError {
    /// Error when interacting with the sqlite database
    SqliteError(rusqlite::Error),
    /// Error convering from a cid to a fixed sized representation.
    /// This can be caused by hashes with more than 32 bytes of size
    CidError(libipld::cid::Error),
    /// Error when converting i64 from sqlite to u64.
    /// This is unlikely to ever happen.
    TryFromIntError(std::num::TryFromIntError),
}

pub type Result<T> = std::result::Result<T, BlockStoreError>;

use derive_more::Display;

#[derive(Debug, Display)]
pub enum BlockStoreError {
    /// Error when interacting with the sqlite database
    #[display(fmt = "sqlite error while {}: {}", _1, _0)]
    SqliteError(rusqlite::Error, &'static str),
    /// Error convering from a cid to a fixed sized representation.
    /// This can be caused by hashes with more than 32 bytes of size
    #[display(fmt = "error packing a CID into fixed size: {}", _0)]
    CidError(libipld::cid::Error),
    /// Error when converting i64 from sqlite to u64.
    /// This is unlikely to ever happen.
    #[display(
        fmt = "DB corrupted, got unsuitable integer value while {}: {}",
        _1,
        _0
    )]
    TryFromIntError(std::num::TryFromIntError, &'static str),
    /// Other error
    Other(anyhow::Error),
}

impl From<anyhow::Error> for BlockStoreError {
    fn from(v: anyhow::Error) -> Self {
        Self::Other(v)
    }
}

impl From<libipld::cid::Error> for BlockStoreError {
    fn from(v: libipld::cid::Error) -> Self {
        Self::CidError(v)
    }
}

impl std::error::Error for BlockStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlockStoreError::SqliteError(e, _) => Some(e),
            BlockStoreError::CidError(e) => Some(e),
            BlockStoreError::TryFromIntError(e, _) => Some(e),
            BlockStoreError::Other(e) => Some(e.as_ref()),
        }
    }
}

pub type Result<T> = std::result::Result<T, BlockStoreError>;

pub(crate) trait Context {
    type Output;
    fn ctx(self, s: &'static str) -> Self::Output;
}

impl<T> Context for std::result::Result<T, rusqlite::Error> {
    type Output = crate::Result<T>;

    fn ctx(self, s: &'static str) -> Self::Output {
        self.map_err(|e| BlockStoreError::SqliteError(e, s))
    }
}

impl<T> Context for std::result::Result<T, std::num::TryFromIntError> {
    type Output = crate::Result<T>;

    fn ctx(self, s: &'static str) -> Self::Output {
        self.map_err(|e| BlockStoreError::TryFromIntError(e, s))
    }
}

#[cfg(test)]
mod tests {
    // FIXME make sure errors come out right!
}

use derive_more::Display;
use std::error::Error;

#[derive(Debug, Display)]
pub enum BlockStoreError {
    /// Error when interacting with the sqlite database
    #[display(
        fmt = "sqlite error while {}: {} caused by {:?}",
        _1,
        _0,
        "_0.source().map(std::string::ToString::to_string)"
    )]
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
    #[display(fmt = "cannot open additional connection for in-memory DB")]
    NoAdditionalInMemory,
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
            BlockStoreError::SqliteError(_e, _) => None,
            BlockStoreError::CidError(e) => Some(e),
            BlockStoreError::TryFromIntError(e, _) => Some(e),
            BlockStoreError::Other(e) => Some(e.as_ref()),
            BlockStoreError::NoAdditionalInMemory => None,
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
    use super::Context;
    use crate::BlockStoreError;
    use anyhow::Context as _;

    #[test]
    fn show() {
        let sqlite = std::result::Result::<(), _>::Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(517),
            Some("sql string".to_owned()),
        ));

        assert_eq!(format!("x {:?}", sqlite), "x Err(SqliteFailure(Error { code: DatabaseBusy, extended_code: 517 }, Some(\"sql string\")))");
        if let Err(ref sqlite) = sqlite {
            assert_eq!(format!("x {}", sqlite), "x sql string");
            assert_eq!(format!("x {:#}", sqlite), "x sql string");
        }

        let db = sqlite.ctx("first");

        assert_eq!(format!("x {:?}", db), "x Err(SqliteError(SqliteFailure(Error { code: DatabaseBusy, extended_code: 517 }, Some(\"sql string\")), \"first\"))");
        if let Err(ref db) = db {
            assert_eq!(
                format!("x {}", db),
                "x sqlite error while first: sql string caused by \
                Some(\"Error code 517: \
                    Cannot promote read transaction to write transaction because of writes by another connection\")"
            );
            assert_eq!(
                format!("x {:#}", db),
                "x sqlite error while first: sql string caused by \
                Some(\"Error code 517: \
                    Cannot promote read transaction to write transaction because of writes by another connection\")"
            );
        }

        let app = db.context("second").map_err(BlockStoreError::from);

        assert_eq!(
            format!("x {:?}", app),
            r#"x Err(Other(second

Caused by:
    sqlite error while first: sql string caused by Some("Error code 517: Cannot promote read transaction to write transaction because of writes by another connection")))"#
        );
        if let Err(ref app) = app {
            assert_eq!(format!("x {}", app), "x second");
            assert_eq!(format!("x {:#}", app), "x second: \
                sqlite error while first: \
                sql string caused by Some(\"Error code 517: \
                Cannot promote read transaction to write transaction because of writes by another connection\")");
        }
    }
}

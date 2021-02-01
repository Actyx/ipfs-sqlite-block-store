use libipld::cid::{self, Cid};
use rusqlite::{
    types::ToSqlOutput,
    types::{FromSql, FromSqlError, ValueRef},
    ToSql,
};
use std::{convert::TryFrom, io::Cursor};

/// This is sufficient for 32 byte hashes like sha2-256, but not for exotic hashes.
const MAX_SIZE: usize = 39;

/// a representation of a cid that implements AsRef<[u8]>
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CidBytes {
    size: u8,
    data: [u8; MAX_SIZE],
}

impl CidBytes {
    fn len(&self) -> usize {
        self.size as usize
    }
}

impl AsRef<[u8]> for CidBytes {
    fn as_ref(&self) -> &[u8] {
        &self.data[0..self.len()]
    }
}

impl Default for CidBytes {
    fn default() -> Self {
        Self {
            size: 0,
            data: [0; MAX_SIZE],
        }
    }
}

impl TryFrom<&Cid> for CidBytes {
    type Error = cid::Error;

    fn try_from(value: &Cid) -> Result<Self, Self::Error> {
        let mut res = Self::default();
        value.write_bytes(&mut res)?;
        Ok(res)
    }
}

impl TryFrom<&CidBytes> for Cid {
    type Error = cid::Error;

    fn try_from(value: &CidBytes) -> Result<Self, Self::Error> {
        Cid::read_bytes(Cursor::new(value.as_ref()))
    }
}

impl TryFrom<&[u8]> for CidBytes {
    type Error = cid::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut res = CidBytes::default();
        if value.len() < 64 {
            res.size = value.len() as u8;
            res.data[0..value.len()].copy_from_slice(value);
            Ok(res)
        } else {
            Err(cid::Error::ParsingError)
        }
    }
}

impl ToSql for CidBytes {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(self.as_ref())))
    }
}

impl FromSql for CidBytes {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        Ok(CidBytes::try_from(value.as_blob()?).map_err(|_| FromSqlError::InvalidType)?)
    }
}

impl std::io::Write for CidBytes {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.len();
        let cap: usize = MAX_SIZE - len;
        let n = cap.min(buf.len());
        self.data[len..len + n].copy_from_slice(&buf[0..n]);
        self.size += n as u8;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// fn to_cid_bytes(cids: impl Iterator<Item = Cid>) -> impl Iterator<Item = cid::Result<CidBytes>> {
//     cids.map(|x| CidBytes::try_from(&x))
// }

// fn from_cid_bytes(cids: impl Iterator<Item = CidBytes>) -> impl Iterator<Item = cid::Result<Cid>> {
//     cids.map(|x| Cid::try_from(&x))
// }

use bytes::{BufMut, BytesMut};

use macaddr::MacAddr8;
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::boxed::Box as StdBox;
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct PgMacAddr8(MacAddr8);

impl fmt::Display for PgMacAddr8 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let w = format!("{}", self.0).to_ascii_lowercase();
        write!(f, "{w}")
    }
}

impl FromSql<'_> for PgMacAddr8 {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let macaddr8 = macaddr8_from_sql(raw)?;
        Ok(PgMacAddr8(MacAddr8::new(
            macaddr8[0],
            macaddr8[1],
            macaddr8[2],
            macaddr8[3],
            macaddr8[4],
            macaddr8[5],
            macaddr8[6],
            macaddr8[7],
        )))
    }

    accepts!(MACADDR8);
}

impl ToSql for PgMacAddr8 {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.0.as_bytes());
        macaddr8_to_sql(bytes, w);
        Ok(IsNull::No)
    }

    accepts!(MACADDR8);
    to_sql_checked!();
}

/// Serializes a `MACADDR8` value.
#[inline]
pub fn macaddr8_to_sql(v: [u8; 8], buf: &mut BytesMut) {
    buf.put_slice(&v);
}

/// Deserializes a `MACADDR8` value.
#[inline]
pub fn macaddr8_from_sql(buf: &[u8]) -> Result<[u8; 8], StdBox<dyn Error + Sync + Send>> {
    if buf.len() != 8 {
        return Err("invalid message length: macaddr length mismatch".into());
    }
    let mut out = [0; 8];
    out.copy_from_slice(buf);
    Ok(out)
}

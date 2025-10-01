use bytes::BytesMut;
use postgres_types::{IsNull, accepts, to_sql_checked};

use std::fmt::Display;

use postgres_types::{FromSql, ToSql, Type};
use std::{error::Error, fmt::Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PgXid(u32);

impl<'a> FromSql<'a> for PgXid {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // XIDs are stored as 4-byte big-endian
        let array: [u8; 4] = raw.try_into().expect("Bad Xid");
        let res = u32::from_be_bytes(array);

        Ok(Self(res))
    }

    accepts!(XID);
}

impl ToSql for PgXid {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0.to_be_bytes());
        Ok(IsNull::No)
    }

    accepts!(XID);
    to_sql_checked!();
}

impl Display for PgXid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;

        Ok(())
    }
}

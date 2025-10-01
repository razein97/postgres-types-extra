use bytes::{Buf, BufMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt};

#[derive(Debug, Clone)]
pub struct MyPgLsn {
    pub lsn: u64,
}

impl fmt::Display for MyPgLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", (self.lsn >> 32) as u32, self.lsn)
    }
}

impl FromSql<'_> for MyPgLsn {
    fn from_sql(_: &Type, mut raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let lsn = raw.get_u64();

        Ok(MyPgLsn { lsn })
    }

    accepts!(PG_LSN);
}

impl ToSql for MyPgLsn {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if ty.name() != "pg_lsn" {
            return Err("Unexpected type".into());
        }

        out.put_u64(self.lsn);

        Ok(IsNull::No)
    }

    accepts!(PG_LSN);

    to_sql_checked!();
}

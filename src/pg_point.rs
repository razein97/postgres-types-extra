use bytes::{Buf, BufMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq)]
pub struct PgPoint {
    pub x: f64,
    pub y: f64,
}

impl fmt::Display for PgPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.x, self.y)
    }
}

impl FromSql<'_> for PgPoint {
    fn from_sql(ty: &Type, mut raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "point" {
            return Err("Unexpected type".into());
        }
        let x = raw.get_f64();
        let y = raw.get_f64();
        Ok(PgPoint { x, y })
    }

    accepts!(POINT);
}

impl ToSql for PgPoint {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if ty.name() != "point" {
            return Err("Unexpected type".into());
        }

        out.put_f64(self.x);
        out.put_f64(self.y);

        Ok(IsNull::No)
    }

    accepts!(POINT);

    to_sql_checked!();
}

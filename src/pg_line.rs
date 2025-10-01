use bytes::{Buf, BufMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt};

#[derive(Debug, Clone)]
pub struct PgLine {
    pub a: f64,
    pub b: f64,
    pub c: f64,
}

impl fmt::Display for PgLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{},{},{}}}", self.a, self.b, self.c)
    }
}

impl FromSql<'_> for PgLine {
    fn from_sql(ty: &Type, mut raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "line" {
            return Err("Unexpected type".into());
        }
        let a = raw.get_f64();
        let b = raw.get_f64();
        let c = raw.get_f64();
        Ok(PgLine { a, b, c })
    }

    accepts!(LINE);
}

impl ToSql for PgLine {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if ty.name() != "line" {
            return Err("Unexpected type".into());
        }

        out.put_f64(self.a);
        out.put_f64(self.b);
        out.put_f64(self.c);

        Ok(IsNull::No)
    }

    accepts!(LINE);

    to_sql_checked!();
}

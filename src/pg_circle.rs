use bytes::{Buf, BufMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt};

use super::pg_point::PgPoint;

#[derive(Debug, Clone)]
pub struct PgCircle {
    pub center: PgPoint,
    pub radius: f64,
}

impl fmt::Display for PgCircle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{},{}>", self.center, self.radius)
    }
}

impl FromSql<'_> for PgCircle {
    fn from_sql(ty: &Type, mut raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "circle" {
            return Err("Unexpected type".into());
        }
        let x = raw.get_f64();
        let y = raw.get_f64();
        let radius = raw.get_f64();
        Ok(PgCircle {
            center: PgPoint { x, y },
            radius,
        })
    }

    accepts!(CIRCLE);
}

impl ToSql for PgCircle {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if ty.name() != "circle" {
            return Err("Unexpected type".into());
        }

        out.put_f64(self.center.x);
        out.put_f64(self.center.y);
        out.put_f64(self.radius);

        Ok(IsNull::No)
    }

    accepts!(CIRCLE);

    to_sql_checked!();
}

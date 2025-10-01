use bytes::{Buf, BufMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt};

use super::pg_point::PgPoint;

#[derive(Debug, Clone, PartialEq)]
pub struct PgPolygon {
    pub points: Vec<PgPoint>,
}

impl fmt::Display for PgPolygon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({})",
            self.points
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

impl FromSql<'_> for PgPolygon {
    fn from_sql(ty: &Type, mut raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "polygon" {
            return Err("Unexpected type".into());
        }
        let npoints = raw.get_i32();
        let mut points = Vec::with_capacity(npoints as usize);
        for _ in 0..npoints {
            let x = raw.get_f64();
            let y = raw.get_f64();
            points.push(PgPoint { x, y });
        }
        Ok(PgPolygon { points })
    }

    accepts!(POLYGON);
}

impl ToSql for PgPolygon {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if ty.name() != "polygon" {
            return Err("Unexpected type".into());
        }

        let npoints: i32 = self.points.len().try_into()?;
        out.put_i32(npoints);

        for pt in &self.points {
            out.put_f64(pt.x);
            out.put_f64(pt.y);
        }

        Ok(IsNull::No)
    }

    accepts!(POLYGON);

    to_sql_checked!();
}

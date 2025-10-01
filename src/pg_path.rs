use bytes::{Buf, BufMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt};

use super::pg_point::PgPoint;

#[derive(Debug, Clone, PartialEq)]
pub struct PgPath {
    pub points: Vec<PgPoint>,
    pub is_closed: bool,
}

impl fmt::Display for PgPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path_type = if self.is_closed { "(" } else { "[" };
        write!(
            f,
            "{}{}{})",
            path_type,
            self.points
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
            if self.is_closed { ")" } else { "]" }
        )
    }
}

impl FromSql<'_> for PgPath {
    fn from_sql(ty: &Type, mut raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "path" {
            return Err("Unexpected type".into());
        }
        let is_closed = raw.get_u8() != 0;
        let npoints = raw.get_i32();
        let mut points = Vec::with_capacity(npoints as usize);
        for _ in 0..npoints {
            let x = raw.get_f64();
            let y = raw.get_f64();
            points.push(PgPoint { x, y });
        }
        Ok(PgPath { points, is_closed })
    }

    accepts!(PATH);
}

impl ToSql for PgPath {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if ty.name() != "path" {
            return Err("Unexpected type".into());
        }

        // Write closed flag (u8)
        out.put_u8(if self.is_closed { 1 } else { 0 });

        // Write number of points (i32)
        let npoints: i32 = self.points.len().try_into()?;
        out.put_i32(npoints);

        // Write each point as (f64, f64)
        for pt in &self.points {
            out.put_f64(pt.x);
            out.put_f64(pt.y);
        }

        Ok(IsNull::No)
    }

    accepts!(PATH);

    to_sql_checked!();
}

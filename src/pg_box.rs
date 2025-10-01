use bytes::{Buf, BufMut, BytesMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt};

use crate::pg_point::PgPoint;

#[derive(Debug)]
pub struct PgBox {
    pub high: PgPoint,
    pub low: PgPoint,
}

impl fmt::Display for PgBox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({},{}),({},{})",
            self.high.x, self.high.y, self.low.x, self.low.y
        )
    }
}

impl FromSql<'_> for PgBox {
    fn from_sql(ty: &Type, mut raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "box" {
            return Err("Unexpected type".into());
        }
        let high_x = raw.get_f64();
        let high_y = raw.get_f64();
        let low_x = raw.get_f64();
        let low_y = raw.get_f64();
        Ok(PgBox {
            high: PgPoint {
                x: high_x,
                y: high_y,
            },
            low: PgPoint { x: low_x, y: low_y },
        })
    }

    accepts!(BOX);
}

impl ToSql for PgBox {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if ty.name() != "box" {
            return Err("Unexpected type".into());
        }

        // Write 4 f64 values in network (big-endian) order
        out.put_f64(self.high.x);
        out.put_f64(self.high.y);
        out.put_f64(self.low.x);
        out.put_f64(self.low.y);

        Ok(IsNull::No)
    }

    accepts!(BOX);

    to_sql_checked!();
}

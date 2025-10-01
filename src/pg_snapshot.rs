use byteorder::{NetworkEndian, ReadBytesExt};
use bytes::BufMut;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use std::error::Error;
use std::fmt;
use std::io::Cursor;

#[derive(Debug, PartialEq)]
pub struct PgSnapshot {
    xmin: i64,
    xmax: i64,
    xip_list: Vec<i64>,
}

impl<'a> FromSql<'a> for PgSnapshot {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let mut rdr = Cursor::new(raw);

        let nxip = rdr.read_i32::<NetworkEndian>()?;
        let xmin = rdr.read_i64::<NetworkEndian>()?;
        let xmax = rdr.read_i64::<NetworkEndian>()?;

        let mut xip_list = Vec::with_capacity(nxip as usize);
        for _ in 0..nxip {
            xip_list.push(rdr.read_i64::<NetworkEndian>()?);
        }

        Ok(PgSnapshot {
            xmin,
            xmax,
            xip_list,
        })
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "pg_snapshot"
    }
}

impl ToSql for PgSnapshot {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_i32(self.xip_list.len() as i32);
        out.put_i64(self.xmin);
        out.put_i64(self.xmax);

        for &xip in &self.xip_list {
            out.put_i64(xip);
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "pg_snapshot"
    }

    to_sql_checked!();
}

impl fmt::Display for PgSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.xmin, self.xmax)?;
        if !self.xip_list.is_empty() {
            write!(f, ":")?;
            for (i, xip) in self.xip_list.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                write!(f, "{xip}")?;
            }
        } else {
            write!(f, ":")?;
        }
        Ok(())
    }
}

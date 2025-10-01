use byteorder::{BigEndian, ReadBytesExt};
use bytes::BufMut;
use chrono::{Duration, FixedOffset, NaiveTime, Timelike};
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use std::{error::Error, fmt, io::Cursor};

#[derive(Debug, PartialEq)]
pub struct PgTimeWithTz {
    pub time: NaiveTime,
    pub offset: FixedOffset,
}

impl fmt::Display for PgTimeWithTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}{:?}", self.time, self.offset)
    }
}

impl<'a> FromSql<'a> for PgTimeWithTz {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "timetz" {
            return Err("Unexpected type".into());
        }

        let mut buf = Cursor::new(raw);

        // TIME is encoded as the microseconds since midnight
        let us = buf.read_i64::<BigEndian>()?;
        // default is midnight
        let time = NaiveTime::default() + Duration::microseconds(us);

        // OFFSET is encoded as seconds from UTC
        let offset_seconds = buf.read_i32::<BigEndian>()?;

        let offset = FixedOffset::east_opt(-offset_seconds).ok_or_else(|| {
            format!("server returned out-of-range offset for `TIMETZ`: {offset_seconds} seconds")
        })?;

        Ok(PgTimeWithTz { time, offset })
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "timetz"
    }
}

impl ToSql for PgTimeWithTz {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if ty.name() != "timetz" {
            return Err("Unexpected type".into());
        }

        // Encode time as microseconds since midnight
        let us = (i64::from(self.time.num_seconds_from_midnight()) * 1_000_000)
            + (i64::from(self.time.nanosecond()) / 1000);

        out.put_i64(us);

        // Encode offset as seconds from UTC
        let offset_seconds = -(self.offset.local_minus_utc());

        out.put_i32(offset_seconds);

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "timetz"
    }

    to_sql_checked!();
}

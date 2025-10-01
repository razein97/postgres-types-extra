use byteorder::{NetworkEndian, ReadBytesExt};
use bytes::BufMut;
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt, io::Cursor};

#[derive(Debug, Eq, PartialEq, Clone, Hash, Default)]
pub struct PgInterval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl fmt::Display for PgInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format_pg_interval(self))
    }
}

impl<'a> FromSql<'a> for PgInterval {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "interval" {
            return Err("Unexpected type".into());
        }

        let mut buf = Cursor::new(raw);

        let microseconds = buf.read_i64::<NetworkEndian>()?;
        let days = buf.read_i32::<NetworkEndian>()?;
        let months = buf.read_i32::<NetworkEndian>()?;

        Ok(PgInterval {
            months,
            days,
            microseconds,
        })
    }

    accepts!(INTERVAL);
}

fn format_pg_interval(interval: &PgInterval) -> String {
    let mut parts = Vec::new();
    if interval.months != 0 {
        parts.push(format!(
            "{} month{}",
            interval.months,
            if interval.months == 1 { "" } else { "s" }
        ));
    }
    if interval.days != 0 {
        parts.push(format!(
            "{} day{}",
            interval.days,
            if interval.days == 1 { "" } else { "s" }
        ));
    }
    if interval.microseconds != 0 {
        let seconds = interval.microseconds / 1_000_000;
        let microseconds = interval.microseconds % 1_000_000;
        if seconds != 0 {
            parts.push(format!(
                "{} second{}",
                seconds,
                if seconds == 1 { "" } else { "s" }
            ));
        }
        if microseconds != 0 {
            parts.push(format!(
                "{} microsecond{}",
                microseconds,
                if microseconds == 1 { "" } else { "s" }
            ));
        }
    }
    if parts.is_empty() {
        "0 seconds".to_string()
    } else {
        parts.join(", ")
    }
}

impl ToSql for PgInterval {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        if ty.name() != "interval" {
            return Err("Unexpected type".into());
        }

        out.put_i64(self.microseconds);
        out.put_i32(self.days);
        out.put_i32(self.months);

        Ok(IsNull::No)
    }

    accepts!(INTERVAL);

    to_sql_checked!();
}

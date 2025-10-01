use std::fmt::{self, Debug, Display, Formatter};
use std::ops::{Bound, Range, RangeBounds, RangeFrom, RangeInclusive, RangeTo, RangeToInclusive};

use bitflags::bitflags;
use bytes::{BufMut, BytesMut};
use postgres_types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PgRange<T> {
    pub start: Bound<T>,
    pub end: Bound<T>,
}
// PostgreSQL range type flags
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct RangeFlags: u8 {
        const EMPTY = 0x01;
        const LB_INC = 0x02;
        const UB_INC = 0x04;
        const LB_INF = 0x08;
        const UB_INF = 0x10;
        const LB_NULL = 0x20; // not used
        const UB_NULL = 0x40; // not used
        const CONTAIN_EMPTY = 0x80; // internal
    }
}

impl<T> From<[Bound<T>; 2]> for PgRange<T> {
    fn from(v: [Bound<T>; 2]) -> Self {
        let [start, end] = v;
        Self { start, end }
    }
}

// Conversions from standard range types to PgRange
impl<T> From<Range<T>> for PgRange<T> {
    fn from(v: Range<T>) -> Self {
        Self {
            start: Bound::Included(v.start),
            end: Bound::Excluded(v.end),
        }
    }
}

impl<T> From<RangeFrom<T>> for PgRange<T> {
    fn from(v: RangeFrom<T>) -> Self {
        Self {
            start: Bound::Included(v.start),
            end: Bound::Unbounded,
        }
    }
}

impl<T> From<RangeInclusive<T>> for PgRange<T> {
    fn from(v: RangeInclusive<T>) -> Self {
        let (start, end) = v.into_inner();
        Self {
            start: Bound::Included(start),
            end: Bound::Included(end),
        }
    }
}

impl<T> From<RangeTo<T>> for PgRange<T> {
    fn from(v: RangeTo<T>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Excluded(v.end),
        }
    }
}

impl<T> From<RangeToInclusive<T>> for PgRange<T> {
    fn from(v: RangeToInclusive<T>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Included(v.end),
        }
    }
}

impl<T> RangeBounds<T> for PgRange<T> {
    fn start_bound(&self) -> Bound<&T> {
        match self.start {
            Bound::Included(ref start) => Bound::Included(start),
            Bound::Excluded(ref start) => Bound::Excluded(start),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&T> {
        match self.end {
            Bound::Included(ref end) => Bound::Included(end),
            Bound::Excluded(ref end) => Bound::Excluded(end),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

// Implement ToSql trait for encoding range types to PostgreSQL
impl<T> ToSql for PgRange<T>
where
    T: ToSql + Sync,
{
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let mut flags = RangeFlags::empty();

        flags |= match self.start {
            Bound::Included(_) => RangeFlags::LB_INC,
            Bound::Unbounded => RangeFlags::LB_INF,
            Bound::Excluded(_) => RangeFlags::empty(),
        };

        flags |= match self.end {
            Bound::Included(_) => RangeFlags::UB_INC,
            Bound::Unbounded => RangeFlags::UB_INF,
            Bound::Excluded(_) => RangeFlags::empty(),
        };

        out.put_u8(flags.bits());

        if let Bound::Included(v) | Bound::Excluded(v) = &self.start {
            v.to_sql(ty, out)?;
        }

        if let Bound::Included(v) | Bound::Excluded(v) = &self.end {
            v.to_sql(ty, out)?;
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Range(_))
    }

    to_sql_checked!();
}

impl<'a, T: FromSql<'a>> FromSql<'a> for PgRange<T> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let mut buf = raw;
        let flags = RangeFlags::from_bits_truncate(buf[0]);
        buf = &buf[1..];

        let mut start = Bound::Unbounded;
        let mut end = Bound::Unbounded;

        if flags.contains(RangeFlags::EMPTY) {
            return Ok(PgRange { start, end });
        }

        if !flags.contains(RangeFlags::LB_INF) {
            let value = T::from_sql(ty, buf)?;
            start = if flags.contains(RangeFlags::LB_INC) {
                Bound::Included(value)
            } else {
                Bound::Excluded(value)
            };
            buf = &buf[std::mem::size_of::<T>()..];
        }

        if !flags.contains(RangeFlags::UB_INF) {
            let value = T::from_sql(ty, buf)?;
            end = if flags.contains(RangeFlags::UB_INC) {
                Bound::Included(value)
            } else {
                Bound::Excluded(value)
            };
        }

        Ok(PgRange { start, end })
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Range(_))
    }
}

// Helper function to parse bounds from character
fn _parse_bound<T>(
    ch: char,
    value: Option<T>,
) -> Result<Bound<T>, Box<dyn std::error::Error + Sync + Send>> {
    Ok(if let Some(value) = value {
        match ch {
            '(' | ')' => Bound::Excluded(value),
            '[' | ']' => Bound::Included(value),
            _ => {
                return Err(format!(
                    "expected `(`, ')', '[', or `]` but found `{ch}` for range literal"
                )
                .into());
            }
        }
    } else {
        Bound::Unbounded
    })
}

// Implement Display for PgRange
impl<T> Display for PgRange<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.start {
            Bound::Unbounded => f.write_str("(,")?,
            Bound::Excluded(v) => write!(f, "({v},")?,
            Bound::Included(v) => write!(f, "[{v},")?,
        }

        match &self.end {
            Bound::Unbounded => f.write_str(")")?,
            Bound::Excluded(v) => write!(f, "{v})")?,
            Bound::Included(v) => write!(f, "{v}]")?,
        }

        Ok(())
    }
}

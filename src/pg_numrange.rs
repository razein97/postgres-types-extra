use postgres_range::{BoundSided, Normalizable, RangeBound};
use postgres_types::{FromSql, IsNull, ToSql, Type};
use rust_decimal::Decimal;
use std::error::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NumRange(pub Decimal);

impl Normalizable for NumRange {
    fn normalize<S>(bound: RangeBound<S, NumRange>) -> RangeBound<S, NumRange>
    where
        S: BoundSided,
    {
        bound
    }
}

impl<'a> FromSql<'a> for NumRange {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Decimal::from_sql(ty, raw).map(NumRange)
    }

    fn accepts(ty: &Type) -> bool {
        <Decimal as FromSql>::accepts(ty)
    }
}

impl ToSql for NumRange {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        <Decimal as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.0.to_sql_checked(ty, out)
    }
}

impl From<Decimal> for NumRange {
    fn from(d: Decimal) -> Self {
        NumRange(d)
    }
}

impl From<NumRange> for Decimal {
    fn from(d: NumRange) -> Self {
        d.0
    }
}

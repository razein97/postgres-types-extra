use bytes::BytesMut;
use cidr::{IpCidr, IpInet};
use postgres_protocol::types;
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use std::{error::Error, fmt};

#[derive(Debug, Clone)]
pub struct PgCidr(IpCidr);

#[derive(Debug, Clone)]

pub struct PgInet(IpInet);

impl fmt::Display for PgCidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let w = format!("{}", self.0).to_ascii_lowercase();
        write!(f, "{w}")
    }
}

impl FromSql<'_> for PgCidr {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let inet = types::inet_from_sql(raw)?;
        Ok(PgCidr(IpCidr::new(inet.addr(), inet.netmask())?))
    }

    accepts!(CIDR);
}

impl ToSql for PgCidr {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::inet_to_sql(self.0.first_address(), self.0.network_length(), w);
        Ok(IsNull::No)
    }

    accepts!(CIDR);
    to_sql_checked!();
}

impl fmt::Display for PgInet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let w = format!("{}", self.0).to_ascii_lowercase();
        write!(f, "{w}")
    }
}
impl FromSql<'_> for PgInet {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let inet = types::inet_from_sql(raw)?;
        Ok(PgInet(IpInet::new(inet.addr(), inet.netmask())?))
    }

    accepts!(INET);
}

impl ToSql for PgInet {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::inet_to_sql(self.0.address(), self.0.network_length(), w);
        Ok(IsNull::No)
    }

    accepts!(INET);
    to_sql_checked!();

    fn encode_format(&self, _ty: &Type) -> postgres_types::Format {
        postgres_types::Format::Binary
    }
}

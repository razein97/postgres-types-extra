use std::{error::Error, fmt};

use bytes::{BufMut, BytesMut};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};

#[derive(Debug, Clone)]
pub struct PgXml(String);

impl FromSql<'_> for PgXml {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if ty.name() != "xml" {
            return Err("Unexpected type".into());
        }
        let xml = String::from_utf8(raw.to_vec())?;
        Ok(PgXml(xml))
    }

    accepts!(XML);
}

impl fmt::Display for PgXml {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // write!(f, "XML: {}", self.0)
        write!(f, "{}", self.0)
    }
}

impl ToSql for PgXml {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if ty.name() != "xml" {
            return Err("Unexpected type".into());
        }
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    accepts!(XML);

    to_sql_checked!();
}

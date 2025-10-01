use bytes::{Buf, BufMut};
use core::str;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::mem;
use std::{error::Error, fmt::Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PgHstore(pub BTreeMap<String, Option<String>>);

impl<'a> FromSql<'a> for PgHstore {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let mut buf = raw;
        let len = read_length(&mut buf)?;

        let len =
            usize::try_from(len).map_err(|_| format!("PgHstore: length out of range: {len}"))?;

        let mut result = Self::default();

        for i in 0..len {
            let key = read_string(&mut buf)
                .map_err(|e| format!("PgHstore: error reading {i}th key: {e}"))?
                .ok_or_else(|| format!("PgHstore: expected {i}th key, got nothing"))?;

            let value = read_string(&mut buf)
                .map_err(|e| format!("PgHstore: error reading value for key {key:?}: {e}"))?;

            result.0.insert(key, value);
        }

        if !buf.is_empty() {
            eprintln!("{} unread bytes at the end of HSTORE value", buf.len());
        }

        Ok(result)
    }
    fn accepts(ty: &Type) -> bool {
        ty.name() == "hstore"
    }
}

impl Display for PgHstore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut hstore_arr = Vec::new();

        for key in self.0.keys() {
            if let Some(Some(val)) = self.0.get(key) {
                hstore_arr.push(format!("\"{key}\"=>\"{val}\""));
            }
        }

        write!(f, "{}", hstore_arr.join(", "))?;

        Ok(())
    }
}

fn read_length(buf: &mut &[u8]) -> Result<i32, String> {
    if buf.len() < mem::size_of::<i32>() {
        return Err(format!(
            "expected {} bytes, got {}",
            mem::size_of::<i32>(),
            buf.len()
        ));
    }

    Ok(buf.get_i32())
}

fn read_string(buf: &mut &[u8]) -> Result<Option<String>, String> {
    let len = read_length(buf)?;

    match len {
        -1 => Ok(None),
        len => {
            let len =
                usize::try_from(len).map_err(|_| format!("string length out of range: {len}"))?;

            if buf.len() < len {
                return Err(format!("expected {len} bytes, got {}", buf.len()));
            }

            let (val, rest) = buf.split_at(len);
            *buf = rest;

            Ok(Some(
                str::from_utf8(val).map_err(|e| e.to_string())?.to_string(),
            ))
        }
    }
}

impl ToSql for PgHstore {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if ty.name() != "hstore" {
            return Err("PgHstore: unexpected type".into());
        }

        // number of pairs
        let len: i32 = self.0.len().try_into()?;
        out.put_i32(len);

        for (key, value) in &self.0 {
            // Key: length + UTF8 bytes
            let key_bytes = key.as_bytes();
            out.put_i32(key_bytes.len() as i32);
            out.put_slice(key_bytes);

            match value {
                Some(val) => {
                    let val_bytes = val.as_bytes();
                    out.put_i32(val_bytes.len() as i32);
                    out.put_slice(val_bytes);
                }
                None => {
                    // NULL → -1
                    out.put_i32(-1);
                }
            }
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "hstore"
    }

    to_sql_checked!();
}

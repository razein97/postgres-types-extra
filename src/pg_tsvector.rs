use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use core::str;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use std::fmt::{Display, Write};
use std::io::{BufRead, Cursor};
use std::{error::Error, fmt::Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct Lexeme {
    pub word: String,
    pub positions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PgTsVector {
    pub words: Vec<Lexeme>,
}

impl<'a> FromSql<'a> for PgTsVector {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let mut reader = Cursor::new(raw);
        let mut words = vec![];

        let num_lexemes = reader.read_u32::<BigEndian>()?;

        for _ in 0..num_lexemes {
            let mut lexeme = vec![];

            reader.read_until(b'\0', &mut lexeme)?;

            let num_positions = reader.read_u16::<BigEndian>()?;
            let mut positions = Vec::<i32>::with_capacity(num_positions as usize);

            if num_positions > 0 {
                for _ in 0..num_positions {
                    let position = reader.read_u16::<BigEndian>()?;
                    positions.push(position as i32);
                }
            }

            words.push(Lexeme {
                word: str::from_utf8(&lexeme)?.trim_end_matches('\0').to_string(),
                positions,
            });
        }

        Ok(Self { words })
    }
    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsvector"
    }
}

impl Display for PgTsVector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut words = self.words.iter().peekable();

        while let Some(word) = words.next() {
            f.write_str(&format!(
                "'{}':{}",
                word.word,
                word.positions
                    .iter()
                    .map(|pos| pos.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ))?;
            if words.peek().is_some() {
                f.write_char(' ')?;
            }
        }

        Ok(())
    }
}

impl ToSql for PgTsVector {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // Write number of lexemes
        out.put_u32(self.words.len() as u32);

        for lexeme in &self.words {
            // Write lexeme string with null terminator
            out.put_slice(lexeme.word.as_bytes());
            out.put_u8(b'\0');

            // Write number of positions
            out.put_u16(lexeme.positions.len() as u16);

            // Write positions
            for &position in &lexeme.positions {
                out.put_u16(position as u16);
            }
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsvector"
    }

    to_sql_checked!();
}

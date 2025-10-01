/*
[0, 0, 0, 5, 2, 3, 1, 0, 0, 99, 97, 116, 0, 2, 2, 1, 0, 0, 114, 97, 116, 0, 1, 0, 0, 102, 97, 116, 0] == 'fat' & 'rat' | 'cat'
[0, 0, 0, 5, -- 5 items
2, 3, -- operator 3 '|'
text:entry -> weight -> prefix -> string 'cat' -> terminated by 0
1, 0, 0, 99, 97, 116, 0,
operator:entry -> operator 2 '&''
2, 2,
text:entry -> weight -> prefix -> string 'rat' -> terminated by 0
1, 0, 0, 114, 97, 116, 0,
text:entry -> weight -> prefix -> string 'fat' -> terminated by 0
1, 0, 0, 102, 97, 116, 0]
*/

/*
To understand how the raw data in the format | & eleph ! bird & | cat dog lazi is converted to the output 'lazi' & ( 'dog' | 'cat' ) | !'bird' & 'eleph' in PostgreSQL's ts_query, let's break down the process.

1. Understanding the Raw Data Format
The raw data represents a postfix (or Reverse Polish Notation - RPN) expression where operators come after their operands. Here's how the provided string maps to this notation:

Operands: lazi, dog, cat, bird, eleph
Operators: |, &, ! (OR, AND, NOT)
The order of the elements indicates how these operands and operators should be combined:

| & eleph ! bird & | cat dog lazi
lazi, dog, cat, bird, eleph are the individual terms.
|, &, ! are the logical operators, and their position in the sequence determines how the terms are logically combined.
2. Processing Postfix Notation
PostgreSQL’s ts_query engine processes the input in postfix notation by following these rules:

Operands (Values) are pushed onto a stack.
When an operator is encountered, it pops the required number of operands from the stack, applies the operator, and then pushes the result back onto the stack.
Given the postfix expression | & eleph ! bird & | cat dog lazi, let's break down how PostgreSQL would process it:

Step-by-Step Evaluation
Push lazi onto the stack.
Push dog onto the stack.
Push cat onto the stack.
Apply | (OR): Pop dog and cat, combine them as 'dog' | 'cat', and push the result back onto the stack.
Stack: ['lazi', "'dog' | 'cat'"]
Apply & (AND): Pop lazi and the result of the previous OR operation, combine them as 'lazi' & ('dog' | 'cat'), and push the result back onto the stack.
Stack: ["'lazi' & ('dog' | 'cat')"]
Push bird onto the stack.
Apply ! (NOT): Pop bird, apply NOT, resulting in !'bird', and push the result back onto the stack.
Stack: ["'lazi' & ('dog' | 'cat')", "!'bird'"]
Push eleph onto the stack.
Apply & (AND): Pop !'bird' and eleph, combine them as !'bird' & 'eleph', and push the result back onto the stack.
Stack: ["'lazi' & ('dog' | 'cat')", "!'bird' & 'eleph'"]
Apply | (OR): Pop the two previous results from the stack, combine them as ('lazi' & ('dog' | 'cat')) | (!'bird' & 'eleph'), and push the final result back onto the stack.
Final Result
The final expression is:

Copy code
'lazi' & ('dog' | 'cat') | !'bird' & 'eleph'
This represents how the raw data is evaluated in PostgreSQL's ts_query engine to produce the correct infix expression.

3. How PostgreSQL Handles This Internally
PostgreSQL's tsquery uses an internal tree structure to represent the query. Each node can be an operand (a term in the query) or an operator (AND, OR, NOT).
When parsing a query, PostgreSQL constructs this tree based on operator precedence and parentheses (if any).
Infix Conversion: PostgreSQL traverses this tree to convert the internal representation into a human-readable infix expression (with operators placed between operands, and proper parentheses to reflect precedence).
Summary
The raw postfix expression | & eleph ! bird & | cat dog lazi is processed by PostgreSQL's ts_query engine, which uses a stack-based approach to apply operators in the correct order.
This processing ultimately results in the infix expression 'lazi' & ('dog' | 'cat') | !'bird' & 'eleph', correctly reflecting operator precedence and logical grouping.
This method of parsing and evaluation ensures that the logical relationships between the terms are preserved in the final query representation.
*/

use bigdecimal::ToPrimitive;
use byteorder::{NetworkEndian, ReadBytesExt};
use bytes::BufMut;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use std::error::Error;
use std::io::{BufRead, Cursor};
use std::{fmt, str};

#[derive(Clone, Debug, PartialEq)]
pub struct PgTsQuery {
    pub entries: Vec<Entry>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, PartialOrd)]
pub enum Operators {
    Not = 1,
    And = 2,
    Or = 3,
    Phrase = 4,
}

impl From<Operators> for i8 {
    fn from(val: Operators) -> Self {
        match val {
            Operators::Not => 1,
            Operators::And => 2,
            Operators::Or => 3,
            Operators::Phrase => 4,
        }
    }
}

impl TryFrom<i8> for Operators {
    type Error = Box<dyn Error>;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Operators::Not),
            2 => Ok(Operators::And),
            3 => Ok(Operators::Or),
            4 => Ok(Operators::Phrase),
            _ => Err("Invalid type".into()),
        }
    }
}

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct Operator {
    pub operator: Operators,
    pub distance: Option<i16>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    pub weight: u8,
    pub text: String,
    pub prefix: u8,
    pub distance: i16,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Entry {
    Operator(Operator),
    Value(Value),
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum EntryType {
    Value = 1,
    Operator = 2,
}

impl From<EntryType> for u8 {
    fn from(val: EntryType) -> Self {
        match val {
            EntryType::Value => 1,
            EntryType::Operator => 2,
        }
    }
}

impl TryFrom<u8> for EntryType {
    type Error = Box<dyn Error>;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EntryType::Value),
            2 => Ok(EntryType::Operator),
            _ => Err("Invalid type".into()),
        }
    }
}

impl<'a> FromSql<'a> for PgTsQuery {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let ts_query = raw.try_into().unwrap();

        Ok(ts_query)
    }
    fn accepts(ty: &Type) -> bool {
        ty.name() == "tsquery"
    }
}

impl fmt::Display for PgTsQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", infix_string(self.entries.clone()))?;
        Ok(())
    }
}

impl TryFrom<&[u8]> for PgTsQuery {
    type Error = Box<dyn Error>;

    /// Decode binary data into [`TsQuery`] based on the binary data format defined in
    /// https://github.com/postgres/postgres/blob/252dcb32397f64a5e1ceac05b29a271ab19aa960/src/backend/utils/adt/tsquery.c#L1174
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut reader = Cursor::new(bytes);
        let size = bytes.len().to_u64().unwrap();

        //First 4 bytes tell you about the content length
        //eg: 'postgr' & 'data' & 'tobon' = 5
        //eg: 'postgr' & 'data' == 0003
        let count = reader.read_u32::<NetworkEndian>()?;

        let mut entries = Vec::<Entry>::with_capacity(count as usize);

        for _ in 0..count {
            let entry_type = reader.read_u8()?;

            if entry_type == 2 {
                let operator_type: Operators = reader.read_i8()?.try_into()?;
                //If the operator is in the last position then error
                if reader.position() == (size - 1) {
                    return Err("Invalid tsquery: invalid pointer to right operand".into());
                }

                if operator_type == Operators::Phrase {
                    //If the operator is a phrase i.e. 4 then read the next two bytes as the distance <3>
                    // The below value gives you ['quick' <2> 'fox' & 'lazi' <3> 'dog']
                    //[0, 0, 0, 7, 2, 2, <2, 4, 0, 3,> 1, 0, 0, 100, 111, 103, 0, 1, 0, 0, 108, 97, 122, 105, 0, <2, 4, 0, 2,> 1, 0, 0, 102, 111, 120, 0, 1, 0, 0, 113, 117, 105, 99, 107, 0]
                    entries.push(Entry::Operator(Operator {
                        operator: operator_type,
                        distance: Some(reader.read_i16::<NetworkEndian>()?),
                    }));
                } else {
                    entries.push(Entry::Operator(Operator {
                        operator: operator_type,
                        distance: None,
                    }));
                }

                // println!("{:?}", operator_type);
            } else if entry_type == 1 {
                // The entry point of the string is '1' byte
                // ['fat':AB & 'rat':B] = [0, 0, 0, 3, 2, 2, text start--[1, 4, 0, 114, 97, 116, 0,]--text end    text start --[1, 12, 0, 102, 97, 116, 0]--text end]
                // text start--[1, 4, 0, 114, 97, 116, 0,]--text end
                // the second byte 4 == weight B check table below
                // text start --[1, 12, 0, 102, 97, 116, 0]--text end
                // the second byte 12 == weight AB check table below
                let weight = reader.read_u8()?;

                // [0, 0, 0, 1, ----[1, 0, 1, 112, 111, 115, 116, 103, 114, 0]] == 'postgr':*
                // the prefix byte 1 = true meaning add * as prefix
                let prefix = reader.read_u8()?;

                let mut text = String::new().into_bytes();
                reader.read_until(0, &mut text)?;
                text.pop();
                let text_utf8 = str::from_utf8(&text)?;

                //this will form distace telling after how many characters to put the operators
                let val_len = text.len().to_i16().unwrap();

                if weight > 15 {
                    return Err(format!("Invalid tsquery: invalid weight={weight}").into());
                }
                if val_len > ((1 << 11) - 1) {
                    return Err(
                        format!("Invalid tsquery: operand too long: length={val_len}").into(),
                    );
                }

                entries.push(Entry::Value(Value {
                    weight,
                    text: text_utf8.to_owned(),
                    prefix,
                    distance: val_len + 1,
                }));
            }
        }

        Ok(PgTsQuery { entries })
    }
}

fn infix_string(mut entries: Vec<Entry>) -> String {
    // println!("{:?}", entries);
    let mut stack: Vec<String> = Vec::new();
    let len = entries.len().to_u32().unwrap();
    let mut priority = 0;

    for num in 0..len {
        match entries.pop() {
            Some(entry) => match entry {
                Entry::Operator(op) => match op.operator {
                    Operators::Not => {
                        if let Some(operand) = stack.pop() {
                            stack.push(format!("!{operand}"));
                            priority = 1;
                        }
                    }
                    Operators::And => {
                        if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                            match 2.cmp(&priority) {
                                std::cmp::Ordering::Less => {
                                    // println!("Less");
                                    stack.push(format!("{left} & {right}"));
                                }
                                std::cmp::Ordering::Equal => {
                                    // println!("Equal");

                                    stack.push(format!("{left} & {right}"));
                                }
                                std::cmp::Ordering::Greater => {
                                    // println!("Greater");

                                    if num == len - 1 {
                                        stack.push(format!("{left} & {right}"));
                                    } else {
                                        stack.push(format!("({left} & {right})"));
                                    }
                                }
                            }
                            priority = 2;
                        }
                    }
                    Operators::Or => {
                        if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                            match 3.cmp(&priority) {
                                std::cmp::Ordering::Less => {
                                    stack.push(format!("{left} | {right}"));
                                }
                                std::cmp::Ordering::Equal => {
                                    stack.push(format!("{left} | {right}"));
                                }
                                std::cmp::Ordering::Greater => {
                                    if num == len - 1 {
                                        stack.push(format!("{left} | {right}"));
                                    } else {
                                        stack.push(format!("({left} | {right})"));
                                    }
                                }
                            }
                            priority = 3;
                        }
                    }

                    Operators::Phrase => {
                        if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                            let op_str = match op.distance {
                                Some(1) | None => "<->".to_string(),
                                Some(distance) => format!("<{distance}>"),
                            };
                            stack.push(format!("{left} {op_str} {right}"));
                            priority = 4;
                        }
                    }
                },
                Entry::Value(val) => {
                    // println!("{:?}", val);
                    if val.prefix > 0 {
                        stack.push(format!("'{}':*", val.text));
                    } else if let Some(weight) = weight_to_string(val.weight) {
                        stack.push(format!("'{}':{}", val.text, weight));
                    } else {
                        stack.push(format!("'{}'", val.text));
                    }
                }
            },
            None => {
                eprintln!("No entry after popped!");
            }
        }
    }

    stack.join(" ").to_string()
}

fn weight_to_string(weight: u8) -> Option<&'static str> {
    match weight {
        0 => None,
        8 => Some("A"),
        4 => Some("B"),
        2 => Some("C"),
        1 => Some("D"),
        12 => Some("AB"),
        10 => Some("AC"),
        9 => Some("AD"),
        6 => Some("BC"),
        5 => Some("BD"),
        3 => Some("CD"),
        14 => Some("ABC"),
        13 => Some("ABD"),
        11 => Some("ACD"),
        7 => Some("BCD"),
        15 => Some("ABCD"),
        _ => None,
    }
}

impl ToSql for PgTsQuery {
    fn to_sql(
        &self,
        _: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Write the count of entries (4 bytes, network endian)
        out.put_u32(self.entries.len() as u32);

        // Write each entry
        for entry in &self.entries {
            match entry {
                Entry::Operator(op) => {
                    // Entry type: 2 for operator
                    out.put_u8(EntryType::Operator.into());

                    // Operator type
                    out.put_i8(op.operator.into());

                    // If it's a phrase operator, write the distance
                    if op.operator == Operators::Phrase {
                        out.put_i16(op.distance.unwrap_or(1));
                    }
                }
                Entry::Value(val) => {
                    // Entry type: 1 for value
                    out.put_u8(EntryType::Value.into());

                    // Weight
                    out.put_u8(val.weight);

                    // Prefix flag
                    out.put_u8(val.prefix);

                    // Text (null-terminated string)
                    out.put_slice(val.text.as_bytes());
                    out.put_u8(0); // null terminator
                }
            }
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        ty.name() == "tsquery"
    }

    to_sql_checked!();
}

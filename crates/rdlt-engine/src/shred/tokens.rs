//! JSON text walked into a seed a token at a time, its numbers exact, as the exact parse walks a
//! parsed document: nothing of the text is built but a string whose escapes are read, so walking
//! a value holds no more than its containers' nesting beside what the seed keeps.
//!
//! Every seed walked here walks each container it is handed to its end, or fails.

#[cfg(test)]
mod tests;

use std::borrow::Cow;

use serde::de::value::{BorrowedStrDeserializer, StringDeserializer};
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
// The fast parse's error, so a seed's visits are built once for both parses.
use sonic_rs::Error;

use super::visit::Context;
use crate::json::{Reader, Text, Token};

/// Walks the JSON value `text` into `seed`.
///
/// # Errors
///
/// Where `text` is not one JSON value, or the seed refuses it.
pub(super) fn visit<'de, S: DeserializeSeed<'de, Value = ()>>(
    text: &'de str,
    seed: S,
    context: &Context,
) -> Result<(), Error> {
    let mut reader = Reader::new(text);
    let token = next(&mut reader)?;
    seed.deserialize(Walked {
        token,
        reader: &mut reader,
        context,
    })?;
    match reader.next() {
        Ok(None) => Ok(()),
        _ => Err(de::Error::custom("more follows the value")),
    }
}

/// The next token `reader` reads, where one is due.
fn next<'de>(reader: &mut Reader<'de>) -> Result<Token<'de>, Error> {
    match reader.next() {
        Ok(Some(token)) => Ok(token),
        Ok(None) => Err(de::Error::custom("the text ends where a token is due")),
        Err(error) => Err(de::Error::custom(error)),
    }
}

/// A value whose first token, `token`, is read, the rest of it still to read from `reader`.
struct Walked<'r, 'de> {
    token: Token<'de>,
    reader: &'r mut Reader<'de>,
    context: &'r Context,
}

impl<'de> Deserializer<'de> for Walked<'_, 'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let Self {
            token,
            reader,
            context,
        } = self;
        match token {
            Token::Null => visitor.visit_unit(),
            Token::Bool(value) => visitor.visit_bool(value),
            Token::Number(text) => super::exact::number(text, context, visitor),
            Token::String(text) => match text.unescaped() {
                Cow::Borrowed(text) => visitor.visit_borrowed_str(text),
                Cow::Owned(text) => visitor.visit_str(&text),
            },
            Token::BeginObject => visitor.visit_map(Members { reader, context }),
            Token::BeginArray => visitor.visit_seq(Items { reader, context }),
            Token::Key(_) | Token::EndObject | Token::EndArray => {
                Err(de::Error::custom("a value is due"))
            }
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier
        ignored_any
    }
}

/// An object's members, read from `reader` to its end.
struct Members<'r, 'de> {
    reader: &'r mut Reader<'de>,
    context: &'r Context,
}

impl<'de> MapAccess<'de> for Members<'_, 'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Error> {
        match next(self.reader)? {
            Token::EndObject => Ok(None),
            Token::Key(key) => key_into(key, seed).map(Some),
            _ => Err(de::Error::custom("a key is due")),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        let token = next(self.reader)?;
        seed.deserialize(Walked {
            token,
            reader: &mut *self.reader,
            context: self.context,
        })
    }
}

/// Hands `seed` the key `key`, its escapes read.
fn key_into<'de, K: DeserializeSeed<'de>>(key: Text<'de>, seed: K) -> Result<K::Value, Error> {
    match key.unescaped() {
        Cow::Borrowed(key) => seed.deserialize(BorrowedStrDeserializer::new(key)),
        Cow::Owned(key) => seed.deserialize(StringDeserializer::new(key)),
    }
}

/// An array's items, read from `reader` to its end.
struct Items<'r, 'de> {
    reader: &'r mut Reader<'de>,
    context: &'r Context,
}

impl<'de> SeqAccess<'de> for Items<'_, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        match next(self.reader)? {
            Token::EndArray => Ok(None),
            token => seed
                .deserialize(Walked {
                    token,
                    reader: &mut *self.reader,
                    context: self.context,
                })
                .map(Some),
        }
    }
}

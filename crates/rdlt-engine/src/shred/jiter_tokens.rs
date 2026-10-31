//! A record walked into a seed over jiter, a token at a time, its numbers exact: jiter keeps no
//! container of its own, so the walk holds the nesting limit, and a number is handed over as it is
//! written.
//!
//! Every seed walked here walks each container it is handed to its end, or fails.

#[cfg(test)]
mod tests;

use jiter::{Jiter, JiterError, Peek};
use rdlt_wire::limits::NESTING_DEPTH;
use serde::de::value::StrDeserializer;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
// The fast parse's error, so a seed's visits are built once for both parses.
use sonic_rs::Error;

use super::ShredError;
use super::visit::Context;
use crate::json::EXPONENT_DIGITS;
use crate::limits::QUOTED_BYTES;

/// Walks the record `bytes`, one JSON value, into `seed`.
///
/// # Errors
///
/// Where `bytes` is not one JSON value, or the seed refuses it.
pub(super) fn visit<'de, S: DeserializeSeed<'de, Value = ()>>(
    bytes: &'de [u8],
    seed: S,
    context: &Context,
) -> Result<(), Error> {
    let mut jiter = Jiter::new(bytes);
    let peek = jiter.peek().map_err(refused)?;
    seed.deserialize(Walked {
        peek,
        jiter: &mut jiter,
        context,
        depth: 0,
    })?;
    jiter.finish().map_err(refused)
}

fn refused(error: JiterError) -> Error {
    de::Error::custom(error)
}

/// A value whose first byte is `peek`, read from `jiter`, inside `depth` containers.
struct Walked<'r, 'de> {
    peek: Peek,
    jiter: &'r mut Jiter<'de>,
    context: &'r Context,
    depth: u64,
}

impl<'de> Deserializer<'de> for Walked<'_, 'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let Self {
            peek,
            jiter,
            context,
            depth,
        } = self;
        match peek {
            Peek::Null => {
                jiter.known_null().map_err(refused)?;
                visitor.visit_unit()
            }
            Peek::True | Peek::False => visitor.visit_bool(jiter.known_bool(peek).map_err(refused)?),
            Peek::String => visitor.visit_str(jiter.known_str().map_err(refused)?),
            Peek::Object | Peek::Array => {
                if depth >= NESTING_DEPTH {
                    return Err(context.fail(ShredError::TooDeep));
                }
                let depth = depth + 1;
                if peek == Peek::Object {
                    visitor.visit_map(Members {
                        jiter,
                        context,
                        depth,
                        first: true,
                    })
                } else {
                    visitor.visit_seq(Items {
                        jiter,
                        context,
                        depth,
                        first: true,
                    })
                }
            }
            _ => {
                let written = jiter.known_number_bytes(peek).map_err(refused)?;
                let text = std::str::from_utf8(written).map_err(de::Error::custom)?;
                within_exponent(text, context)?;
                super::exact::number(text, context, visitor)
            }
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier
        ignored_any
    }
}

/// Refuses the number written `text` where its exponent has more digits than the limit beside
/// its leading zeros.
fn within_exponent(text: &str, context: &Context) -> Result<(), Error> {
    let Some(at) = text.find(['e', 'E']) else {
        return Ok(());
    };
    let exponent = text[at + 1..].trim_start_matches(['+', '-']).trim_start_matches('0');
    if exponent.len() > EXPONENT_DIGITS {
        let shown = rdlt_connector::text::shown(text, QUOTED_BYTES);
        return Err(context.fail(ShredError::Exponent(shown)));
    }
    Ok(())
}

/// An object's members, read from `jiter` to its end.
struct Members<'r, 'de> {
    jiter: &'r mut Jiter<'de>,
    context: &'r Context,
    depth: u64,
    first: bool,
}

impl<'de> MapAccess<'de> for Members<'_, 'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Error> {
        let key = if std::mem::replace(&mut self.first, false) {
            self.jiter.known_object()
        } else {
            self.jiter.next_key()
        };
        match key.map_err(refused)? {
            None => Ok(None),
            Some(key) => seed.deserialize(StrDeserializer::<Error>::new(key)).map(Some),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        let peek = self.jiter.peek().map_err(refused)?;
        seed.deserialize(Walked {
            peek,
            jiter: &mut *self.jiter,
            context: self.context,
            depth: self.depth,
        })
    }
}

/// An array's items, read from `jiter` to its end.
struct Items<'r, 'de> {
    jiter: &'r mut Jiter<'de>,
    context: &'r Context,
    depth: u64,
    first: bool,
}

impl<'de> SeqAccess<'de> for Items<'_, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        let peek = if std::mem::replace(&mut self.first, false) {
            self.jiter.known_array()
        } else {
            self.jiter.array_step()
        };
        match peek.map_err(refused)? {
            None => Ok(None),
            Some(peek) => seed
                .deserialize(Walked {
                    peek,
                    jiter: &mut *self.jiter,
                    context: self.context,
                    depth: self.depth,
                })
                .map(Some),
        }
    }
}

//! The observing parse: a chunk whose builders would take more than it was admitted for is read
//! for what its values are and how many there are, building nothing; its batch is built once
//! the push's shape is known and what it takes is reserved.
//!
//! Observing keeps a node a column and a count a list, never a cell, so it takes no more than
//! the chunk's columns, which the limit bounds, whatever its rows.

use std::fmt;

use arrow_buffer::i256;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use super::ShredError;
use super::build::Scalar;
use super::meter::{KEY, Meter, OBJECT_SHAPE};
use super::observe::{Observed, Shape};
use super::visit::{Context, MAX_DEPTH, Skip, nest};

#[cfg(test)]
mod tests;

/// One record, which is an object, observed into `shape`.
pub(crate) struct Record<'a> {
    pub(crate) shape: &'a mut Shape,
    pub(crate) context: &'a Context,
}

impl<'de> DeserializeSeed<'de> for Record<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Record<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a record")
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        object(self.shape, map, self.context, 1)
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Err(self.context.fail(ShredError::NotObject))
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Err(self.context.fail(ShredError::NotObject))
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Err(self.context.fail(ShredError::NotObject))
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Err(self.context.fail(ShredError::NotObject))
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Err(self.context.fail(ShredError::NotObject))
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Err(self.context.fail(ShredError::NotObject))
    }

    fn visit_bytes<E: de::Error>(self, _: &[u8]) -> Result<(), E> {
        Err(self.context.fail(ShredError::NotObject))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, _: A) -> Result<(), A::Error> {
        Err(self.context.fail(ShredError::NotObject))
    }
}

/// Observes the object `map`, `depth` levels deep, into `shape`.
fn object<'de, A: MapAccess<'de>>(
    shape: &mut Shape,
    mut map: A,
    context: &Context,
    depth: u64,
) -> Result<(), A::Error> {
    nest(move || {
        while let Some(position) = map.next_key_seed(Key {
            shape: &mut *shape,
            context,
        })? {
            map.next_value_seed(Look {
                node: shape.field_mut(position),
                context,
                depth: depth + 1,
            })?;
        }
        Ok(())
    })
}

/// The position of an object key among the fields observed, a new field counted among the
/// chunk's columns.
struct Key<'a> {
    shape: &'a mut Shape,
    context: &'a Context,
}

impl<'de> DeserializeSeed<'de> for Key<'_> {
    type Value = usize;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<usize, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for Key<'_> {
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_str<E: de::Error>(self, name: &str) -> Result<usize, E> {
        if let Some(position) = self.shape.position(name) {
            return Ok(position);
        }
        self.context
            .columns
            .add()
            .map_err(|error| self.context.fail(error))?;
        self.context.held(Meter::key(name))?;
        self.shape.push(name.into(), Observed::Null);
        Ok(self.shape.fields().len() - 1)
    }
}

/// One value, `depth` levels deep, observed into `node`.
pub(crate) struct Look<'a> {
    pub(crate) node: &'a mut Observed,
    pub(crate) context: &'a Context,
    pub(crate) depth: u64,
}

impl<'a> Look<'a> {
    /// Joins a value observed as `observed` into the node; one that makes it `Json` from a
    /// node holding floats notes them, which building it as JSON text needs written as they were.
    fn join(&mut self, observed: &Observed) {
        let floats = self.node.floats() || observed.floats();
        self.node.join(observed);
        if floats && *self.node == Observed::Json {
            self.context.json_float();
        }
    }

    fn scalar(mut self, value: Scalar<'_>) {
        self.join(&value.observed());
    }

    /// The rest of a value whose node is `Json`: only its nesting is checked.
    fn skip(&self) -> Skip<'a> {
        Skip {
            context: self.context,
            depth: self.depth,
        }
    }
}

impl<'de> DeserializeSeed<'de> for Look<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.depth > MAX_DEPTH {
            return Err(self.context.fail(ShredError::TooDeep));
        }
        if *self.node == Observed::Json {
            return self.skip().deserialize(deserializer);
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Look<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_bool<E>(self, value: bool) -> Result<(), E> {
        self.scalar(Scalar::Bool(value));
        Ok(())
    }

    fn visit_i64<E>(self, value: i64) -> Result<(), E> {
        self.scalar(Scalar::Int(value));
        Ok(())
    }

    fn visit_u64<E>(self, value: u64) -> Result<(), E> {
        self.scalar(i64::try_from(value).map_or(Scalar::Wide(value), Scalar::Int));
        Ok(())
    }

    fn visit_f64<E>(self, value: f64) -> Result<(), E> {
        self.context.number_text();
        self.context.float(value);
        self.scalar(Scalar::Float(value));
        Ok(())
    }

    fn visit_i128<E>(self, value: i128) -> Result<(), E> {
        self.scalar(Scalar::Huge(value));
        Ok(())
    }

    fn visit_bytes<E: de::Error>(self, digits: &[u8]) -> Result<(), E> {
        let digits = std::str::from_utf8(digits).map_err(|error| {
            self.context.fail(ShredError::Internal(format!(
                "an integer's digits: {error}"
            )))
        })?;
        let vast = (digits.trim_start_matches('-').len() <= 76)
            .then(|| i256::from_string(digits))
            .flatten();
        self.scalar(vast.map_or(Scalar::Beyond, Scalar::Vast));
        Ok(())
    }

    fn visit_str<E>(self, value: &str) -> Result<(), E> {
        self.scalar(Scalar::Text(value));
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(mut self, map: A) -> Result<(), A::Error> {
        if *self.node == Observed::Null {
            self.context.held(OBJECT_SHAPE)?;
            *self.node = Observed::Object(Shape::default());
        }
        if let Observed::Object(shape) = self.node {
            return object(shape, map, self.context, self.depth);
        }
        self.join(&Observed::Object(Shape::default()));
        self.skip().visit_map(map)
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        nest(move || {
            if *self.node == Observed::Null {
                self.context
                    .columns
                    .add()
                    .map_err(|error| self.context.fail(error))?;
                self.context.held(KEY)?;
                *self.node = Observed::Array(Box::new(Observed::Null), 0);
            }
            let Observed::Array(item, items) = self.node else {
                self.join(&Observed::Array(Box::new(Observed::Null), 0));
                return self.skip().visit_seq(seq);
            };
            let depth = self.depth + 1;
            while seq
                .next_element_seed(Look {
                    node: &mut *item,
                    context: self.context,
                    depth,
                })?
                .is_some()
            {
                *items = items.saturating_add(1);
            }
            Ok(())
        })
    }
}

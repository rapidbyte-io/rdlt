//! The observing parse: a chunk whose builders would take more than it was admitted for is read
//! for what its values are and how many there are, building nothing; its batch is built once
//! the push's shape is known and what it takes is reserved.
//!
//! Observing keeps a node a column and a count a list, never a cell, so it takes no more than
//! the chunk's columns, which the limit bounds, whatever its rows. A value fitted to a column's
//! type is observed as the type guides, so it keeps no more than a node a column of the type.

use std::fmt;

use rdlt_connector::LogicalType;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use super::ShredError;
use super::build::Scalar;
use super::meter::{KEY, Meter, OBJECT_SHAPE};
use super::observe::{Observed, Shape};
use super::visit::{Context, Fields, Skip, digits, nest, scalar_of_digits, within_depth};
use crate::limits::QUOTED_BYTES;

#[cfg(test)]
mod tests;

/// One record, which is an object, observed into `shape`.
pub(crate) struct Record<'a> {
    pub(crate) shape: &'a mut Shape,
    pub(crate) context: &'a Context,
}

impl Fields for Record<'_> {
    fn context(&self) -> &Context {
        self.context
    }

    fn fields<'de, A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        object(self.shape, map, self.context, 1, None)
    }
}

/// Observes the object `map`, `depth` levels deep, into `shape`; a key it names twice is refused,
/// and so is one that `fields`, the struct the value is fitted to where it is, lacks.
fn object<'de, A: MapAccess<'de>>(
    shape: &mut Shape,
    mut map: A,
    context: &Context,
    depth: u64,
    fields: Option<&rdlt_connector::Fields>,
) -> Result<(), A::Error> {
    nest(move || {
        let object = context.object();
        while let Some((position, target)) = map.next_key_seed(Key {
            shape: &mut *shape,
            context,
            object,
            fields,
        })? {
            map.next_value_seed(Look {
                node: shape.field_mut(position),
                context,
                depth: depth + 1,
                target,
            })?;
        }
        Ok(())
    })
}

/// The position of an object key among the fields observed, a new field counted among the
/// chunk's columns, and the type its value is fitted to; a key the object `object` named before
/// is refused, and so is one `fields` lacks.
struct Key<'a, 'f> {
    shape: &'a mut Shape,
    context: &'a Context,
    object: u64,
    fields: Option<&'f rdlt_connector::Fields>,
}

impl<'de, 'f> DeserializeSeed<'de> for Key<'_, 'f> {
    type Value = (usize, Option<&'f LogicalType>);

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'f> Visitor<'_> for Key<'_, 'f> {
    type Value = (usize, Option<&'f LogicalType>);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_str<E: de::Error>(self, name: &str) -> Result<Self::Value, E> {
        let target = match self.fields {
            Some(fields) => match fields.get(name) {
                Some(field) => Some(field.logical_type()),
                None => return Err(misfit()),
            },
            None => None,
        };
        match self.shape.name(name, self.object) {
            Some((position, false)) => return Ok((position, target)),
            Some((_, true)) => return Err(repeated(self.context, name)),
            None => {}
        }
        self.context
            .columns
            .add()
            .map_err(|error| self.context.fail(error))?;
        self.context.held(Meter::key(name))?;
        Ok((self.shape.push_named(name.into(), self.object), target))
    }
}

/// The error of a key an object names again.
#[cold]
fn repeated<E: de::Error>(context: &Context, name: &str) -> E {
    let key = rdlt_connector::text::shown(name, QUOTED_BYTES);
    context.fail(ShredError::DuplicateKey(key))
}

/// The error that stops observing a value its column's type cannot hold: it fits no column.
#[cold]
fn misfit<E: de::Error>() -> E {
    E::custom("the value is of no type its column holds")
}

/// One value, `depth` levels deep, observed into `node`, as `target`, the type the value is
/// fitted to where it is, guides.
pub(crate) struct Look<'a> {
    pub(crate) node: &'a mut Observed,
    pub(crate) context: &'a Context,
    pub(crate) depth: u64,
    pub(crate) target: Option<&'a LogicalType>,
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
        within_depth(self.depth, self.context)?;
        if *self.node == Observed::Json {
            return self.skip().deserialize(deserializer);
        }
        if let Some(LogicalType::Json) = self.target {
            let skip = self.skip();
            return deserializer.deserialize_any(Part {
                node: self.node,
                skip,
            });
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
        self.context.float(value);
        self.scalar(Scalar::Float(value));
        Ok(())
    }

    fn visit_i128<E>(self, value: i128) -> Result<(), E> {
        self.scalar(Scalar::Huge(value));
        Ok(())
    }

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<(), E> {
        let digits = digits(bytes, self.context)?;
        self.scalar(scalar_of_digits(digits));
        Ok(())
    }

    fn visit_str<E>(self, value: &str) -> Result<(), E> {
        self.scalar(Scalar::Text(value));
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(mut self, map: A) -> Result<(), A::Error> {
        let fields = match self.target {
            None => None,
            Some(LogicalType::Struct(fields)) => Some(fields),
            Some(_) => return Err(misfit()),
        };
        if *self.node == Observed::Null {
            self.context.held(OBJECT_SHAPE)?;
            *self.node = Observed::Object(Shape::default());
        }
        if let Observed::Object(shape) = self.node {
            return object(shape, map, self.context, self.depth, fields);
        }
        self.join(&Observed::Object(Shape::default()));
        self.skip().visit_map(map)
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        let target = match self.target {
            None => None,
            Some(LogicalType::List(item)) => Some(item.logical_type()),
            Some(_) => return Err(misfit()),
        };
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
                    target,
                })?
                .is_some()
            {
                *items = items.saturating_add(1);
            }
            Ok(())
        })
    }
}

/// One value of a part its column holds as JSON: observed as JSON, nothing built below it and
/// only its nesting checked; a null stays null, which a field that may not be null refuses.
struct Part<'a> {
    node: &'a mut Observed,
    skip: Skip<'a>,
}

impl<'a> Part<'a> {
    /// Observes the value, which is not null, as JSON: the rest of it is only checked.
    fn json(self) -> Skip<'a> {
        *self.node = Observed::Json;
        self.skip
    }
}

impl<'de> Visitor<'de> for Part<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<(), E> {
        self.json().visit_bool(value)
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<(), E> {
        self.json().visit_i64(value)
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<(), E> {
        self.json().visit_u64(value)
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<(), E> {
        self.json().visit_f64(value)
    }

    fn visit_i128<E: de::Error>(self, value: i128) -> Result<(), E> {
        self.json().visit_i128(value)
    }

    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<(), E> {
        self.json().visit_bytes(value)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<(), E> {
        self.json().visit_str(value)
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        self.json().visit_map(map)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<(), A::Error> {
        self.json().visit_seq(seq)
    }
}

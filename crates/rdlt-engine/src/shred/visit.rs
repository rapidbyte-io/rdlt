//! The parse itself: serde seeds that walk a chunk's records as sonic-rs parses them, appending
//! each value to its column.

#[cfg(test)]
mod tests;

use std::cell::Cell;
use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::ShredError;
use super::build::{Column, Record, Scalar};
use super::observe::{Observed, Shape};
use super::render::Render;

/// The deepest a value may nest, counting the record itself as depth 1.
pub(crate) const MAX_DEPTH: u64 = rdlt_connector::limits::MAX_NESTING_DEPTH;

/// What a parse carries besides its columns: why it stopped, and whether any column stopped
/// building.
#[derive(Default)]
pub(crate) struct Context {
    fault: Cell<Option<ShredError>>,
    spoiled: Cell<bool>,
}

impl Context {
    /// Stops the parse with `error`.
    pub(crate) fn fail<E: de::Error>(&self, error: ShredError) -> E {
        let message = error.to_string();
        self.fault.set(Some(error));
        E::custom(message)
    }

    /// Why the parse stopped, when it was the shredder that stopped it.
    pub(crate) fn fault(&self) -> Option<ShredError> {
        self.fault.take()
    }

    /// Whether a column stopped building.
    pub(crate) fn spoiled(&self) -> bool {
        self.spoiled.get()
    }
}

/// The position of an object key among a record's fields, trying `hint` first.
struct Field<'a> {
    record: &'a mut Record,
    hint: usize,
    context: &'a Context,
}

impl<'de> DeserializeSeed<'de> for Field<'_> {
    type Value = usize;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<usize, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for Field<'_> {
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_str<E: de::Error>(self, name: &str) -> Result<usize, E> {
        self.record
            .position(name, self.hint)
            .map_err(|error| self.context.fail(error))
    }
}

/// One record, which is an object.
pub(crate) struct Row<'a> {
    pub(crate) record: &'a mut Record,
    pub(crate) context: &'a Context,
}

impl<'de> DeserializeSeed<'de> for Row<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Row<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a record")
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        object(self.record, map, self.context, 1)
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

    fn visit_seq<A: SeqAccess<'de>>(self, _: A) -> Result<(), A::Error> {
        Err(self.context.fail(ShredError::NotObject))
    }
}

/// Appends the object `map`, `depth` levels deep, as one row of `record`.
fn object<'de, A: MapAccess<'de>>(
    record: &mut Record,
    mut map: A,
    context: &Context,
    depth: u64,
) -> Result<(), A::Error> {
    let capacity = record.capacity();
    let mut fields = 0;
    while let Some(position) = map.next_key_seed(Field {
        record: &mut *record,
        hint: fields,
        context,
    })? {
        let column = record
            .field(position)
            .map_err(|error| context.fail(error))?;
        map.next_value_seed(Value {
            column,
            context,
            depth: depth + 1,
            capacity,
        })?;
        fields += 1;
    }
    record.end_row(fields);
    Ok(())
}

/// One value, `depth` levels deep, appended to `column`.
struct Value<'a> {
    column: &'a mut Column,
    context: &'a Context,
    depth: u64,
    capacity: usize,
}

impl<'a> Value<'a> {
    fn scalar(self, value: Scalar<'_>) {
        if !self.column.scalar(value, self.capacity) {
            self.spoil();
        }
    }

    /// Stops the column building, since its values joined to `Json`, and checks the rest of this
    /// value instead.
    fn spoil(self) -> Skip<'a> {
        self.column.spoil();
        self.context.spoiled.set(true);
        Skip {
            context: self.context,
            depth: self.depth,
        }
    }
}

impl<'de> DeserializeSeed<'de> for Value<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.depth > MAX_DEPTH {
            return Err(self.context.fail(ShredError::TooDeep));
        }
        if let Column::Json(builder) = self.column {
            let mut text = String::new();
            let render = Render {
                text: &mut text,
                context: self.context,
            };
            render.deserialize(deserializer)?;
            // Only a JSON null renders as `null`; a string holding it is quoted.
            if text == "null" {
                builder.append_null();
            } else {
                builder.append_value(text);
            }
            return Ok(());
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Value<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        self.column.null();
        Ok(())
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<(), E> {
        self.scalar(Scalar::Bool(value));
        Ok(())
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<(), E> {
        self.scalar(Scalar::Int(value));
        Ok(())
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<(), E> {
        self.scalar(i64::try_from(value).map_or(Scalar::Wide(value), Scalar::Int));
        Ok(())
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<(), E> {
        self.scalar(Scalar::Float(value));
        Ok(())
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<(), E> {
        self.scalar(Scalar::Text(value));
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        if let Column::Null(nulls) = *self.column {
            *self.column = Column::new(&Observed::Object(Shape::default()), nulls, self.capacity);
        }
        match self.column {
            Column::Struct(record) => object(record, map, self.context, self.depth),
            _ => self.spoil().visit_map(map),
        }
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        if let Column::Null(nulls) = *self.column {
            *self.column = Column::new(
                &Observed::Array(Box::new(Observed::Null)),
                nulls,
                self.capacity,
            );
        }
        let Column::List(list) = self.column else {
            return self.spoil().visit_seq(seq);
        };
        let mut items = 0;
        while seq
            .next_element_seed(Value {
                column: list.item(),
                context: self.context,
                depth: self.depth + 1,
                capacity: self.capacity,
            })?
            .is_some()
        {
            items += 1;
        }
        list.end_row(items)
            .map_err(|error| self.context.fail(error))
    }
}

/// One value, `depth` levels deep, of a column that stopped building: only its nesting is
/// checked.
struct Skip<'a> {
    context: &'a Context,
    depth: u64,
}

impl<'de> DeserializeSeed<'de> for Skip<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.depth > MAX_DEPTH {
            return Err(self.context.fail(ShredError::TooDeep));
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Skip<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_bool<E>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while map.next_key::<IgnoredAny>()?.is_some() {
            map.next_value_seed(Skip {
                context: self.context,
                depth: self.depth + 1,
            })?;
        }
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let depth = self.depth + 1;
        while seq
            .next_element_seed(Skip {
                context: self.context,
                depth,
            })?
            .is_some()
        {}
        Ok(())
    }
}

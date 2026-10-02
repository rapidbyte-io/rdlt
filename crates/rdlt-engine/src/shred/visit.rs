//! The parse itself: serde seeds that walk a chunk's records as sonic-rs parses them, appending
//! each value to its column.

mod skip;
#[cfg(test)]
mod tests;

use std::cell::Cell;
use std::fmt;

use arrow_buffer::i256;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use super::ShredError;
use super::build::{Column, List, Record, Scalar};
use super::meter::{Columns, Meter, Over};
use super::observe::{Observed, Shape};
use super::render::Render;
pub(crate) use skip::Skip;

/// The deepest a value may nest, counting the record itself as depth 1.
pub(crate) const MAX_DEPTH: u64 = rdlt_connector::limits::MAX_NESTING_DEPTH;

/// Stack a nesting level may use before [`nest`] grows the stack, 128 KiB: more than any one level
/// of the parse uses, in every build.
const RED_ZONE: usize = 131_072;

/// Stack [`nest`] adds when it grows it, 1 MiB.
const SEGMENT: usize = 1_048_576;

/// Runs `parse`, one nesting level of a value, on more stack when little is left: a value at the
/// nesting limit needs more than a thread's stack in unoptimized builds.
pub(crate) fn nest<T>(parse: impl FnOnce() -> T) -> T {
    stacker::maybe_grow(RED_ZONE, SEGMENT, parse)
}

/// What a parse carries besides its columns: what its builders may take and the columns its
/// records may hold, why it stopped, and what it found that a later parse needs.
pub(crate) struct Context {
    fault: Cell<Option<ShredError>>,
    spoiled: Cell<bool>,
    imprecise: Cell<bool>,
    pub(crate) meter: Meter,
    pub(crate) columns: Columns,
}

/// The smallest magnitude of a float that may be an integer beyond 64 bits the fast parse
/// rounded: 2⁶³, below which every integer parses exactly.
const ROUNDED_FROM: f64 = 9_223_372_036_854_775_808.0;

/// The integers a 38-digit decimal holds are below this in magnitude.
pub(crate) const DECIMAL_LIMIT: u128 = 100_000_000_000_000_000_000_000_000_000_000_000_000;

/// The most digits of an integer a decimal holds.
const VAST_DIGITS: usize = 76;

impl Context {
    /// A parse whose builders take what `meter` lets them, of records holding `columns`.
    pub(crate) fn new(meter: Meter, columns: Columns) -> Self {
        Self {
            fault: Cell::new(None),
            spoiled: Cell::new(false),
            imprecise: Cell::new(false),
            meter,
            columns,
        }
    }

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

    /// Notes that a column stopped building.
    pub(crate) fn spoil(&self) {
        self.spoiled.set(true);
    }

    /// Notes `value`, a float the parse read, which may be an integer beyond 64 bits rounded to
    /// the float nearest it when it is whole and that large.
    pub(crate) fn float(&self, value: f64) {
        if value.abs() >= ROUNDED_FROM && value.fract() == 0.0 {
            self.imprecise.set(true);
        }
    }

    /// Notes that the chunk must be parsed again exactly.
    pub(crate) fn reparse(&self) {
        self.imprecise.set(true);
    }

    /// Whether the parse read a float that may be a rounded integer, or must parse again exactly.
    pub(crate) fn imprecise(&self) -> bool {
        self.imprecise.get()
    }
}

/// The error that stops a parse whose builders would take more than its meter has room for.
fn over<E: de::Error>(_: Over) -> E {
    E::custom("the chunk's builders would take more than it was admitted for")
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
            .position(name, self.hint, &self.context.columns)
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

    /// An integer beyond 38 digits, which the exact parse visits as its digits.
    fn visit_bytes<E: de::Error>(self, _: &[u8]) -> Result<(), E> {
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
    nest(move || {
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
        record.end_row(fields, &context.meter).map_err(over)
    })
}

/// One value, `depth` levels deep, appended to `column`, which a column made for it is sized for
/// `capacity` rows of.
struct Value<'a> {
    column: &'a mut Column,
    context: &'a Context,
    depth: u64,
    capacity: usize,
}

impl<'a> Value<'a> {
    fn scalar<E: de::Error>(self, value: Scalar<'_>) -> Result<(), E> {
        match self
            .column
            .scalar(value, self.capacity, &self.context.meter)
        {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.spoil();
                Ok(())
            }
            Err(refused) => Err(over(refused)),
        }
    }

    /// Stops the column building, since its values joined to `Json`, and checks the rest of this
    /// value instead.
    fn spoil(self) -> Skip<'a> {
        self.column.spoil();
        self.context.spoil();
        Skip {
            context: self.context,
            depth: self.depth,
        }
    }

    /// Makes the column, holding nulls only, one of `observed`, sized as a value of it is.
    fn make<E: de::Error>(&mut self, observed: &Observed) -> Result<(), E> {
        if let Column::Null(nulls) = *self.column {
            if let Observed::Array(..) = observed {
                self.context
                    .columns
                    .add()
                    .map_err(|error| self.context.fail(error))?;
            }
            *self.column =
                Column::new(observed, nulls, self.capacity, &self.context.meter).map_err(over)?;
        }
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for Value<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.depth > MAX_DEPTH {
            return Err(self.context.fail(ShredError::TooDeep));
        }
        if let Column::Json(..) = self.column {
            let mut text = String::new();
            let render = Render {
                text: &mut text,
                context: self.context,
            };
            render.deserialize(deserializer)?;
            let meter = &self.context.meter;
            // Only a JSON null renders as `null`; a string holding it is quoted.
            let appended = if text == "null" {
                self.column.null(meter)
            } else {
                self.column.json(&text, meter)
            };
            return appended.map_err(over);
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Value<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.column.null(&self.context.meter).map_err(over)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<(), E> {
        self.scalar(Scalar::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<(), E> {
        self.scalar(Scalar::Int(value))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<(), E> {
        self.scalar(i64::try_from(value).map_or(Scalar::Wide(value), Scalar::Int))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<(), E> {
        self.context.float(value);
        self.scalar(Scalar::Float(value))
    }

    fn visit_i128<E: de::Error>(self, value: i128) -> Result<(), E> {
        self.scalar(Scalar::Huge(value))
    }

    /// An integer beyond 38 digits, as its digits: the exact parse visits such integers so, since
    /// no other visit holds them and no JSON value visits bytes otherwise.
    fn visit_bytes<E: de::Error>(self, digits: &[u8]) -> Result<(), E> {
        let digits = std::str::from_utf8(digits).map_err(|error| {
            self.context.fail(ShredError::Internal(format!(
                "an integer's digits: {error}"
            )))
        })?;
        let vast = (digits.trim_start_matches('-').len() <= VAST_DIGITS)
            .then(|| i256::from_string(digits))
            .flatten();
        self.scalar(vast.map_or(Scalar::Beyond, Scalar::Vast))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<(), E> {
        self.scalar(Scalar::Text(value))
    }

    fn visit_map<A: MapAccess<'de>>(mut self, map: A) -> Result<(), A::Error> {
        self.make(&Observed::Object(Shape::default()))?;
        match self.column {
            Column::Struct(record) => object(record, map, self.context, self.depth),
            _ => self.spoil().visit_map(map),
        }
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        nest(move || {
            self.make(&Observed::Array(Box::new(Observed::Null), 0))?;
            let Column::List(list) = self.column else {
                return self.spoil().visit_seq(seq);
            };
            let mut items = 0;
            while seq
                .next_element_seed(Item {
                    list: &mut *list,
                    context: self.context,
                    depth: self.depth + 1,
                })?
                .is_some()
            {
                items += 1;
            }
            list.end_row(items)
                .map_err(|error| self.context.fail(error))
        })
    }
}

/// The next item of an array, `depth` levels deep, appended to `list`'s item column.
struct Item<'a> {
    list: &'a mut List,
    context: &'a Context,
    depth: u64,
}

impl<'de> DeserializeSeed<'de> for Item<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        let (column, capacity) = self.list.item(&self.context.meter).map_err(over)?;
        Value {
            column,
            context: self.context,
            depth: self.depth,
            capacity,
        }
        .deserialize(deserializer)
    }
}

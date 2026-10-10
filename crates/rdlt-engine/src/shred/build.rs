//! Arrow columns built as a chunk is parsed, typed by the values as they arrive, every byte a
//! builder takes charged to the chunk's meter before it is taken.
//!
//! A column starts as nulls and takes the type of its first value. A later value of a wider type
//! the column can take without loss (a float after exact integers, a large unsigned integer after
//! integers) converts it. Any other makes the column `Json`, which it cannot build without the
//! values it already took, so the column stops building and the chunk is built again once the
//! push's shape is known.
//!
//! A builder is presized for the rows of its level and charged for them when it is made; a level
//! that holds more grows its builders as they would grow themselves, doubling, and is charged for
//! each growth before it.

mod list;
mod record;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Decimal128Builder, Decimal256Builder, Float64Builder,
    Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, NullArray};
use arrow_buffer::i256;
use rdlt_wire::limits::count;

use super::ShredError;
use super::meter::{BUILDER, Meter, Over, RECORD};
use super::observe::Observed;
pub(crate) use list::List;
pub(crate) use record::Record;

/// A scalar JSON value.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Scalar<'a> {
    Bool(bool),
    Int(i64),
    /// An integer above the signed 64-bit range.
    Wide(u64),
    /// An integer beyond 64 bits, within 38 digits.
    Huge(i128),
    /// An integer beyond 38 digits, within 76.
    Vast(i256),
    /// An integer beyond 76 digits, which only JSON text holds: its column is built again as
    /// JSON, rendering its digits.
    Beyond,
    Float(f64),
    Text(&'a str),
}

impl Scalar<'_> {
    /// What the value is observed as.
    pub(crate) fn observed(self) -> Observed {
        match self {
            Self::Bool(_) => Observed::Bool,
            Self::Int(value) => Observed::integer(value),
            Self::Wide(_) => Observed::Wide,
            Self::Huge(_) => Observed::Huge,
            Self::Vast(_) => Observed::Vast,
            Self::Beyond => Observed::Json,
            Self::Float(_) => Observed::Float,
            Self::Text(_) => Observed::Text,
        }
    }
}

/// A column being built, typed by the values it has held.
pub(crate) enum Column {
    /// Only nulls so far: this many.
    Null(usize),
    Bool(BooleanBuilder),
    Int {
        builder: Int64Builder,
        /// Whether every integer is exact as a 64-bit float.
        exact: bool,
    },
    Wide(Decimal128Builder),
    Huge(Decimal128Builder),
    Vast(Decimal256Builder),
    Float(Float64Builder),
    /// Strings, and the bytes of text charged and written.
    Text(StringBuilder, Room),
    /// JSON text, and the bytes of text charged and written.
    Json(StringBuilder, Room),
    Struct(Box<Record>),
    List(Box<List>),
    /// A column whose values joined to `Json` after it built others: its values are only checked
    /// from then on.
    Spoiled,
}

/// Bytes a text or list column's offset takes for each row.
const OFFSET: u64 = 4;

impl Column {
    /// A column for values observed as `observed`, holding `nulls` nulls, sized for `capacity`
    /// rows and charged to `meter` for them; a list's items are sized for as many as it was
    /// observed to hold.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for the builders.
    pub(crate) fn new(
        observed: &Observed,
        nulls: usize,
        capacity: usize,
        meter: &Meter,
    ) -> Result<Self, Over> {
        let capacity = capacity.max(nulls);
        let text = meter.text_bytes(capacity);
        let rows = count(capacity);
        let (width, bits, more) = match observed {
            Observed::Null => return Ok(Self::Null(nulls)),
            Observed::Bool => (0, 2, BUILDER),
            Observed::Int { .. } | Observed::Float => (8, 1, BUILDER),
            Observed::Wide | Observed::Huge => (16, 1, BUILDER),
            Observed::Vast => (32, 1, BUILDER),
            Observed::Text | Observed::Json => (OFFSET, 1, count(text).saturating_add(BUILDER)),
            Observed::Object(_) => (0, 1, RECORD),
            Observed::Array(..) => (OFFSET, 1, OFFSET + BUILDER),
        };
        meter.charge(rows_of(rows, width, bits).saturating_add(more))?;
        let mut column = match observed {
            Observed::Null => Self::Null(0),
            Observed::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            Observed::Int { exact } => Self::Int {
                builder: Int64Builder::with_capacity(capacity),
                exact: *exact,
            },
            Observed::Wide => Self::Wide(decimals(capacity, 20)),
            Observed::Huge => Self::Huge(decimals(capacity, 38)),
            Observed::Vast => Self::Vast(vast(capacity)),
            Observed::Float => Self::Float(Float64Builder::with_capacity(capacity)),
            Observed::Text => {
                Self::Text(StringBuilder::with_capacity(capacity, text), Room::of(text))
            }
            Observed::Json => {
                Self::Json(StringBuilder::with_capacity(capacity, text), Room::of(text))
            }
            Observed::Object(shape) => Self::Struct(Box::new(Record::new(shape, capacity, meter)?)),
            Observed::Array(item, items) => {
                let items = usize::try_from(*items).unwrap_or(usize::MAX);
                Self::List(Box::new(List::new(item, capacity, items, meter)?))
            }
        };
        for _ in 0..nulls {
            column.null(meter)?;
        }
        Ok(column)
    }

    /// What the column's values are observed as.
    pub(crate) fn observed(&self) -> Observed {
        match self {
            Self::Null(_) => Observed::Null,
            Self::Bool(_) => Observed::Bool,
            Self::Int { exact, .. } => Observed::Int { exact: *exact },
            Self::Wide(_) => Observed::Wide,
            Self::Huge(_) => Observed::Huge,
            Self::Vast(_) => Observed::Vast,
            Self::Float(_) => Observed::Float,
            Self::Text(..) => Observed::Text,
            Self::Struct(record) => Observed::Object(record.shape()),
            Self::List(list) => list.observed(),
            Self::Json(..) | Self::Spoiled => Observed::Json,
        }
    }

    /// Bytes a row takes in the column's own builders beside its bits, what growing it by a row
    /// takes: a struct's fields and a list's items grow by their own.
    fn width(&self) -> u64 {
        match self {
            Self::Int { .. } | Self::Float(_) => 8,
            Self::Wide(_) | Self::Huge(_) => 16,
            Self::Vast(_) => 32,
            Self::Text(..) | Self::Json(..) | Self::List(_) => OFFSET,
            Self::Null(_) | Self::Spoiled | Self::Bool(_) | Self::Struct(_) => 0,
        }
    }

    /// Bits a row takes in the column's own builders: its validity, and a boolean's value.
    fn bits(&self) -> u64 {
        match self {
            Self::Null(_) | Self::Spoiled => 0,
            Self::Bool(_) => 2,
            _ => 1,
        }
    }

    /// Appends a null.
    ///
    /// # Errors
    ///
    /// [`Over`] where a struct's fields must grow and the meter has no room.
    pub(crate) fn null(&mut self, meter: &Meter) -> Result<(), Over> {
        match self {
            Self::Null(rows) => *rows += 1,
            Self::Bool(builder) => builder.append_null(),
            Self::Int { builder, .. } => builder.append_null(),
            Self::Wide(builder) | Self::Huge(builder) => builder.append_null(),
            Self::Vast(builder) => builder.append_null(),
            Self::Float(builder) => builder.append_null(),
            Self::Text(builder, _) | Self::Json(builder, _) => builder.append_null(),
            Self::Struct(record) => record.null(meter)?,
            Self::List(list) => list.null(),
            Self::Spoiled => {}
        }
        Ok(())
    }

    /// Appends `value`, converting the column when it widens it without loss, a column made for
    /// it sized for `capacity` rows; returns whether it fitted.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for a builder the value needs.
    pub(crate) fn scalar(
        &mut self,
        value: Scalar<'_>,
        capacity: usize,
        meter: &Meter,
    ) -> Result<bool, Over> {
        match (&*self, value) {
            (Self::Null(nulls), _) => {
                *self = Self::new(&value.observed(), *nulls, capacity, meter)?;
            }
            (Self::Int { exact: true, .. }, Scalar::Float(_)) => {
                self.widen(&Observed::Float, capacity, meter)?;
            }
            (Self::Int { .. }, Scalar::Wide(_)) => self.widen(&Observed::Wide, capacity, meter)?,
            (Self::Int { .. } | Self::Wide(_), Scalar::Huge(_)) => {
                self.widen(&Observed::Huge, capacity, meter)?;
            }
            (Self::Int { .. } | Self::Wide(_) | Self::Huge(_), Scalar::Vast(_)) => {
                self.widen(&Observed::Vast, capacity, meter)?;
            }
            _ => {}
        }
        self.append(value, meter)
    }

    /// Appends `value` to a column of its type; returns whether it was one.
    fn append(&mut self, value: Scalar<'_>, meter: &Meter) -> Result<bool, Over> {
        match (self, value) {
            (Self::Bool(builder), Scalar::Bool(value)) => builder.append_value(value),
            (Self::Int { builder, exact }, Scalar::Int(value)) => {
                *exact &= Observed::integer(value) == Observed::Int { exact: true };
                builder.append_value(value);
            }
            (Self::Wide(builder) | Self::Huge(builder), Scalar::Int(value)) => {
                builder.append_value(i128::from(value));
            }
            (Self::Wide(builder) | Self::Huge(builder), Scalar::Wide(value)) => {
                builder.append_value(i128::from(value));
            }
            (Self::Huge(builder), Scalar::Huge(value)) => builder.append_value(value),
            (Self::Vast(builder), Scalar::Int(value)) => {
                builder.append_value(i256::from_i128(i128::from(value)));
            }
            (Self::Vast(builder), Scalar::Wide(value)) => {
                builder.append_value(i256::from_i128(i128::from(value)));
            }
            (Self::Vast(builder), Scalar::Huge(value)) => {
                builder.append_value(i256::from_i128(value));
            }
            (Self::Vast(builder), Scalar::Vast(value)) => builder.append_value(value),
            (Self::Float(builder), Scalar::Float(value)) => builder.append_value(value),
            #[expect(
                clippy::cast_precision_loss,
                reason = "the integer is exact as a float"
            )]
            (Self::Float(builder), Scalar::Int(value))
                if Observed::integer(value) == (Observed::Int { exact: true }) =>
            {
                builder.append_value(value as f64);
            }
            (Self::Text(builder, room), Scalar::Text(value)) => {
                write_text(room, value.len(), meter)?;
                builder.append_value(value);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Appends `text`, a value of a column of JSON.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for the text.
    pub(crate) fn json(&mut self, text: &str, meter: &Meter) -> Result<(), Over> {
        if let Self::Json(builder, room) = self {
            write_text(room, text.len(), meter)?;
            builder.append_value(text);
        }
        Ok(())
    }

    /// Stops building: a value joined the column's type to `Json`.
    pub(crate) fn spoil(&mut self) {
        *self = Self::Spoiled;
    }

    /// Converts the integers built so far to the wider `observed`: a float, or a whole decimal of
    /// 20, 38 or 76 digits, in builders sized for `capacity` rows and charged to `meter`.
    fn widen(&mut self, observed: &Observed, capacity: usize, meter: &Meter) -> Result<(), Over> {
        let rows = match self {
            Self::Int { builder, .. } => builder.len(),
            Self::Wide(builder) | Self::Huge(builder) => builder.len(),
            _ => return Ok(()),
        };
        let mut widened = Self::new(observed, 0, capacity.max(rows), meter)?;
        match self {
            Self::Int { builder, .. } => {
                for value in &builder.finish() {
                    widened.append_widened(value.map(i128::from));
                }
            }
            Self::Wide(builder) | Self::Huge(builder) => {
                for value in &builder.finish() {
                    widened.append_widened(value);
                }
            }
            _ => {}
        }
        *self = widened;
        Ok(())
    }

    /// Appends `value`, an integer or a null, to a column a widening just made, sized for it.
    #[expect(
        clippy::cast_precision_loss,
        reason = "the integers widened to floats are exact as floats"
    )]
    fn append_widened(&mut self, value: Option<i128>) {
        match self {
            Self::Float(builder) => builder.append_option(value.map(|value| value as f64)),
            Self::Wide(builder) | Self::Huge(builder) => builder.append_option(value),
            Self::Vast(builder) => builder.append_option(value.map(i256::from_i128)),
            _ => {}
        }
    }

    /// The column built.
    pub(crate) fn finish(self) -> Result<ArrayRef, ShredError> {
        let array: ArrayRef = match self {
            Self::Null(rows) => Arc::new(NullArray::new(rows)),
            Self::Bool(mut builder) => Arc::new(builder.finish()),
            Self::Int { mut builder, .. } => Arc::new(builder.finish()),
            Self::Wide(mut builder) | Self::Huge(mut builder) => Arc::new(builder.finish()),
            Self::Vast(mut builder) => Arc::new(builder.finish()),
            Self::Float(mut builder) => Arc::new(builder.finish()),
            Self::Text(mut builder, _) | Self::Json(mut builder, _) => Arc::new(builder.finish()),
            Self::Struct(record) => Arc::new(record.finish_struct()?),
            Self::List(list) => list.finish()?,
            Self::Spoiled => {
                return Err(ShredError::Internal(
                    "finishing a column never built".to_owned(),
                ));
            }
        };
        Ok(array)
    }
}

/// The bytes of text a builder is charged for and holds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Room {
    capacity: usize,
    written: usize,
}

impl Room {
    /// A builder presized for `capacity` bytes, holding none.
    fn of(capacity: usize) -> Self {
        Self {
            capacity,
            written: 0,
        }
    }
}

/// Writes `bytes` of text into a builder with `room`: past what it was charged for, the builder
/// grows as it would, to twice what it held or to what the text needs, and is charged for what
/// it grows by; the copy it grows from is a builder's spare capacity while it grows.
fn write_text(room: &mut Room, bytes: usize, meter: &Meter) -> Result<(), Over> {
    let needed = room.written.saturating_add(bytes);
    if needed > room.capacity {
        let grown = needed.max(room.capacity.saturating_mul(2));
        meter.charge(count(grown - room.capacity))?;
        room.capacity = grown;
    }
    room.written = needed;
    Ok(())
}

/// Bytes `rows` rows take of `width` bytes and `bits` bits each.
fn rows_of(rows: u64, width: u64, bits: u64) -> u64 {
    rows.saturating_mul(width)
        .saturating_add(rows.saturating_mul(bits).div_ceil(8))
}

/// A builder of whole decimals of `digits` digits, which a 128-bit decimal holds.
fn decimals(capacity: usize, digits: u8) -> Decimal128Builder {
    Decimal128Builder::with_capacity(capacity)
        .with_precision_and_scale(digits, 0)
        .expect("at most 38 digits fit a 128-bit decimal")
}

/// A builder of whole decimals of 76 digits, which a 256-bit decimal holds.
fn vast(capacity: usize) -> Decimal256Builder {
    Decimal256Builder::with_capacity(capacity)
        .with_precision_and_scale(76, 0)
        .expect("76 digits fit a 256-bit decimal")
}

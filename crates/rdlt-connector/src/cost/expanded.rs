//! What a range of an array's rows becomes: decoded out of dictionary and run-end encodings,
//! views and list views by what they name, nested values as their JSON text.
//!
//! Measuring is linear in the rows measured and the items they name:
//!
//! - a stretch of fixed-width values, of strings or bytes by offsets, or of lists and structs of
//!   those, is measured from its widths and offsets, however long it is;
//! - a value that many keys or runs name is measured once and remembered, where measuring it
//!   takes more than a few steps, so rows naming it again cost a lookup each;
//! - every step adds at least a byte to the meter, and the meter stops once it is beyond its
//!   limit, so the work is bounded by the limit too, whatever a list view or a union names twice.
//!
//! What is remembered is an entry of a few words for each value that took more than [`DEAR`]
//! steps, each step a byte charged at least, and no more entries than take an eighth of the
//! limit measured against: the memory measuring takes is in proportion to what it charges, and
//! never to how long a dictionary is or how great a key. A value beyond those is measured each
//! time it is named, within the limit as everything is.

mod named;
mod text;

use std::collections::HashMap;

use std::ops::Range;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_schema::{DataType, Fields};

use self::named::{Named, Place};
use super::widths::{BRACKETS, NULL_TEXT, OFFSET, Scalar, count, key, keys, null_slot, scalar};
use super::{Rendering, Stored};
use crate::types::{self, LogicalType, TypeKind};

/// Steps: a value a key or a run names is remembered once measuring it took more than this many,
/// and measured again each time it is named otherwise.
const DEAR: u64 = 16;

/// Bytes: how many a scan for escapes reads in one step.
const SCANNED: u64 = 64;

/// Bytes: about what remembering one value takes.
const REMEMBERED_BYTES: u64 = 64;

/// A fraction of the limit measured against, its denominator: what the values remembered take
/// together at most, so a measure against a small budget holds little beside it.
const REMEMBERED_SHARE: u64 = 8;

/// Slots: what converting a date, a time or an instant to another holds for each value while it
/// runs.
const TEMPORAL: u64 = 4;

/// Values: how many are remembered whatever the limit, a few kilobytes of them.
const REMEMBERED_LEAST: u64 = 64;

/// The levels a value may nest before measuring stops: more than any batch the limits admit.
const DEPTH: u32 = 256;

/// Adds up what rows expand to, up to a limit.
pub(super) struct Meter {
    rendering: Rendering,
    /// Bytes: what a value is measured against, and what is remembered of it.
    limit: u64,
    /// Bytes: where measuring stops now, the limit at most.
    stop: u64,
    spent: u64,
    /// Whether something measured is beyond the limit, or nests too deep to measure, whatever
    /// was added up so far.
    beyond: bool,
    depth: u32,
    /// How many rows, items and stretches measuring has looked at.
    steps: u64,
    /// What each value that was dear to measure takes, by where it lies.
    named: HashMap<Place, Named>,
    /// Steps: a value that takes more to measure is remembered.
    dear: u64,
    /// How many values are remembered at most.
    room: usize,
    /// Bytes: what each item a list names costs beside itself.
    item: u64,
}

/// Where a value is measured: inside a nested value or not, and how its table stores it.
#[derive(Clone, Copy, Debug)]
pub(super) struct Within<'t> {
    /// Whether the value lies inside a nested value, whose JSON text it is part of.
    pub(super) nested: bool,
    /// Whether the value's table is known: what is measured is then what lowering the value
    /// holds at once, and no longer what the destination's kinds say it may become.
    pub(super) planned: bool,
    /// The type of the table column the value is converted to, where it is to one.
    pub(super) target: Option<&'t LogicalType>,
    /// Whether the destination stores the column as text.
    pub(super) text: bool,
}

impl<'t> Within<'t> {
    /// A column of a batch as `stored` says its table stores it, or as nothing says yet.
    pub(super) fn column(stored: Option<&'t Stored>) -> Self {
        Self {
            nested: false,
            planned: stored.is_some(),
            target: stored.map(|stored| &stored.column),
            text: stored.is_some_and(|stored| stored.text),
        }
    }

    /// Inside a value measured here, converted to `target`.
    fn inside(self, target: Option<&'t LogicalType>) -> Self {
        Self {
            nested: true,
            target,
            text: false,
            ..self
        }
    }

    /// The value as it is decoded, before it is converted or rendered.
    fn decoded(self) -> Self {
        Self {
            target: None,
            text: false,
            ..self
        }
    }

    /// Whether the value is converted to JSON text, as a column or a field of any type joined
    /// with one it shares no other type with is.
    fn json(self) -> bool {
        self.target == Some(&LogicalType::Json)
    }
}

/// A value inside a nested one whose table is not known, or is JSON: its JSON text.
pub(super) const JSON: Within<'static> = Within {
    nested: true,
    planned: false,
    target: None,
    text: false,
};

impl Meter {
    pub(super) fn new(rendering: &Rendering, limit: u64) -> Self {
        let room = (limit / REMEMBERED_SHARE / REMEMBERED_BYTES).max(REMEMBERED_LEAST);
        Self {
            rendering: rendering.clone(),
            limit,
            stop: limit,
            spent: 0,
            beyond: false,
            depth: 0,
            steps: 0,
            named: HashMap::new(),
            dear: DEAR,
            room: usize::try_from(room).unwrap_or(usize::MAX),
            item: 0,
        }
    }

    /// The meter, each item a list names costing `bytes` beside itself.
    pub(super) fn with_items(mut self, bytes: u64) -> Self {
        self.item = bytes;
        self
    }

    /// Bytes: what the meter measures against.
    pub(super) fn limit(&self) -> u64 {
        self.limit
    }

    /// A meter that remembers no value, for tests of what remembering changes.
    #[cfg(test)]
    pub(super) fn forgetful(mut self) -> Self {
        self.dear = u64::MAX;
        self
    }

    /// Starts measuring anew, no further than `stop` bytes, keeping what was learned of the
    /// values keys and runs name: the next rows measured must be of the same arrays.
    pub(super) fn restart(&mut self, stop: u64) {
        (self.spent, self.beyond, self.stop) = (0, false, stop.min(self.limit));
    }

    /// What was measured: beyond where measuring stops, some value beyond it.
    pub(super) fn spent(&self) -> u64 {
        if self.beyond {
            return self.spent.max(self.stop.saturating_add(1));
        }
        self.spent
    }

    /// Whether measuring went beyond where it stops, and so stopped.
    pub(super) fn over(&self) -> bool {
        self.beyond || self.spent > self.stop
    }

    /// Adds what `rows` rows cost beside their columns, `each` bytes a row.
    pub(super) fn rows(&mut self, rows: usize, each: u64) {
        self.times(rows, each);
    }

    /// How many rows, items and stretches measuring has looked at.
    #[cfg(test)]
    pub(super) fn steps(&self) -> u64 {
        self.steps
    }

    /// How many values measuring remembers.
    #[cfg(test)]
    pub(super) fn remembered(&self) -> usize {
        self.named.len()
    }

    fn step(&mut self) {
        self.steps = self.steps.saturating_add(1);
    }

    fn add(&mut self, bytes: u64) {
        self.spent = self.spent.saturating_add(bytes);
    }

    fn times(&mut self, rows: usize, each: u64) {
        self.add(count(rows).saturating_mul(each));
    }

    /// Measures `rows` of `array`, a column of a batch, stored as `stored` says where its table
    /// is known.
    pub(super) fn column(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        stored: Option<&Stored>,
    ) {
        self.range(array, clamp(rows, array.len()), Within::column(stored));
    }

    /// Measures every value of `array` as its JSON text.
    pub(super) fn inside(&mut self, array: &dyn Array) {
        self.range(array, 0..array.len(), JSON);
    }

    /// Measures `rows` of `array`, which lies `within` a nested value or a column.
    fn range(&mut self, array: &dyn Array, rows: Range<usize>, within: Within<'_>) {
        if rows.is_empty() || self.over() {
            return;
        }
        if within.json() {
            // A column of JSON holds each value decoded, and its JSON text beside.
            self.range(array, rows.clone(), within.decoded());
            return self.range(array, rows, JSON);
        }
        if within.planned && within.text && !within.nested && array.data_type().is_nested() {
            // A nested column stored as text holds its values converted, and their JSON text
            // beside.
            let converted = Within {
                text: false,
                ..within
            };
            self.range(array, rows.clone(), converted);
            return self.range(array, rows, JSON);
        }
        self.step();
        if self.depth >= DEPTH {
            self.beyond = true;
            return;
        }
        self.depth += 1;
        if array.nulls().is_some() {
            // Validity: a bit a row.
            self.add(count(rows.len()).div_ceil(8));
        }
        self.typed(array, rows, within);
        self.depth -= 1;
    }

    fn typed(&mut self, array: &dyn Array, rows: Range<usize>, within: Within<'_>) {
        let data_type = array.data_type();
        if let Some(own) = scalar(data_type) {
            let each = self.fixed(data_type, own, within);
            return self.times(rows.len(), each);
        }
        match data_type {
            DataType::Utf8 => self.strings::<i32>(array, rows, within),
            DataType::LargeUtf8 => self.strings::<i64>(array, rows, within),
            DataType::Binary => self.bytes::<i32>(array, rows, within),
            DataType::LargeBinary => self.bytes::<i64>(array, rows, within),
            DataType::Utf8View => self.string_views(array, rows, within),
            DataType::BinaryView => self.binary_views(array, rows, within),
            DataType::List(_) => self.list::<i32>(array, rows, within),
            DataType::LargeList(_) => self.list::<i64>(array, rows, within),
            DataType::Map(..) => {
                let map = array.as_map();
                self.listed(map.value_offsets(), map.entries(), rows, within);
            }
            DataType::ListView(_) => self.list_view::<i32>(array, rows, within),
            DataType::LargeListView(_) => self.list_view::<i64>(array, rows, within),
            DataType::FixedSizeList(_, size) => {
                let list = array.as_fixed_size_list();
                let size = usize::try_from(*size).unwrap_or(0);
                let first = usize::try_from(list.value_offset(rows.start)).unwrap_or(0);
                let items = rows.len().saturating_mul(size);
                self.times(rows.len(), OFFSET + BRACKETS);
                self.add(count(items).saturating_mul(1 + self.item));
                let items = clamp(first..first.saturating_add(items), list.values().len());
                self.range(list.values().as_ref(), items, within.inside(item(within)));
            }
            DataType::Struct(fields) => self.structs(array, fields, rows, within),
            DataType::Union(..) => self.union(array, rows),
            DataType::Dictionary(key, _) => match key.as_ref() {
                DataType::Int8 => self.keyed::<Int8Type>(array, rows, within),
                DataType::Int16 => self.keyed::<Int16Type>(array, rows, within),
                DataType::Int32 => self.keyed::<Int32Type>(array, rows, within),
                DataType::Int64 => self.keyed::<Int64Type>(array, rows, within),
                DataType::UInt8 => self.keyed::<UInt8Type>(array, rows, within),
                DataType::UInt16 => self.keyed::<UInt16Type>(array, rows, within),
                DataType::UInt32 => self.keyed::<UInt32Type>(array, rows, within),
                _ => self.keyed::<UInt64Type>(array, rows, within),
            },
            DataType::RunEndEncoded(ends, _) => match ends.data_type() {
                DataType::Int16 => self.runs::<Int16Type>(array, rows, within),
                DataType::Int32 => self.runs::<Int32Type>(array, rows, within),
                _ => self.runs::<Int64Type>(array, rows, within),
            },
            // Every other type is a scalar, measured above.
            _ => self.times(rows.len(), 1),
        }
    }

    /// The bytes one value of `data_type`, whose values all take what `own` says, becomes.
    ///
    /// Where its table is not known yet, the larger of its slot and of its text where the
    /// destination stores its kind as text. Where it is, what lowering holds at once: the value
    /// decoded, the value converted to its column's type where that differs, and its text where
    /// the column is stored as text or the value lies in a nested one.
    fn fixed(&self, data_type: &DataType, own: Scalar, within: Within<'_>) -> u64 {
        if !within.planned {
            let renders = |kind: &TypeKind| self.rendering.renders(*kind);
            let rendered = within.nested || own.kinds.iter().any(renders);
            let text = if rendered { own.text + OFFSET } else { 0 };
            return own.slot.max(text).max(1);
        }
        let stored = within.target.map(LogicalType::to_arrow);
        let to = stored.as_ref().and_then(scalar);
        let converted = match (&stored, to) {
            // A conversion between dates, times and instants holds each value as an optional
            // 64-bit integer, twice, beside its input in the unit asked for and its result.
            (Some(stored), Some(to)) if stored != data_type && stored.is_temporal() => {
                TEMPORAL * to.slot
            }
            (Some(stored), Some(to)) if stored != data_type => to.slot,
            // Bytes of a fixed width copied into bytes by offsets.
            (Some(DataType::Binary | DataType::Utf8), None) => own.slot,
            _ => 0,
        };
        let text = if within.text || within.nested {
            to.map_or(own.text, |to| to.text.max(own.text)) + OFFSET
        } else {
            0
        };
        own.slot.max(1) + converted + text
    }

    /// Measures `rows` of structs of `fields`: each row its fields' names in its JSON text, each
    /// field its column, and each field of the table's column the structs lack a null a row,
    /// stored and named in the row's text.
    fn structs(
        &mut self,
        array: &dyn Array,
        fields: &Fields,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        self.times(rows.len(), keys(fields));
        let stored = match within.target {
            Some(LogicalType::Struct(stored)) => Some(stored),
            _ => None,
        };
        for (field, column) in fields.iter().zip(array.as_struct().columns()) {
            let target = stored.and_then(|stored| stored.get(field.name()));
            let inside = within.inside(target.map(types::Field::logical_type));
            self.range(column.as_ref(), clamp(rows.clone(), column.len()), inside);
        }
        let absent = stored.into_iter().flat_map(types::Fields::iter);
        let absent = absent.filter(|stored| fields.find(stored.name()).is_none());
        let nulls: u64 = absent
            .map(|stored| {
                let slot = null_slot(&stored.logical_type().to_arrow());
                slot.saturating_add(key(stored.name()) + NULL_TEXT)
            })
            .fold(0, u64::saturating_add);
        self.times(rows.len(), nulls);
    }
}

/// The type the items of a list stored as `within` says are converted to.
pub(super) fn item(within: Within<'_>) -> Option<&LogicalType> {
    match within.target {
        Some(LogicalType::List(item)) => Some(item.logical_type()),
        _ => None,
    }
}

/// `rows` within an array of `len` rows.
fn clamp(rows: Range<usize>, len: usize) -> Range<usize> {
    rows.start.min(len)..rows.end.min(len)
}

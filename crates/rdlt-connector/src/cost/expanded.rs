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
//! steps, each step a byte charged at least: the memory measuring takes is in proportion to
//! what it charges, and never to how long a dictionary is or how great a key.

mod named;

use std::collections::HashMap;

use std::ops::Range;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

use self::named::{Named, Place};
use super::Rendering;
use super::widths::{BRACKETS, OFFSET, VIEW, count, escapes, keys, scalar};

/// Steps: a value a key or a run names is remembered once measuring it took more than this many,
/// and measured again each time it is named otherwise.
const DEAR: u64 = 16;

/// Bytes: how many a scan for escapes reads in one step.
const SCANNED: u64 = 64;

/// The levels a value may nest before measuring stops: more than any batch the limits admit.
const DEPTH: u32 = 256;

/// Adds up what rows expand to, up to a limit.
pub(super) struct Meter<'r> {
    rendering: &'r Rendering,
    limit: u64,
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
}

/// How a string or bytes value is rendered.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Text {
    /// As it is.
    Plain,
    /// As a JSON string: quoted, its quotes and control characters escaped.
    Escaped,
    /// As hex, quoted.
    Hex,
}

impl<'r> Meter<'r> {
    pub(super) fn new(rendering: &'r Rendering, limit: u64) -> Self {
        Self {
            rendering,
            limit,
            spent: 0,
            beyond: false,
            depth: 0,
            steps: 0,
            named: HashMap::new(),
            dear: DEAR,
        }
    }

    /// A meter that remembers no value, for tests of what remembering changes.
    #[cfg(test)]
    pub(super) fn forgetful(mut self) -> Self {
        self.dear = u64::MAX;
        self
    }

    /// Starts measuring anew, keeping what was learned of the values keys and runs name: the
    /// next rows measured must be of the same arrays.
    pub(super) fn restart(&mut self) {
        (self.spent, self.beyond) = (0, false);
    }

    /// What was measured: beyond the limit, some value beyond it.
    pub(super) fn spent(&self) -> u64 {
        if self.beyond {
            return self.spent.max(self.limit.saturating_add(1));
        }
        self.spent
    }

    /// Whether measuring went beyond the limit, and so stopped.
    pub(super) fn over(&self) -> bool {
        self.beyond || self.spent > self.limit
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

    /// Measures `rows` of `array`, a column of a batch.
    pub(super) fn column(&mut self, array: &dyn Array, rows: Range<usize>) {
        self.range(array, clamp(rows, array.len()), false);
    }

    /// Measures `rows` of `array`, which lies inside a nested value where `nested`.
    fn range(&mut self, array: &dyn Array, rows: Range<usize>, nested: bool) {
        if rows.is_empty() || self.over() {
            return;
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
        self.typed(array, rows, nested);
        self.depth -= 1;
    }

    fn typed(&mut self, array: &dyn Array, rows: Range<usize>, nested: bool) {
        let data_type = array.data_type();
        if let Some(scalar) = scalar(data_type) {
            let rendered = nested
                || scalar
                    .kinds
                    .iter()
                    .any(|kind| self.rendering.renders(*kind));
            let text = if rendered { scalar.text + OFFSET } else { 0 };
            return self.times(rows.len(), scalar.slot.max(text).max(1));
        }
        match data_type {
            DataType::Utf8 => self.strings::<i32>(array, rows, nested),
            DataType::LargeUtf8 => self.strings::<i64>(array, rows, nested),
            DataType::Binary => self.bytes::<i32>(array, rows, nested),
            DataType::LargeBinary => self.bytes::<i64>(array, rows, nested),
            DataType::Utf8View => self.string_views(array, rows, nested),
            DataType::BinaryView => self.binary_views(array, rows, nested),
            DataType::List(_) => self.list::<i32>(array, rows),
            DataType::LargeList(_) => self.list::<i64>(array, rows),
            DataType::Map(..) => {
                let map = array.as_map();
                self.listed(map.value_offsets(), map.entries(), rows);
            }
            DataType::ListView(_) => self.list_view::<i32>(array, rows),
            DataType::LargeListView(_) => self.list_view::<i64>(array, rows),
            DataType::FixedSizeList(_, size) => {
                let list = array.as_fixed_size_list();
                let size = usize::try_from(*size).unwrap_or(0);
                let first = usize::try_from(list.value_offset(rows.start)).unwrap_or(0);
                let items = rows.len().saturating_mul(size);
                self.times(rows.len(), OFFSET + BRACKETS);
                self.add(count(items));
                let items = clamp(first..first.saturating_add(items), list.values().len());
                self.range(list.values().as_ref(), items, true);
            }
            DataType::Struct(fields) => {
                self.times(rows.len(), keys(fields));
                for column in array.as_struct().columns() {
                    self.range(column.as_ref(), clamp(rows.clone(), column.len()), true);
                }
            }
            DataType::Union(..) => self.union(array, rows),
            DataType::Dictionary(key, _) => match key.as_ref() {
                DataType::Int8 => self.keyed::<Int8Type>(array, rows, nested),
                DataType::Int16 => self.keyed::<Int16Type>(array, rows, nested),
                DataType::Int32 => self.keyed::<Int32Type>(array, rows, nested),
                DataType::Int64 => self.keyed::<Int64Type>(array, rows, nested),
                DataType::UInt8 => self.keyed::<UInt8Type>(array, rows, nested),
                DataType::UInt16 => self.keyed::<UInt16Type>(array, rows, nested),
                DataType::UInt32 => self.keyed::<UInt32Type>(array, rows, nested),
                _ => self.keyed::<UInt64Type>(array, rows, nested),
            },
            DataType::RunEndEncoded(ends, _) => match ends.data_type() {
                DataType::Int16 => self.runs::<Int16Type>(array, rows, nested),
                DataType::Int32 => self.runs::<Int32Type>(array, rows, nested),
                _ => self.runs::<Int64Type>(array, rows, nested),
            },
            // Every other type is a scalar, measured above.
            _ => self.times(rows.len(), 1),
        }
    }

    /// Whether a value of `kind` here is rendered as text.
    fn renders(&self, nested: bool, kind: crate::types::TypeKind) -> bool {
        nested || self.rendering.renders(kind)
    }

    fn strings<O: OffsetSizeTrait>(&mut self, array: &dyn Array, rows: Range<usize>, nested: bool) {
        let strings = array.as_string::<O>();
        // A string outside a nested value is stored as it is, whatever stores it.
        let text = if nested { Text::Escaped } else { Text::Plain };
        self.spanned(strings.value_offsets(), strings.value_data(), rows, text);
    }

    fn bytes<O: OffsetSizeTrait>(&mut self, array: &dyn Array, rows: Range<usize>, nested: bool) {
        let bytes = array.as_binary::<O>();
        let text = if self.renders(nested, crate::types::TypeKind::Binary) {
            Text::Hex
        } else {
            Text::Plain
        };
        self.spanned(bytes.value_offsets(), bytes.value_data(), rows, text);
    }

    /// Measures the `rows` values lying in `data` between consecutive `offsets`.
    fn spanned<O: ArrowNativeType>(
        &mut self,
        offsets: &[O],
        data: &[u8],
        rows: Range<usize>,
        text: Text,
    ) {
        let offset = |row: usize| offsets.get(row).map_or(0, |offset| offset.as_usize());
        let (start, end) = (offset(rows.start), offset(rows.end));
        let bytes = count(end.saturating_sub(start));
        self.times(rows.len() + 1, OFFSET);
        self.add(bytes);
        match text {
            Text::Plain => {}
            Text::Hex => {
                self.add(bytes);
                self.times(rows.len(), BRACKETS);
            }
            Text::Escaped => {
                self.times(rows.len(), BRACKETS);
                if !self.over() {
                    self.steps = self.steps.saturating_add(bytes / SCANNED);
                    self.add(escapes(data.get(start..end).unwrap_or_default()));
                }
            }
        }
    }

    fn string_views(&mut self, array: &dyn Array, rows: Range<usize>, nested: bool) {
        let strings = array.as_string_view();
        self.times(rows.len(), VIEW + OFFSET);
        for row in rows {
            if self.over() {
                return;
            }
            self.step();
            let length = u64::from(length(strings.views()[row]));
            self.add(length);
            if nested {
                self.add(BRACKETS);
                if !self.over() {
                    self.steps = self.steps.saturating_add(length / SCANNED);
                    self.add(escapes(strings.value(row).as_bytes()));
                }
            }
        }
    }

    fn binary_views(&mut self, array: &dyn Array, rows: Range<usize>, nested: bool) {
        let hex = self.renders(nested, crate::types::TypeKind::Binary);
        let views = array.as_binary_view().views();
        self.times(rows.len(), VIEW + OFFSET);
        for row in rows {
            if self.over() {
                return;
            }
            self.step();
            let length = u64::from(length(views[row]));
            self.add(if hex { 2 * length + BRACKETS } else { length });
        }
    }
}

/// The bytes the value a view names is long.
fn length(view: u128) -> u32 {
    u32::try_from(view & u128::from(u32::MAX)).unwrap_or(u32::MAX)
}

/// `rows` within an array of `len` rows.
fn clamp(rows: Range<usize>, len: usize) -> Range<usize> {
    rows.start.min(len)..rows.end.min(len)
}

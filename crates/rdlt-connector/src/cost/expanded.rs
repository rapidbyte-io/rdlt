//! What a range of an array's rows becomes: decoded out of dictionary and run-end encodings,
//! views and list views by what they name, nested values as their JSON text.
//!
//! Every step of work adds at least a byte to the meter, and the meter stops once it is beyond
//! its limit: the work is bounded by the limit however an encoding multiplies its values.

use std::ops::Range;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

use super::Rendering;
use super::widths::{BRACKETS, OFFSET, VIEW, count, escapes, keys, null_slot, scalar};

/// Bytes: the index of a row's run, which decoding a run-end encoding takes a row.
const RUN: u64 = 4;

/// The levels a value may nest before measuring stops: more than any batch the limits admit.
const DEPTH: u32 = 256;

/// Adds up what rows expand to, up to a limit.
pub(super) struct Meter<'r> {
    rendering: &'r Rendering,
    limit: u64,
    spent: u64,
    depth: u32,
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
            depth: 0,
        }
    }

    /// What was measured: beyond the limit, some value beyond it.
    pub(super) fn spent(&self) -> u64 {
        self.spent
    }

    /// Whether measuring went beyond the limit, and so stopped.
    pub(super) fn over(&self) -> bool {
        self.spent > self.limit
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
        if self.depth >= DEPTH {
            self.spent = u64::MAX;
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
            let length = u64::from(length(strings.views()[row]));
            self.add(length);
            if nested {
                self.add(BRACKETS);
                if !self.over() {
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
            let length = u64::from(length(views[row]));
            self.add(if hex { 2 * length + BRACKETS } else { length });
        }
    }

    fn list<O: OffsetSizeTrait>(&mut self, array: &dyn Array, rows: Range<usize>) {
        let list = array.as_list::<O>();
        self.listed(list.value_offsets(), list.values().as_ref(), rows);
    }

    /// Measures the `rows` lists whose items are `values` between consecutive `offsets`: each an
    /// offset and its brackets, each item its comma.
    fn listed<O: ArrowNativeType>(
        &mut self,
        offsets: &[O],
        values: &dyn Array,
        rows: Range<usize>,
    ) {
        let offset = |row: usize| offsets.get(row).map_or(0, |offset| offset.as_usize());
        let items = clamp(offset(rows.start)..offset(rows.end), values.len());
        self.times(rows.len() + 1, OFFSET);
        self.times(rows.len(), BRACKETS);
        self.add(count(items.len()));
        self.range(values, items, true);
    }

    fn list_view<O: OffsetSizeTrait>(&mut self, array: &dyn Array, rows: Range<usize>) {
        let list = array.as_list_view::<O>();
        let values = list.values().as_ref();
        self.times(rows.len(), 2 * OFFSET + BRACKETS);
        for row in rows {
            if self.over() {
                return;
            }
            let first = list.offsets()[row].as_usize();
            let items = clamp(
                first..first.saturating_add(list.sizes()[row].as_usize()),
                values.len(),
            );
            self.add(count(items.len()));
            self.range(values, items, true);
        }
    }

    fn union(&mut self, array: &dyn Array, rows: Range<usize>) {
        let union = array.as_union();
        let DataType::Union(fields, _) = array.data_type() else {
            return;
        };
        // A type id and an offset a row.
        self.times(rows.len(), 1 + 4);
        for row in rows {
            if self.over() {
                return;
            }
            let id = union.type_id(row);
            if fields.iter().any(|(member, _)| member == id) {
                let child = union.child(id).as_ref();
                let at = union.value_offset(row);
                self.range(child, clamp(at..at.saturating_add(1), child.len()), true);
            }
        }
    }

    /// Measures `rows` of a dictionary: each the value its key names, and a null key, or one
    /// naming no value, a null slot of the values' type, which decoding gives every row.
    fn keyed<K: ArrowDictionaryKeyType>(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        nested: bool,
    ) {
        let dictionary = array.as_dictionary::<K>();
        let (keys, values) = (dictionary.keys(), dictionary.values().as_ref());
        self.times(rows.len(), count(size_of::<K::Native>()));
        if scalar(values.data_type()).is_some() {
            // Every value takes the same, a null too, whatever its key.
            return self.typed(values, 0..rows.len(), nested);
        }
        if matches!(values.data_type(), DataType::RunEndEncoded(..)) {
            // Decoding names each key's run through a wider copy of the key.
            self.times(rows.len(), OFFSET);
        }
        if keys.nulls().is_some() {
            // Decoding may place each row among the values its keys name, four bytes a row.
            self.times(rows.len(), RUN);
        }
        let null = null_slot(values.data_type());
        for row in rows {
            if self.over() {
                return;
            }
            let key = keys
                .is_valid(row)
                .then(|| keys.values()[row].as_usize())
                .filter(|key| *key < values.len());
            match key {
                Some(key) => self.range(values, key..key + 1, nested),
                None => self.add(null),
            }
        }
    }

    /// Measures `rows` of a run-end encoded array: each run its value, once a row it spans.
    fn runs<R: RunEndIndexType>(&mut self, array: &dyn Array, rows: Range<usize>, nested: bool) {
        let runs = array.as_run::<R>();
        let (ends, values) = (runs.run_ends(), runs.values().as_ref());
        // Decoding names each row's run in four bytes.
        self.times(rows.len(), RUN);
        let (mut at, last) = (
            ends.offset().saturating_add(rows.start),
            ends.offset().saturating_add(rows.end),
        );
        let mut run = ends.get_physical_index(rows.start);
        while at < last && !self.over() {
            let Some(end) = ends.values().get(run) else {
                return;
            };
            let end = end.as_usize().min(last);
            let spanned = end.saturating_sub(at);
            let before = self.spent;
            self.add(count(size_of::<R::Native>()));
            if run < values.len() {
                self.range(values, run..run + 1, nested);
            } else {
                self.add(null_slot(values.data_type()));
            }
            let each = self.spent.saturating_sub(before);
            self.add(each.saturating_mul(count(spanned.saturating_sub(1))));
            if spanned == 0 {
                return;
            }
            (at, run) = (end, run + 1);
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

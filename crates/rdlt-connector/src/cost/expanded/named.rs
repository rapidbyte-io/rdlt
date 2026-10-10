//! What the rows of the layouts that name other items become: lists, list views, unions, and
//! the dictionary keys and runs whose values are measured once however many rows name them.

use std::ops::Range;

use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowDictionaryKeyType, RunEndIndexType};
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;
use rdlt_wire::limits::count;

use super::{JSON, Meter, Within, clamp, item};
use crate::cost::widths::{BRACKETS, OFFSET, null_slot, scalar};

/// Bytes: the index of a row's run, which decoding a run-end encoding takes a row.
const RUN: u64 = 4;

/// Where a value a key or a run names lies: the array holding it, its place there, and whether
/// it is measured inside a nested value.
///
/// An array is told from another by where it lies in memory, which holds while the batch
/// measured is borrowed: measuring builds no array of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct Place {
    values: usize,
    index: usize,
    nested: bool,
    planned: bool,
    /// Where the type the value is converted to lies, or nowhere.
    target: usize,
    text: bool,
}

/// What a value takes, as it was measured against the meter's limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Named {
    /// Bytes: what the value becomes, within the limit.
    Bytes(u64),
    /// The value alone is beyond the limit, or nests too deep to measure.
    Beyond,
}

/// Whether one value of `data_type` is measured from its offsets or its view alone, in a step:
/// strings and bytes, but for a string held as a JSON string, whose escapes are read.
fn direct(data_type: &DataType, within: Within<'_>) -> bool {
    match data_type {
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => true,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => !within.nested,
        _ => false,
    }
}

impl Meter {
    pub(super) fn list<O: OffsetSizeTrait>(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        let list = array.as_list::<O>();
        self.listed(list.value_offsets(), list.values().as_ref(), rows, within);
    }

    /// Measures the `rows` lists whose items are `values` between consecutive `offsets`: each an
    /// offset and its brackets, each item its comma.
    pub(super) fn listed<O: ArrowNativeType>(
        &mut self,
        offsets: &[O],
        values: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        let offset = |row: usize| offsets.get(row).map_or(0, |offset| offset.as_usize());
        let items = clamp(offset(rows.start)..offset(rows.end), values.len());
        self.times(rows.len() + 1, OFFSET);
        self.times(rows.len(), BRACKETS);
        self.add(count(items.len()));
        self.times(items.len(), self.item);
        self.range(values, items, within.inside(item(within)));
    }

    pub(super) fn list_view<O: OffsetSizeTrait>(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        let list = array.as_list_view::<O>();
        let values = list.values().as_ref();
        self.times(rows.len(), 2 * OFFSET + BRACKETS);
        for row in rows {
            if self.over() {
                return;
            }
            self.step();
            let first = list.offsets()[row].as_usize();
            let items = clamp(
                first..first.saturating_add(list.sizes()[row].as_usize()),
                values.len(),
            );
            // Each item its comma, and the place it is taken from.
            self.times(items.len(), 1 + OFFSET + self.item);
            self.range(values, items, within.inside(item(within)));
        }
    }

    pub(super) fn union(&mut self, array: &dyn Array, rows: Range<usize>) {
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
            self.step();
            let id = union.type_id(row);
            if fields.iter().any(|(member, _)| member == id) {
                let child = union.child(id).as_ref();
                let at = union.value_offset(row);
                self.range(child, clamp(at..at.saturating_add(1), child.len()), JSON);
            }
        }
    }

    /// Measures item `index` of `values`, which a key or a run names.
    ///
    /// A value that takes more than a few steps to measure is measured once, apart from what
    /// was added up so far and against the whole limit, and remembered: within the limit its
    /// bytes, exactly, and beyond it only that it is beyond. Rows naming it again add what was
    /// remembered.
    fn value(&mut self, values: &dyn Array, index: usize, within: Within<'_>) {
        if direct(values.data_type(), within) {
            return self.range(values, index..index + 1, within);
        }
        let place = Place {
            values: std::ptr::from_ref(values).cast::<()>().addr(),
            index,
            nested: within.nested,
            planned: within.planned,
            target: within
                .target
                .map_or(0, |target| std::ptr::from_ref(target).addr()),
            text: within.text,
        };
        self.step();
        match self.named.get(&place) {
            Some(Named::Bytes(bytes)) => return self.add(*bytes),
            Some(Named::Beyond) => return self.beyond = true,
            None => {}
        }
        let (before, steps) = (std::mem::take(&mut self.spent), self.steps);
        let stop = std::mem::replace(&mut self.stop, self.limit);
        self.range(values, index..index + 1, within);
        let named = if self.over() {
            self.beyond = true;
            Named::Beyond
        } else {
            Named::Bytes(self.spent)
        };
        self.stop = stop;
        self.spent = self.spent.saturating_add(before);
        let dear = self.steps.saturating_sub(steps) > self.dear;
        if dear && self.named.len() < self.room {
            self.named.insert(place, named);
        }
    }

    /// Measures `rows` of a dictionary: each the value its key names, and a null key, or one
    /// naming no value, a null slot of the values' type, which decoding gives every row.
    pub(super) fn keyed<K: ArrowDictionaryKeyType>(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        let dictionary = array.as_dictionary::<K>();
        let (keys, values) = (dictionary.keys(), dictionary.values().as_ref());
        self.times(rows.len(), count(size_of::<K::Native>()));
        if scalar(values.data_type()).is_some() {
            // Every value takes the same, a null too, whatever its key.
            return self.typed(values, 0..rows.len(), within);
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
            self.step();
            let key = keys
                .is_valid(row)
                .then(|| keys.values()[row].as_usize())
                .filter(|key| *key < values.len());
            match key {
                Some(key) => self.value(values, key, within),
                None => self.add(null),
            }
        }
    }

    /// Measures `rows` of a run-end encoded array: each run its value, once a row it spans.
    pub(super) fn runs<R: RunEndIndexType>(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
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
            self.step();
            let Some(end) = ends.values().get(run) else {
                return;
            };
            let end = end.as_usize().min(last);
            let spanned = end.saturating_sub(at);
            let before = self.spent;
            self.add(count(size_of::<R::Native>()));
            if run < values.len() {
                self.value(values, run, within);
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

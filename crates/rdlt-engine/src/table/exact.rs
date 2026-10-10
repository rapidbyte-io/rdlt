//! Which columns of 64-bit integers hold a value a 64-bit float would round: a table's column of
//! integers every one of which a float holds exactly takes floats without loss.
//!
//! A column is read where it lies, in any encoding: a dictionary's values through the keys its
//! rows hold, a run's through the runs they fall in. Only values rows hold are read, and nothing
//! is decoded or held a row.

use std::collections::BTreeSet;
use std::ops::Range;
use std::rc::Rc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;
use rdlt_connector::{ColumnPath, LogicalType, TableSchema};

use crate::named::{Ends, Rows};

/// Integers whose magnitude is at most this, 2⁵³, are exact as a 64-bit float.
pub(crate) const EXACT_IN_FLOAT: u64 = 1 << 53;

/// The paths, among `paths`, of the columns of 64-bit integers of `schema` that hold, in any of
/// `batches`, a value a 64-bit float would round; each batch's columns are `schema`'s, in order.
pub(crate) fn rounding(
    schema: &TableSchema,
    paths: &[ColumnPath],
    batches: &[RecordBatch],
) -> BTreeSet<ColumnPath> {
    schema
        .fields()
        .iter()
        .zip(paths)
        .enumerate()
        .filter(|(_, (field, _))| *field.logical_type() == LogicalType::Int64)
        .filter(|(index, _)| {
            batches
                .iter()
                .any(|batch| rounds(batch.column(*index).as_ref()))
        })
        .map(|(_, (_, path))| path.clone())
        .collect()
}

/// Whether `array`, of 64-bit integers in any encoding, holds at any row one a 64-bit float
/// would round.
// Out of line, its loop over values keeps 2⁵³ in a register; inlined into a caller's loop over
// columns, it loads it again at every value.
#[inline(never)]
pub(crate) fn rounds(array: &dyn Array) -> bool {
    match array.data_type() {
        // A plain column is read without mapping its rows.
        DataType::Int64 => beyond(array.as_primitive::<Int64Type>(), 0..array.len()),
        _ => rounds_at(array, &Rows::All(array.len())),
    }
}

/// Whether a value `rows` names of `array`, of 64-bit integers in any encoding, is one a 64-bit
/// float would round.
///
/// Unsigned 32-bit integers, and arrays of any type no 64-bit integer is held in, hold none.
pub(crate) fn rounds_at(array: &dyn Array, rows: &Rows<'_>) -> bool {
    match array.data_type() {
        DataType::Int64 => {
            let integers = array.as_primitive::<Int64Type>();
            rows.ranges().any(|range| beyond(integers, range))
        }
        DataType::Dictionary(key, _) => {
            let values = array.as_any_dictionary().values().as_ref();
            rows.ranges()
                .flatten()
                .any(|row| key_at(array, key, row).is_some_and(|value| rounds_in(values, value)))
        }
        DataType::RunEndEncoded(ends, _) => {
            let (ends, values) = runs(array, ends.data_type());
            let runs = Rows::Runs(Rc::new(rows.clone()), ends);
            rounds_at(values, &Rows::Clamped(Rc::new(runs), values.len()))
        }
        _ => false,
    }
}

/// Whether the value at `index` of `array`, of 64-bit integers in any encoding, is one a 64-bit
/// float would round.
fn rounds_in(array: &dyn Array, index: usize) -> bool {
    match array.data_type() {
        DataType::Int64 => {
            let integers = array.as_primitive::<Int64Type>();
            integers.is_valid(index) && outside(integers.value(index))
        }
        DataType::Dictionary(key, _) => key_at(array, key, index)
            .is_some_and(|value| rounds_in(array.as_any_dictionary().values().as_ref(), value)),
        DataType::RunEndEncoded(ends, _) => {
            let (ends, values) = runs(array, ends.data_type());
            rounds_in(values, ends.physical(index))
        }
        _ => false,
    }
}

/// Whether a value of `integers` in `range` that is not null is one a 64-bit float would round.
fn beyond(integers: &Int64Array, range: Range<usize>) -> bool {
    let values = &integers.values()[range.clone()];
    match integers.nulls() {
        None => values.iter().any(|value| outside(*value)),
        Some(nulls) => values
            .iter()
            .zip(nulls.inner().slice(range.start, range.len()).iter())
            .any(|(value, valid)| valid && outside(*value)),
    }
}

/// Whether a 64-bit float would round `value`.
fn outside(value: i64) -> bool {
    value.unsigned_abs() > EXACT_IN_FLOAT
}

/// The value the key at `row` of `array`, a dictionary keyed by `key`, names, where it is valid.
fn key_at(array: &dyn Array, key: &DataType, row: usize) -> Option<usize> {
    fn at<K: ArrowDictionaryKeyType>(array: &dyn Array, row: usize) -> Option<usize> {
        let keys = array.as_dictionary::<K>().keys();
        keys.is_valid(row).then(|| keys.value(row).as_usize())
    }
    match key {
        DataType::Int8 => at::<Int8Type>(array, row),
        DataType::Int16 => at::<Int16Type>(array, row),
        DataType::Int32 => at::<Int32Type>(array, row),
        DataType::Int64 => at::<Int64Type>(array, row),
        DataType::UInt8 => at::<UInt8Type>(array, row),
        DataType::UInt16 => at::<UInt16Type>(array, row),
        DataType::UInt32 => at::<UInt32Type>(array, row),
        _ => at::<UInt64Type>(array, row),
    }
}

/// The ends of `array`'s runs, of the width `ends` names, and the values they hold.
fn runs<'a>(array: &'a dyn Array, ends: &DataType) -> (Ends<'a>, &'a dyn Array) {
    match ends {
        DataType::Int16 => {
            let runs = array.as_run::<Int16Type>();
            (Ends::I16(runs.run_ends()), runs.values().as_ref())
        }
        DataType::Int32 => {
            let runs = array.as_run::<Int32Type>();
            (Ends::I32(runs.run_ends()), runs.values().as_ref())
        }
        _ => {
            let runs = array.as_run::<Int64Type>();
            (Ends::I64(runs.run_ends()), runs.values().as_ref())
        }
    }
}

#[cfg(test)]
mod tests;

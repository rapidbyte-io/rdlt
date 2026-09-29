//! Which columns of 64-bit integers hold a value a 64-bit float would round: a table's column of
//! integers every one of which a float holds exactly takes floats without loss.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use rdlt_connector::{ColumnPath, LogicalType, TableSchema};

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
        .filter(|(index, _)| batches.iter().any(|batch| rounds(batch.column(*index))))
        .map(|(_, (_, path))| path.clone())
        .collect()
}

/// Whether `array`, of 64-bit integers, holds one a 64-bit float would round.
///
/// An encoded column is decoded first, so only the values its rows hold are read; one that cannot
/// be decoded counts as rounding, which at worst keeps floats from the column.
fn rounds(array: &ArrayRef) -> bool {
    let beyond = |value: i64| value.unsigned_abs() > EXACT_IN_FLOAT;
    if *array.data_type() != DataType::Int64 {
        return arrow_cast::cast(array, &DataType::Int64).map_or(true, |plain| rounds(&plain));
    }
    let integers = array.as_primitive::<Int64Type>();
    match integers.nulls() {
        None => integers.values().iter().any(|value| beyond(*value)),
        Some(nulls) => integers
            .values()
            .iter()
            .zip(nulls.iter())
            .any(|(value, valid)| valid && beyond(*value)),
    }
}

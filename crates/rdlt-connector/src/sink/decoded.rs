//! How much memory a batch takes once its encoded columns are decoded.

#[cfg(test)]
mod tests;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;

/// The bytes `batch` takes once its dictionary and run-end encoded columns are decoded, as the
/// engine decodes them: a few bytes pushed may take far more once written.
///
/// Each encoded column counts a copy of its values' average for every row, at any depth.
pub fn decoded_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|column| decoded(column.as_ref()))
        .fold(0, u64::saturating_add)
}

fn decoded(array: &dyn Array) -> u64 {
    match array.data_type() {
        DataType::RunEndEncoded(ends, _) => {
            let values = match ends.data_type() {
                DataType::Int16 => array.as_run::<Int16Type>().values(),
                DataType::Int32 => array.as_run::<Int32Type>().values(),
                _ => array.as_run::<Int64Type>().values(),
            };
            per_row(values.as_ref(), array.len())
        }
        DataType::Dictionary(..) => {
            let dictionary = array.as_any_dictionary();
            size(dictionary.keys())
                .saturating_add(per_row(dictionary.values().as_ref(), array.len()))
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .map(|column| decoded(column.as_ref()))
            .fold(offsets(array), u64::saturating_add),
        DataType::List(_) => {
            offsets(array).saturating_add(decoded(array.as_list::<i32>().values()))
        }
        DataType::LargeList(_) => {
            offsets(array).saturating_add(decoded(array.as_list::<i64>().values()))
        }
        DataType::FixedSizeList(..) => {
            offsets(array).saturating_add(decoded(array.as_fixed_size_list().values()))
        }
        DataType::Map(..) => offsets(array).saturating_add(decoded(array.as_map().entries())),
        _ => size(array),
    }
}

/// `rows` copies of the average of `values`, decoded.
fn per_row(values: &dyn Array, rows: usize) -> u64 {
    let count = u64::try_from(values.len().max(1)).unwrap_or(u64::MAX);
    let rows = u64::try_from(rows).unwrap_or(u64::MAX);
    (decoded(values) / count).saturating_mul(rows)
}

/// The memory a nested array's own buffers take: its offsets and nulls, a generous 8 bytes a row.
fn offsets(array: &dyn Array) -> u64 {
    u64::try_from(array.len())
        .unwrap_or(u64::MAX)
        .saturating_add(1)
        .saturating_mul(8)
}

/// The memory `array`'s own rows take: a slice counts its rows, not the buffers it shares, and a
/// buffer its data does, not the capacity it was built with.
fn size(array: &dyn Array) -> u64 {
    let bytes = array
        .to_data()
        .get_slice_memory_size()
        .unwrap_or_else(|_| array.get_array_memory_size());
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

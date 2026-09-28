//! How much memory a batch takes once its encoded columns are decoded.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;

/// The bytes `batch` takes once its dictionary and run-end encoded columns are decoded, as the
/// engine decodes them: a few bytes pushed may take far more once written.
///
/// Each encoded row counts the value it decodes to, at any depth, so an encoding skewed toward
/// one large value counts every copy of it.
pub fn decoded_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|column| decoded(column.as_ref()))
        .fold(0, u64::saturating_add)
}

/// The bytes each row of `batch` takes once decoded, in order, so a batch can be cut where its
/// rows' own values say; together they come to about [`decoded_bytes`].
pub fn decoded_rows(batch: &RecordBatch) -> Vec<u64> {
    let mut rows = vec![0_u64; batch.num_rows()];
    for column in batch.columns() {
        for (row, bytes) in rows.iter_mut().zip(costs(column.as_ref())) {
            *row = row.saturating_add(bytes);
        }
    }
    rows
}

fn decoded(array: &dyn Array) -> u64 {
    match array.data_type() {
        DataType::RunEndEncoded(ends, _) => {
            let (values, spans) = runs(ends.data_type(), array);
            let each = costs(values.as_ref());
            spans
                .into_iter()
                .map(|(value, rows)| each[value].saturating_mul(count(rows)))
                .fold(0, u64::saturating_add)
        }
        DataType::Dictionary(..) => {
            let dictionary = array.as_any_dictionary();
            let each = costs(dictionary.values().as_ref());
            keyed(array, &each).fold(size(dictionary.keys()), u64::saturating_add)
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

/// The bytes a row's offset, nulls and view take beside its value: a generous 8, as in [`offsets`].
const ROW: u64 = 8;

/// The bytes a view takes, beside the value it points to when that is too long to inline.
const VIEW: u64 = 16;

/// The bytes each row of `array` takes once decoded.
fn costs(array: &dyn Array) -> Vec<u64> {
    let rows = array.len();
    match array.data_type() {
        DataType::RunEndEncoded(ends, _) => {
            let (values, spans) = runs(ends.data_type(), array);
            let each = costs(values.as_ref());
            spans
                .into_iter()
                .flat_map(|(value, rows)| std::iter::repeat_n(each[value], rows))
                .collect()
        }
        DataType::Dictionary(..) => {
            let each = costs(array.as_any_dictionary().values().as_ref());
            keyed(array, &each).collect()
        }
        DataType::Utf8 | DataType::Binary => spans(array.to_data().buffer::<i32>(0), rows),
        DataType::LargeUtf8 | DataType::LargeBinary => {
            spans(array.to_data().buffer::<i64>(0), rows)
        }
        DataType::Utf8View | DataType::BinaryView => views(array),
        DataType::Struct(_) => {
            let mut costs_of_rows = vec![ROW; rows];
            for column in array.as_struct().columns() {
                for (row, bytes) in costs_of_rows.iter_mut().zip(costs(column.as_ref())) {
                    *row = row.saturating_add(bytes);
                }
            }
            costs_of_rows
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            nested(list.value_offsets(), list.values().as_ref())
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            nested(list.value_offsets(), list.values().as_ref())
        }
        DataType::Map(..) => {
            let map = array.as_map();
            nested(map.value_offsets(), map.entries())
        }
        DataType::FixedSizeList(..) => fixed(array),
        _ => vec![size(array).div_ceil(count(rows.max(1))); rows],
    }
}

/// What each view of `array` takes, with the value it points to when that is not inlined.
fn views(array: &dyn Array) -> Vec<u64> {
    array.to_data().buffer::<u128>(0)[..array.len()]
        .iter()
        .map(|view| {
            let length = u64::try_from(view & u128::from(u32::MAX)).unwrap_or(u64::MAX);
            VIEW.saturating_add(if length > 12 { length } else { 0 })
        })
        .collect()
}

/// What each list of the fixed-size list `array` takes.
fn fixed(array: &dyn Array) -> Vec<u64> {
    let list = array.as_fixed_size_list();
    let each = costs(list.values().as_ref());
    let width = usize::try_from(list.value_length()).unwrap_or(0);
    (0..array.len())
        .map(|row| {
            let first = usize::try_from(list.value_offset(row)).unwrap_or(0);
            each.get(first..first + width)
                .unwrap_or_default()
                .iter()
                .fold(ROW, |sum, bytes| sum.saturating_add(*bytes))
        })
        .collect()
}

/// The values of the run-end encoded `array`, whose run ends are of type `ends`, and each run
/// its rows span, in order: the run's value and how many of the rows it holds.
fn runs(ends: &DataType, array: &dyn Array) -> (ArrayRef, Vec<(usize, usize)>) {
    match ends {
        DataType::Int16 => spanned::<Int16Type>(array),
        DataType::Int32 => spanned::<Int32Type>(array),
        _ => spanned::<Int64Type>(array),
    }
}

fn spanned<R: RunEndIndexType>(array: &dyn Array) -> (ArrayRef, Vec<(usize, usize)>) {
    let runs = array.as_run::<R>();
    let ends = runs.run_ends();
    let mut start = ends.offset();
    let last = start + ends.len();
    let spans = if ends.is_empty() {
        Vec::new()
    } else {
        (ends.get_start_physical_index()..=ends.get_end_physical_index())
            .map(|value| {
                let end = ends.values()[value].as_usize().min(last);
                let rows = end.saturating_sub(start);
                start = end;
                (value, rows)
            })
            .collect()
    };
    (Arc::clone(runs.values()), spans)
}

/// What each row of the dictionary `array` decodes to, given what each of its values takes; a
/// null key decodes to nothing.
fn keyed<'a>(array: &dyn Array, each: &'a [u64]) -> impl Iterator<Item = u64> + 'a {
    let dictionary = array.as_any_dictionary();
    let nulls = dictionary.keys().logical_nulls();
    // A dictionary of no values has only null keys, which it cannot normalize.
    let keys = if each.is_empty() {
        vec![0; array.len()]
    } else {
        dictionary.normalized_keys()
    };
    keys.into_iter().enumerate().map(move |(row, key)| {
        if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
            0
        } else {
            each.get(key).copied().unwrap_or(0)
        }
    })
}

/// What each of `rows` variable-length values takes, from its `offsets`.
fn spans<O: ArrowNativeType>(offsets: &[O], rows: usize) -> Vec<u64> {
    offsets
        .get(..=rows)
        .unwrap_or_default()
        .windows(2)
        .map(|pair| {
            ROW.saturating_add(count(pair[1].as_usize().saturating_sub(pair[0].as_usize())))
        })
        .collect()
}

/// What each list of `values` between consecutive `offsets` takes.
fn nested<O: ArrowNativeType>(offsets: &[O], values: &dyn Array) -> Vec<u64> {
    let mut sums = Vec::with_capacity(values.len() + 1);
    sums.push(0_u64);
    for bytes in costs(values) {
        sums.push(sums[sums.len() - 1].saturating_add(bytes));
    }
    let sum = |offset: &O| sums.get(offset.as_usize()).copied().unwrap_or(0);
    offsets
        .windows(2)
        .map(|pair| ROW.saturating_add(sum(&pair[1]).saturating_sub(sum(&pair[0]))))
        .collect()
}

fn count(rows: usize) -> u64 {
    u64::try_from(rows).unwrap_or(u64::MAX)
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

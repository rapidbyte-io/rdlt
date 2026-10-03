//! The parts of a lowering plan only merge tables use: key checks, sequences and compaction.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float16Type, Float32Type, Float64Type, UInt32Type};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array};
use arrow_row::{RowConverter, SortField};
use arrow_schema::DataType;
use rdlt_connector::{LogicalType, StreamName};

use super::{ChangeRows, Source, Stamp, lower_array};
use crate::error::Error;
use crate::table::TableView;
use crate::table::convert::decoded;

/// The positions of the rows `kept` keeps.
pub(super) fn positions(kept: &BooleanArray) -> ArrayRef {
    let kept = (0..kept.len()).filter(|row| kept.value(*row));
    Arc::new(UInt32Array::from_iter_values(
        kept.map(|row| u32::try_from(row).unwrap_or(u32::MAX)),
    ))
}

/// Refuses a merge batch that lacks a key column, holds a null or NaN key, or whose changes flag
/// a key column unchanged: no row could be matched by such a key.
pub(super) fn check_key(
    stream: &StreamName,
    view: &TableView,
    batch: &RecordBatch,
    sources: &[Source],
    changes: Option<&ChangeRows>,
) -> Result<(), Error> {
    if view.table.merge.is_none() {
        return Ok(());
    }
    // A truncate names no key.
    let keyed = |row: usize| changes.is_none_or(|changes| !changes.truncates(row));
    let refuse = |code: &str, detail: String| {
        Err(Error::schema(format!("stream {stream}: {detail}"))
            .with_code(code)
            .with_stream(stream))
    };
    if view.key.len() < view.key_len {
        return refuse(
            "merge_key_missing",
            "the table has no column for part of the merge key".to_owned(),
        );
    }
    let flagged = changes.map(ChangeRows::flagged).unwrap_or_default();
    for column in &view.key {
        let name = view.model.columns[*column].name();
        match &sources[*column] {
            Source::Nulls => {
                return refuse(
                    "merge_key_missing",
                    format!("a batch has no key column {name}"),
                );
            }
            Source::Incoming(index, _) => {
                check_key_values(stream, name, batch.column(*index), &keyed)?;
                if flagged.contains(index) {
                    let detail = format!("a change flags key column {name} unchanged");
                    return refuse("merge_key_unchanged", detail);
                }
            }
            Source::Read(_) | Source::Rest(_) => {}
        }
    }
    Ok(())
}

/// Refuses `stream`'s key column `name`, `column`, where a row `keyed` says names a key holds a
/// null or a NaN, at any depth and in any encoding: `merge_key_null` or `merge_key_nan`, Schema
/// errors.
pub(crate) fn check_key_values(
    stream: &StreamName,
    name: &str,
    column: &ArrayRef,
    keyed: &dyn Fn(usize) -> bool,
) -> Result<(), Error> {
    let refuse = |code: &str, what: &str| {
        Err(
            Error::schema(format!("stream {stream}: key column {name} holds {what}"))
                .with_code(code)
                .with_stream(stream),
        )
    };
    if nulls(column, keyed) {
        return refuse("merge_key_null", "a null");
    }
    let decoded = decoded(column).map_err(|error| {
        Error::internal(format!(
            "stream {stream}: reading key column {name}: {error}"
        ))
    })?;
    if nans(decoded.as_ref())
        .iter()
        .enumerate()
        .any(|(row, nan)| *nan && keyed(row))
    {
        return refuse("merge_key_nan", "a NaN, which equals no value");
    }
    Ok(())
}

/// Whether `column` holds a null in a row `keyed` says names a key.
fn nulls(column: &ArrayRef, keyed: &dyn Fn(usize) -> bool) -> bool {
    if column.logical_null_count() == 0 {
        return false;
    }
    let nulls = column.logical_nulls();
    (0..column.len())
        .any(|row| keyed(row) && nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)))
}

/// Which rows of `array`, holding only what its rows name, hold a NaN at any depth.
fn nans(array: &dyn Array) -> Vec<bool> {
    let valid = |row: usize| array.is_valid(row);
    let floats =
        |nan: &dyn Fn(usize) -> bool| (0..array.len()).map(|row| valid(row) && nan(row)).collect();
    match array.data_type() {
        DataType::Float16 => floats(&|row| array.as_primitive::<Float16Type>().value(row).is_nan()),
        DataType::Float32 => floats(&|row| array.as_primitive::<Float32Type>().value(row).is_nan()),
        DataType::Float64 => floats(&|row| array.as_primitive::<Float64Type>().value(row).is_nan()),
        DataType::Struct(_) => {
            let mut rows = vec![false; array.len()];
            for column in array.as_struct().columns() {
                for (row, nan) in nans(column.as_ref()).into_iter().enumerate() {
                    rows[row] |= nan && valid(row);
                }
            }
            rows
        }
        DataType::List(_) => items(
            array,
            array.as_list::<i32>().offsets(),
            array.as_list::<i32>().values(),
        ),
        DataType::LargeList(_) => items(
            array,
            array.as_list::<i64>().offsets(),
            array.as_list::<i64>().values(),
        ),
        DataType::Map(..) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            items(array, map.offsets(), &entries)
        }
        DataType::FixedSizeList(_, size) => {
            let list = array.as_fixed_size_list();
            let size = usize::try_from(*size).unwrap_or(0);
            let held = nans(list.values().as_ref());
            (0..array.len())
                .map(|row| valid(row) && held.iter().skip(row * size).take(size).any(|nan| *nan))
                .collect()
        }
        _ => vec![false; array.len()],
    }
}

/// Which rows of `array`, a list whose items are `values` and whose rows `offsets` bound, hold
/// an item holding a NaN.
fn items<O: arrow_array::OffsetSizeTrait>(
    array: &dyn Array,
    offsets: &arrow_buffer::OffsetBuffer<O>,
    values: &ArrayRef,
) -> Vec<bool> {
    let held = nans(values.as_ref());
    offsets
        .windows(2)
        .enumerate()
        .map(|(row, bounds)| {
            let (start, end) = (bounds[0].as_usize(), bounds[1].as_usize());
            array.is_valid(row)
                && held
                    .get(start..end)
                    .is_some_and(|items| items.contains(&true))
        })
        .collect()
}

/// Each row's sequence in a merge table: its segment, then its position among the segment's rows,
/// the row's own unless `positions` gives each row's.
pub(super) fn sequence(
    view: &TableView,
    stamp: &Stamp,
    rows: usize,
    positions: Option<&ArrayRef>,
) -> Result<ArrayRef, arrow_schema::ArrowError> {
    let lowered = view.physical[view.model.columns.len() + 2].logical_type();
    let positions = positions.map(AsArray::as_primitive::<UInt32Type>);
    let mut seq = BinaryBuilder::with_capacity(rows, rows * 16);
    for row in 0..rows {
        let offset = positions.map_or(row as u64, |positions| u64::from(positions.value(row)));
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&stamp.segment.0.to_be_bytes());
        bytes[8..].copy_from_slice(&(stamp.first_row + offset).to_be_bytes());
        seq.append_value(bytes);
    }
    let seq: ArrayRef = Arc::new(seq.finish());
    lower_array(&seq, &LogicalType::Binary, lowered)
}

/// `batch` with only the last row of each key, in their order: the rows a merge keeps.
pub(super) fn compact(
    batch: &RecordBatch,
    key: &[usize],
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let columns: Vec<ArrayRef> = key
        .iter()
        .map(|index| Arc::clone(batch.column(*index)))
        .collect();
    let fields = columns
        .iter()
        .map(|column| SortField::new(column.data_type().clone()))
        .collect();
    let rows = RowConverter::new(fields)?.convert_columns(&columns)?;
    let mut last: BTreeMap<&[u8], u32> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        last.insert(rows.row(row).data(), u32::try_from(row).unwrap_or(u32::MAX));
    }
    if last.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let mut kept: Vec<u32> = last.into_values().collect();
    kept.sort_unstable();
    arrow_select::take::take_record_batch(batch, &UInt32Array::from(kept))
}

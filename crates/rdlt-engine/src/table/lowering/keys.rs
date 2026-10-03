//! The keys of a merge table's rows: refused where no row could be matched by them, and each
//! negative zero in them the zero it equals.

use std::num::FpCategory;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{ArrowPrimitiveType, Float16Type, Float32Type, Float64Type};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, MapArray, OffsetSizeTrait, RecordBatch,
    RecordBatchOptions, StructArray,
};
use arrow_schema::{ArrowError, DataType, FieldRef};
use rdlt_connector::StreamName;

use super::{ChangeRows, Source};
use crate::error::Error;
use crate::table::TableView;
use crate::table::convert::decoded;

/// `batch`, a merge table's, with each key column's negative zeros the zero they equal; refused
/// where it lacks a key column, holds a null or NaN key, or its changes flag a key column
/// unchanged, since no row could be matched by such a key.
pub(super) fn keyed(
    stream: &StreamName,
    view: &TableView,
    batch: RecordBatch,
    sources: &[Source],
    changes: Option<&ChangeRows>,
) -> Result<RecordBatch, Error> {
    if view.table.merge.is_none() {
        return Ok(batch);
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
    let mut columns = batch.columns().to_vec();
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
                columns[*index] = key_values(stream, name, batch.column(*index), &keyed)?;
                if flagged.contains(index) {
                    let detail = format!("a change flags key column {name} unchanged");
                    return refuse("merge_key_unchanged", detail);
                }
            }
            Source::Read(_) | Source::Rest(_) => {}
        }
    }
    with_columns(&batch, columns).map_err(|error| {
        Error::internal(format!("stream {stream}: keeping a batch's keys: {error}"))
    })
}

/// `batch` holding `columns`, each under its field's name, as the type it is: a key column
/// whose zeros changed is no longer in the encoding it arrived in.
pub(crate) fn with_columns(
    batch: &RecordBatch,
    columns: Vec<ArrayRef>,
) -> Result<RecordBatch, ArrowError> {
    let fields: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, column)| {
            field
                .as_ref()
                .clone()
                .with_data_type(column.data_type().clone())
        })
        .collect();
    let schema = arrow_schema::Schema::new_with_metadata(fields, batch.schema().metadata().clone());
    let rows = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::new(schema), columns, &rows)
}

/// `stream`'s key column `name`, `column`, with each negative zero in it, at any depth and in any
/// encoding, the zero it equals; refused where a row `keyed` says names a key holds a null or a
/// NaN: `merge_key_null` or `merge_key_nan`, Schema errors.
///
/// Keys compare as floating-point numbers do, so `-0.0` is the key `0.0`. A negative zero is the
/// only value a key column is changed in: every destination, and every row id, sees one zero.
pub(crate) fn key_values(
    stream: &StreamName,
    name: &str,
    column: &ArrayRef,
    keyed: &dyn Fn(usize) -> bool,
) -> Result<ArrayRef, Error> {
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
    let zeroed = zeroed(&decoded).map_err(|error| {
        Error::internal(format!(
            "stream {stream}: reading key column {name}: {error}"
        ))
    })?;
    Ok(zeroed.unwrap_or_else(|| Arc::clone(column)))
}

/// `array`, holding only what its rows name, with every negative zero at any depth the zero it
/// equals; none where it holds no negative zero.
fn zeroed(array: &ArrayRef) -> Result<Option<ArrayRef>, ArrowError> {
    macro_rules! floats {
        ($type:ty) => {{
            let floats = array.as_primitive::<$type>();
            let negative = |value: <$type as ArrowPrimitiveType>::Native| {
                value.is_sign_negative() && value.classify() == FpCategory::Zero
            };
            if !floats.values().iter().any(|value| negative(*value)) {
                return Ok(None);
            }
            let zeroed =
                floats.unary::<_, $type>(|value| if negative(value) { -value } else { value });
            Ok(Some(Arc::new(zeroed) as ArrayRef))
        }};
    }
    match array.data_type() {
        DataType::Float16 => floats!(Float16Type),
        DataType::Float32 => floats!(Float32Type),
        DataType::Float64 => floats!(Float64Type),
        DataType::Struct(fields) => {
            let rows = array.as_struct();
            let zeroed = rows
                .columns()
                .iter()
                .map(zeroed)
                .collect::<Result<Vec<_>, _>>()?;
            if zeroed.iter().all(Option::is_none) {
                return Ok(None);
            }
            let columns = zeroed
                .into_iter()
                .zip(rows.columns())
                .map(|(zeroed, column)| zeroed.unwrap_or_else(|| Arc::clone(column)))
                .collect();
            let rows = StructArray::try_new(fields.clone(), columns, rows.nulls().cloned())?;
            Ok(Some(Arc::new(rows)))
        }
        DataType::List(field) => listed(array.as_list::<i32>(), field),
        DataType::LargeList(field) => listed(array.as_list::<i64>(), field),
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            let Some(values) = zeroed(list.values())? else {
                return Ok(None);
            };
            let nulls = list.nulls().cloned();
            let list = FixedSizeListArray::try_new(Arc::clone(field), *size, values, nulls)?;
            Ok(Some(Arc::new(list)))
        }
        DataType::Map(field, sorted) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let Some(entries) = zeroed(&entries)? else {
                return Ok(None);
            };
            let (offsets, nulls) = (map.offsets().clone(), map.nulls().cloned());
            let entries = entries.as_struct().clone();
            let map = MapArray::try_new(Arc::clone(field), offsets, entries, nulls, *sorted)?;
            Ok(Some(Arc::new(map)))
        }
        _ => Ok(None),
    }
}

/// `list`, of items `field`, with every negative zero in its items the zero it equals; none where
/// they hold no negative zero.
fn listed<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<Option<ArrayRef>, ArrowError> {
    let Some(values) = zeroed(list.values())? else {
        return Ok(None);
    };
    let (offsets, nulls) = (list.offsets().clone(), list.nulls().cloned());
    let list = GenericListArray::<O>::try_new(Arc::clone(field), offsets, values, nulls)?;
    Ok(Some(Arc::new(list)))
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
fn items<O: OffsetSizeTrait>(
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

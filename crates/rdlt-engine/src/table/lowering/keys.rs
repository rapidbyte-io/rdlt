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
use arrow_schema::{ArrowError, DataType, FieldRef, Fields};
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
    let read = |error: ArrowError| {
        Error::internal(format!("stream {stream}: reading key column {name}")).with_source(error)
    };
    let found = floats(&decoded(column).map_err(read)?).map_err(read)?;
    if found
        .nans
        .iter()
        .enumerate()
        .any(|(row, nan)| *nan && keyed(row))
    {
        return refuse("merge_key_nan", "a NaN, which equals no value");
    }
    Ok(found.zeroed.unwrap_or_else(|| Arc::clone(column)))
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

/// What `array`'s floats hold at any depth: which rows hold a NaN, and the array with each
/// negative zero the zero it equals, where it holds one.
struct Floats {
    nans: Vec<bool>,
    zeroed: Option<ArrayRef>,
}

/// The floats of `array`, holding only what its rows name, walked once.
///
/// Decoding leaves nothing beneath a null row, so only a float's own validity is read.
fn floats(array: &ArrayRef) -> Result<Floats, ArrowError> {
    macro_rules! leaf {
        ($type:ty) => {{
            let floats = array.as_primitive::<$type>();
            let nans = (0..floats.len())
                .map(|row| floats.is_valid(row) && floats.value(row).is_nan())
                .collect();
            let negative = |value: <$type as ArrowPrimitiveType>::Native| {
                value.is_sign_negative() && value.classify() == FpCategory::Zero
            };
            let zeroed = floats
                .values()
                .iter()
                .any(|value| negative(*value))
                .then(|| {
                    let zeroed = floats
                        .unary::<_, $type>(|value| if negative(value) { -value } else { value });
                    Arc::new(zeroed) as ArrayRef
                });
            Ok(Floats { nans, zeroed })
        }};
    }
    match array.data_type() {
        DataType::Float16 => leaf!(Float16Type),
        DataType::Float32 => leaf!(Float32Type),
        DataType::Float64 => leaf!(Float64Type),
        DataType::Struct(fields) => structured(array, fields),
        DataType::List(field) => listed(array.as_list::<i32>(), field),
        DataType::LargeList(field) => listed(array.as_list::<i64>(), field),
        DataType::FixedSizeList(field, size) => fixed(array, field, *size),
        DataType::Map(field, sorted) => mapped(array, field, *sorted),
        _ => Ok(Floats {
            nans: vec![false; array.len()],
            zeroed: None,
        }),
    }
}

/// The floats of `array`, a struct of `fields`.
fn structured(array: &ArrayRef, fields: &Fields) -> Result<Floats, ArrowError> {
    let rows = array.as_struct();
    let mut nans = vec![false; array.len()];
    let mut zeroed = Vec::with_capacity(rows.num_columns());
    for column in rows.columns() {
        let found = floats(column)?;
        for (row, nan) in found.nans.into_iter().enumerate() {
            nans[row] |= nan;
        }
        zeroed.push(found.zeroed);
    }
    if zeroed.iter().all(Option::is_none) {
        return Ok(Floats { nans, zeroed: None });
    }
    let columns = zeroed
        .into_iter()
        .zip(rows.columns())
        .map(|(zeroed, column)| zeroed.unwrap_or_else(|| Arc::clone(column)))
        .collect();
    let rows = StructArray::try_new(fields.clone(), columns, rows.nulls().cloned())?;
    Ok(Floats {
        nans,
        zeroed: Some(Arc::new(rows)),
    })
}

/// The floats of `array`, a fixed-size list of items `field` and `size` items a row.
fn fixed(array: &ArrayRef, field: &FieldRef, size: i32) -> Result<Floats, ArrowError> {
    let list = array.as_fixed_size_list();
    let found = floats(list.values())?;
    let width = usize::try_from(size).unwrap_or(0);
    let nans = (0..array.len())
        .map(|row| {
            found
                .nans
                .iter()
                .skip(row * width)
                .take(width)
                .any(|nan| *nan)
        })
        .collect();
    let zeroed = found
        .zeroed
        .map(|values| {
            let nulls = list.nulls().cloned();
            FixedSizeListArray::try_new(Arc::clone(field), size, values, nulls)
                .map(|list| Arc::new(list) as ArrayRef)
        })
        .transpose()?;
    Ok(Floats { nans, zeroed })
}

/// The floats of `array`, a map of entries `field`, `sorted` or not.
fn mapped(array: &ArrayRef, field: &FieldRef, sorted: bool) -> Result<Floats, ArrowError> {
    let map = array.as_map();
    let entries: ArrayRef = Arc::new(map.entries().clone());
    let found = floats(&entries)?;
    let nans = rows_of(map.offsets(), &found.nans);
    let zeroed = found
        .zeroed
        .map(|entries| {
            let (offsets, nulls) = (map.offsets().clone(), map.nulls().cloned());
            let entries = entries.as_struct().clone();
            MapArray::try_new(Arc::clone(field), offsets, entries, nulls, sorted)
                .map(|map| Arc::new(map) as ArrayRef)
        })
        .transpose()?;
    Ok(Floats { nans, zeroed })
}

/// The floats of `list`, of items `field`.
fn listed<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<Floats, ArrowError> {
    let found = floats(list.values())?;
    let nans = rows_of(list.offsets(), &found.nans);
    let zeroed = found
        .zeroed
        .map(|values| {
            let (offsets, nulls) = (list.offsets().clone(), list.nulls().cloned());
            GenericListArray::<O>::try_new(Arc::clone(field), offsets, values, nulls)
                .map(|list| Arc::new(list) as ArrayRef)
        })
        .transpose()?;
    Ok(Floats { nans, zeroed })
}

/// Which rows of a list, whose rows `offsets` bound, hold an item `held` says holds a NaN.
fn rows_of<O: OffsetSizeTrait>(
    offsets: &arrow_buffer::OffsetBuffer<O>,
    held: &[bool],
) -> Vec<bool> {
    offsets
        .windows(2)
        .map(|bounds| {
            let (start, end) = (bounds[0].as_usize(), bounds[1].as_usize());
            held.get(start..end)
                .is_some_and(|items| items.contains(&true))
        })
        .collect()
}

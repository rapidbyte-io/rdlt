//! The check each column of JSON in an Arrow push meets before the engine uses it: every value
//! its rows name is one JSON value nested no deeper than the limit.
//!
//! Only the values rows name are read, each once, whatever names it: a dictionary's value its
//! keys name, a run's value, a list's or a list view's items, a struct's fields where the struct
//! is not null. Nothing is decoded or copied.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow_array::{Array, OffsetSizeTrait, RecordBatch};
use arrow_buffer::ArrowNativeType;
use arrow_schema::{DataType, Field, Schema};

use super::{JsonError, check};

#[cfg(test)]
mod tests;

/// The Arrow field metadata key naming an extension type, and the JSON extension's name.
const EXTENSION_NAME: &str = "ARROW:extension:name";
const JSON_EXTENSION: &str = "arrow.json";

/// A value of a column of JSON that is not JSON.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NotJson {
    /// The column's name in the batch.
    pub(crate) column: String,
    pub(crate) error: JsonError,
}

/// Checks every value of `batch`'s columns of JSON, at any depth, that its rows name.
///
/// # Errors
///
/// The first column holding a value that is not JSON, and why.
pub(crate) fn check_batch(batch: &RecordBatch) -> Result<(), NotJson> {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !field_holds_json(field) {
            continue;
        }
        let rows = vec![true; column.len()];
        walk(field, false, column.as_ref(), &rows).map_err(|error| NotJson {
            column: field.name().clone(),
            error,
        })?;
    }
    Ok(())
}

/// Whether a batch of `schema` holds a column of JSON at any depth.
pub(crate) fn holds_json(schema: &Schema) -> bool {
    schema.fields().iter().any(|field| field_holds_json(field))
}

/// Whether `field` is a column of JSON, or holds one at any depth.
fn field_holds_json(field: &Field) -> bool {
    is_json(field)
        || children(field.data_type())
            .iter()
            .any(|child| field_holds_json(child))
}

/// Whether `field` is a column of JSON: the JSON extension over text, as it is or encoded.
fn is_json(field: &Field) -> bool {
    let storage = match field.data_type() {
        DataType::Dictionary(_, values) => values.as_ref(),
        DataType::RunEndEncoded(_, values) => values.data_type(),
        other => other,
    };
    field.metadata().get(EXTENSION_NAME).map(String::as_str) == Some(JSON_EXTENSION)
        && matches!(
            storage,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
}

/// The fields a value of `data_type` holds others in.
fn children(data_type: &DataType) -> Vec<&Field> {
    match data_type {
        DataType::Struct(fields) => fields.iter().map(AsRef::as_ref).collect(),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => vec![item.as_ref()],
        DataType::Dictionary(_, values) => children(values),
        DataType::RunEndEncoded(_, values) => children(values.data_type()),
        _ => Vec::new(),
    }
}

/// Checks the values of `array`, of `field`, at the rows `rows` names: as JSON where the field,
/// or the encoded column it is the values of, is one of JSON (`encoded`).
fn walk(field: &Field, encoded: bool, array: &dyn Array, rows: &[bool]) -> Result<(), JsonError> {
    let json = encoded || is_json(field);
    let named = |row: usize| rows[row] && array.is_valid(row);
    match array.data_type() {
        DataType::Utf8 if json => texts(array.as_string::<i32>().iter(), rows),
        DataType::LargeUtf8 if json => texts(array.as_string::<i64>().iter(), rows),
        DataType::Utf8View if json => texts(array.as_string_view().iter(), rows),
        DataType::Dictionary(key, _) => keyed(field, json, array, key, rows),
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => runs::<Int16Type>(field, json, array, rows),
            DataType::Int32 => runs::<Int32Type>(field, json, array, rows),
            _ => runs::<Int64Type>(field, json, array, rows),
        },
        DataType::Struct(fields) => {
            let within: Vec<bool> = (0..array.len()).map(named).collect();
            let columns = array.as_struct().columns();
            for (field, column) in fields.iter().zip(columns) {
                if field_holds_json(field) {
                    walk(field, false, column.as_ref(), &within)?;
                }
            }
            Ok(())
        }
        DataType::List(item) => listed(item, array.as_list::<i32>().offsets(), array, rows),
        DataType::LargeList(item) => listed(item, array.as_list::<i64>().offsets(), array, rows),
        DataType::Map(item, _) => {
            let map = array.as_map();
            let entries: &dyn Array = map.entries();
            items(item, entries, ranges(map.offsets(), array, rows))
        }
        DataType::ListView(item) => viewed::<i32>(item, array, rows),
        DataType::LargeListView(item) => viewed::<i64>(item, array, rows),
        DataType::FixedSizeList(item, _) => {
            let list = array.as_fixed_size_list();
            let size = list.value_length().as_usize();
            let named = (0..array.len()).filter(|row| named(*row));
            let spans = named.map(|row| {
                let first = list.value_offset(row).as_usize();
                first..first + size
            });
            items(item, list.values().as_ref(), spans.collect())
        }
        _ => Ok(()),
    }
}

/// Checks the texts `rows` names among `texts`.
fn texts<'a>(texts: impl Iterator<Item = Option<&'a str>>, rows: &[bool]) -> Result<(), JsonError> {
    for (text, named) in texts.zip(rows) {
        if let (Some(text), true) = (text, *named) {
            check(text)?;
        }
    }
    Ok(())
}

/// Checks the values of a dictionary array that its keys name at `rows`, each once.
fn keyed(
    field: &Field,
    json: bool,
    array: &dyn Array,
    key: &DataType,
    rows: &[bool],
) -> Result<(), JsonError> {
    let (keys, values) = match key {
        DataType::Int8 => named_keys::<Int8Type>(array, rows),
        DataType::Int16 => named_keys::<Int16Type>(array, rows),
        DataType::Int32 => named_keys::<Int32Type>(array, rows),
        DataType::Int64 => named_keys::<Int64Type>(array, rows),
        DataType::UInt8 => named_keys::<UInt8Type>(array, rows),
        DataType::UInt16 => named_keys::<UInt16Type>(array, rows),
        DataType::UInt32 => named_keys::<UInt32Type>(array, rows),
        _ => named_keys::<UInt64Type>(array, rows),
    };
    walk(field, json, values, &keys)
}

/// Which values of the dictionary array `array` its valid keys at `rows` name, and its values.
fn named_keys<'a, K: arrow_array::types::ArrowDictionaryKeyType>(
    array: &'a dyn Array,
    rows: &[bool],
) -> (Vec<bool>, &'a dyn Array) {
    let dictionary = array.as_dictionary::<K>();
    let values = dictionary.values().as_ref();
    let mut named = vec![false; values.len()];
    for (key, row) in dictionary.keys().iter().zip(rows) {
        if let (Some(key), true) = (key, *row)
            && let Some(slot) = named.get_mut(key.as_usize())
        {
            *slot = true;
        }
    }
    (named, values)
}

/// Checks the values of a run-end encoded array that its runs hold at `rows`, each once.
fn runs<R: RunEndIndexType>(
    field: &Field,
    json: bool,
    array: &dyn Array,
    rows: &[bool],
) -> Result<(), JsonError>
where
    R::Native: ArrowNativeType,
{
    let runs = array.as_run::<R>();
    let values = runs.values().as_ref();
    let mut named = vec![false; values.len()];
    for (row, _) in rows.iter().enumerate().filter(|(_, named)| **named) {
        if let Some(slot) = named.get_mut(runs.get_physical_index(row)) {
            *slot = true;
        }
    }
    walk(field, json, values, &named)
}

/// Checks the items of a list of `item` that its rows at `rows` hold, between `offsets`.
fn listed<O: OffsetSizeTrait>(
    item: &Field,
    offsets: &[O],
    array: &dyn Array,
    rows: &[bool],
) -> Result<(), JsonError> {
    let values = match array.data_type() {
        DataType::LargeList(_) => array.as_list::<i64>().values().as_ref(),
        _ => array.as_list::<i32>().values().as_ref(),
    };
    items(item, values, ranges(offsets, array, rows))
}

/// The ranges of items between `offsets` that the valid rows of `array` at `rows` hold.
fn ranges<O: ArrowNativeType>(
    offsets: &[O],
    array: &dyn Array,
    rows: &[bool],
) -> Vec<std::ops::Range<usize>> {
    (0..array.len())
        .filter(|row| rows[*row] && array.is_valid(*row))
        .map(|row| offsets[row].as_usize()..offsets[row + 1].as_usize())
        .collect()
}

/// Checks the items a list view of `item` names at its valid rows `rows` names, each once.
fn viewed<O: OffsetSizeTrait>(
    item: &Field,
    array: &dyn Array,
    rows: &[bool],
) -> Result<(), JsonError> {
    let views = array.as_list_view::<O>();
    let spans = (0..array.len())
        .filter(|row| rows[*row] && array.is_valid(*row))
        .map(|row| {
            let first = views.offsets()[row].as_usize();
            first..first + views.sizes()[row].as_usize()
        });
    items(item, views.values().as_ref(), spans.collect())
}

/// Checks the items of `values`, of `item`, in `spans`, each once however many spans name it.
fn items(
    item: &Field,
    values: &dyn Array,
    spans: Vec<std::ops::Range<usize>>,
) -> Result<(), JsonError> {
    if !field_holds_json(item) {
        return Ok(());
    }
    // How many spans open less how many close before each item: an item is named where more
    // have opened, so spans naming the same items cost one step each, not one an item.
    let mut opened = vec![0_i64; values.len() + 1];
    for span in spans {
        let end = span.end.min(values.len());
        let start = span.start.min(end);
        opened[start] += 1;
        opened[end] -= 1;
    }
    let mut open = 0;
    let named: Vec<bool> = opened[..values.len()]
        .iter()
        .map(|change| {
            open += change;
            open > 0
        })
        .collect();
    walk(item, false, values, &named)
}

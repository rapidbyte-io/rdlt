//! The check each column of JSON in an Arrow push meets before the engine uses it: every value
//! its rows name is one JSON value nested no deeper than the limit.
//!
//! Only the values rows name are read, each once, whatever names it: a dictionary's value its
//! keys name, a run's value, a list's or a list view's items, a struct's fields where the struct
//! is not null. Nothing is decoded or copied, and nothing is held a row: which rows are named is
//! read as ranges, each level mapping its parent's. Only a dictionary's named values and a list
//! view naming its items out of order are held, as [`held`] says, before they are read.

use std::rc::Rc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_buffer::{ArrowNativeType, BooleanBufferBuilder};
use arrow_schema::{DataType, Field};
use rdlt_connector::LogicalType;

use super::{JsonError, check};
use rows::{Ends, Offsets, Rows, Views};

mod held;
mod rows;
#[cfg(test)]
mod tests;

pub(crate) use held::held;

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
        walk(column.as_ref(), &Rows::All(column.len())).map_err(|error| NotJson {
            column: field.name().clone(),
            error,
        })?;
    }
    Ok(())
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
    rdlt_connector::Field::extension_type(field) == Some(LogicalType::Json)
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

/// Checks the values of `array`, of a field holding JSON, at the rows `rows` names: text there is
/// the field's JSON, as it is or the values of its dictionary or its runs.
fn walk(array: &dyn Array, rows: &Rows<'_>) -> Result<(), JsonError> {
    let valid = || match array.nulls() {
        Some(nulls) => Rows::Valid(Rc::new(rows.clone()), nulls.clone()),
        None => rows.clone(),
    };
    match array.data_type() {
        DataType::Utf8 => {
            let texts = array.as_string::<i32>();
            checked(rows, |row| texts.is_valid(row).then(|| texts.value(row)))
        }
        DataType::LargeUtf8 => {
            let texts = array.as_string::<i64>();
            checked(rows, |row| texts.is_valid(row).then(|| texts.value(row)))
        }
        DataType::Utf8View => {
            let texts = array.as_string_view();
            checked(rows, |row| texts.is_valid(row).then(|| texts.value(row)))
        }
        DataType::Dictionary(key, _) => keyed(array, key, rows),
        DataType::RunEndEncoded(ends, _) => {
            let ends = match ends.data_type() {
                DataType::Int16 => Ends::I16(array.as_run::<Int16Type>().run_ends()),
                DataType::Int32 => Ends::I32(array.as_run::<Int32Type>().run_ends()),
                _ => Ends::I64(array.as_run::<Int64Type>().run_ends()),
            };
            let values = match ends {
                Ends::I16(_) => array.as_run::<Int16Type>().values(),
                Ends::I32(_) => array.as_run::<Int32Type>().values(),
                Ends::I64(_) => array.as_run::<Int64Type>().values(),
            };
            let runs = Rows::Runs(Rc::new(rows.clone()), ends);
            walk(values.as_ref(), &Rows::Clamped(Rc::new(runs), values.len()))
        }
        _ => nested(array, valid()),
    }
}

/// Checks the values a struct, a list, a map or a list view `array` holds at its `valid` rows,
/// those a batch's rows name and it does not hold null.
fn nested(array: &dyn Array, valid: Rows<'_>) -> Result<(), JsonError> {
    match array.data_type() {
        DataType::Struct(fields) => {
            let within = valid;
            let columns = array.as_struct().columns();
            for (field, column) in fields.iter().zip(columns) {
                if field_holds_json(field) {
                    walk(column.as_ref(), &within)?;
                }
            }
            Ok(())
        }
        DataType::List(item) => {
            let list = array.as_list::<i32>();
            let named = Rows::Items(Rc::new(valid.clone()), Offsets::Small(list.offsets()));
            items(item, list.values().as_ref(), &named)
        }
        DataType::LargeList(item) => {
            let list = array.as_list::<i64>();
            let named = Rows::Items(Rc::new(valid.clone()), Offsets::Large(list.offsets()));
            items(item, list.values().as_ref(), &named)
        }
        DataType::Map(item, _) => {
            let map = array.as_map();
            let named = Rows::Items(Rc::new(valid.clone()), Offsets::Small(map.offsets()));
            items(item, map.entries(), &named)
        }
        DataType::ListView(item) => {
            let views = array.as_list_view::<i32>();
            let spans = Views::Small(views.offsets(), views.sizes());
            viewed(
                item,
                views.values().as_ref(),
                spans,
                array.len(),
                valid.clone(),
            )
        }
        DataType::LargeListView(item) => {
            let views = array.as_list_view::<i64>();
            let spans = Views::Large(views.offsets(), views.sizes());
            viewed(
                item,
                views.values().as_ref(),
                spans,
                array.len(),
                valid.clone(),
            )
        }
        DataType::FixedSizeList(item, _) => {
            let list = array.as_fixed_size_list();
            let size = list.value_length().as_usize();
            let named = Rows::Fixed(Rc::new(valid.clone()), size);
            items(item, list.values().as_ref(), &named)
        }
        _ => Ok(()),
    }
}

/// Checks the text `text` gives for each row `rows` names, where it gives one.
fn checked<'a>(rows: &Rows<'_>, text: impl Fn(usize) -> Option<&'a str>) -> Result<(), JsonError> {
    for row in rows.ranges().flatten() {
        if let Some(text) = text(row) {
            check(text)?;
        }
    }
    Ok(())
}

/// Checks the values of a dictionary array that its keys name at `rows`, each once.
fn keyed(array: &dyn Array, key: &DataType, rows: &Rows<'_>) -> Result<(), JsonError> {
    let (named, values) = match key {
        DataType::Int8 => named_keys::<Int8Type>(array, rows),
        DataType::Int16 => named_keys::<Int16Type>(array, rows),
        DataType::Int32 => named_keys::<Int32Type>(array, rows),
        DataType::Int64 => named_keys::<Int64Type>(array, rows),
        DataType::UInt8 => named_keys::<UInt8Type>(array, rows),
        DataType::UInt16 => named_keys::<UInt16Type>(array, rows),
        DataType::UInt32 => named_keys::<UInt32Type>(array, rows),
        _ => named_keys::<UInt64Type>(array, rows),
    };
    walk(values, &named)
}

/// Which values of the dictionary array `array` its valid keys at `rows` name, and its values:
/// a bit a value where that is no more than a byte a key, else the keys named, sorted.
///
/// A valid key names one of the values, as Arrow checks wherever an array is made or read.
fn named_keys<'a, K: ArrowDictionaryKeyType>(
    array: &'a dyn Array,
    rows: &Rows<'_>,
) -> (Rows<'a>, &'a dyn Array) {
    let dictionary = array.as_dictionary::<K>();
    let values = dictionary.values().as_ref();
    let keys = dictionary.keys();
    let named = rows
        .ranges()
        .flatten()
        .filter(|row| keys.is_valid(*row))
        .map(|row| keys.value(row).as_usize());
    if held::bitmapped(values.len(), keys.len()) {
        let mut bits = BooleanBufferBuilder::new(values.len());
        bits.append_n(values.len(), false);
        for key in named {
            bits.set_bit(key, true);
        }
        return (Rows::Bits(Rc::new(bits.finish())), values);
    }
    // A key a row at most, gathered in place: what `held` charges.
    let mut listed: Vec<usize> = Vec::with_capacity(keys.len());
    listed.extend(named);
    listed.sort_unstable();
    listed.dedup();
    (Rows::Listed(Rc::new(listed)), values)
}

/// Checks the items of a list view of `item` that its `named` rows of `len` name, each once:
/// read as a list's where its rows name them in order, else gathered and ordered first.
fn viewed(
    item: &Field,
    values: &dyn Array,
    views: Views<'_>,
    len: usize,
    named: Rows<'_>,
) -> Result<(), JsonError> {
    if !field_holds_json(item) {
        return Ok(());
    }
    if views.ordered(len) {
        return items(item, values, &Rows::Viewed(Rc::new(named), views));
    }
    // A span a row at most, gathered in place: what `held` charges.
    let mut spans: Vec<std::ops::Range<usize>> = Vec::with_capacity(len);
    spans.extend(
        named
            .ranges()
            .flatten()
            .map(|row| views.span(row))
            .filter(|span| !span.is_empty()),
    );
    rows::disjoint(&mut spans);
    items(item, values, &Rows::Ranges(Rc::new(spans)))
}

/// Checks the items of `values`, of `item`, that `named` names.
fn items(item: &Field, values: &dyn Array, named: &Rows<'_>) -> Result<(), JsonError> {
    if !field_holds_json(item) {
        return Ok(());
    }
    let within = Rows::Clamped(Rc::new(named.clone()), values.len());
    walk(values, &within)
}

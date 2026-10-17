//! Stored values as the JSON text a column that widened to `Json` holds of them, exactly as the
//! engine renders a value of their type into a `Json` column.
//!
//! Text becomes a JSON string, but text the engine rendered a value of a type the destination
//! does not store as, which its field names, becomes that value's JSON. Booleans and numbers
//! become their JSON text, a decimal its digits as written, a float the shortest text that reads
//! back as it, bytes a string of lower-case hex, a UUID its hyphenated string, a temporal value a
//! string of its exact text, a struct an object of every field, null ones too, and a list an
//! array; a value already `Json` stays as it is, at any depth.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, PrimitiveArray, StringArray, StructArray,
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, FieldRef};

use super::refused::{TYPE_UNCONVERTIBLE, VALUE_UNHOLDABLE, refused};
use crate::{Field, LogicalType, instants, json};

/// The JSON text of each value of a column; `None` for a null.
type Texts = Vec<Option<String>>;

/// Whether `field` holds `Json`.
pub(super) fn is_json(field: &ArrowField) -> bool {
    Field::extension_type(field) == Some(LogicalType::Json)
}

/// Whether `field` holds text, not JSON, as it is or under a dictionary or a run-end encoding.
pub(super) fn holds_text(field: &ArrowField) -> bool {
    let storage = match field.data_type() {
        DataType::Dictionary(_, values) => values.as_ref(),
        DataType::RunEndEncoded(_, values) => values.data_type(),
        other => other,
    };
    let text = matches!(
        storage,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    );
    text && !is_json(field)
}

/// The type `field`'s text was rendered from, which the merge reads where its column widens to
/// `Json`: where it holds text and names a type.
pub(super) fn rendered_from(field: &ArrowField) -> Option<&String> {
    let named = field.metadata().get(crate::LOGICAL_TYPE_KEY);
    named.filter(|_| holds_text(field))
}

/// Each value of `array`, whose field is `field`, as its JSON text; nulls stay null.
pub(super) fn rendered(array: &ArrayRef, field: &ArrowField) -> Result<ArrayRef, ArrowError> {
    Ok(Arc::new(StringArray::from(texts(array, field)?)))
}

/// The JSON text of each value of `array`, whose field is `field`.
fn texts(array: &ArrayRef, field: &ArrowField) -> Result<Texts, ArrowError> {
    let decoded = |to: DataType| {
        let values = arrow_cast::cast(array, &to)?;
        texts(&values, &field.clone().with_data_type(to))
    };
    match array.data_type() {
        DataType::Null => Ok(vec![None; array.len()]),
        DataType::Dictionary(_, values) => decoded(values.as_ref().clone()),
        DataType::RunEndEncoded(_, values) => decoded(values.data_type().clone()),
        DataType::LargeUtf8 | DataType::Utf8View => decoded(DataType::Utf8),
        DataType::FixedSizeBinary(16)
            if Field::extension_type(field) == Some(LogicalType::Uuid) =>
        {
            let uuids = array.as_fixed_size_binary();
            Ok(each(array, |row, out| {
                quoted_hex(uuids.value(row), true, out);
            }))
        }
        DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            decoded(DataType::Binary)
        }
        DataType::Struct(_) => structs(array.as_struct()),
        DataType::List(item) => lists(array, item),
        DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::ListView(item)
        | DataType::LargeListView(item) => decoded(DataType::List(Arc::clone(item))),
        _ => scalars(array, field),
    }
}

/// The JSON text of each value of `array`, a column of plain text, bytes, booleans, numbers or
/// temporal values, whose field is `field`.
fn scalars(array: &ArrayRef, field: &ArrowField) -> Result<Texts, ArrowError> {
    match array.data_type() {
        DataType::Utf8 if is_json(field) => {
            let strings = array.as_string::<i32>();
            Ok(each(array, |row, out| out.push_str(strings.value(row))))
        }
        DataType::Utf8 => {
            if let Some(logical) = Field::lowered_from(field) {
                return lowered(array, &logical);
            }
            let strings = array.as_string::<i32>();
            Ok(each(array, |row, out| {
                json::write_string(strings.value(row), out);
            }))
        }
        DataType::Binary => {
            let bytes = array.as_binary::<i32>();
            Ok(each(array, |row, out| {
                quoted_hex(bytes.value(row), false, out);
            }))
        }
        DataType::Boolean => {
            let booleans = array.as_boolean();
            Ok(each(array, |row, out| {
                out.push_str(if booleans.value(row) { "true" } else { "false" });
            }))
        }
        DataType::Float32 | DataType::Float64 => {
            Ok(each(array, |row, out| float(array.as_ref(), row, out)))
        }
        kind if kind.is_integer() || kind.is_decimal() => {
            let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())?;
            Ok(each(array, |row, out| {
                out.push_str(&formatter.value(row).to_string());
            }))
        }
        kind => {
            let renderer =
                instants::Renderer::new(array.as_ref())?.ok_or_else(|| unrendered(kind))?;
            Ok(each(array, |row, out| {
                out.push('"');
                renderer.write(row, out);
                out.push('"');
            }))
        }
    }
}

/// The JSON text of each value of `array`, text the engine rendered values of `logical` as, a
/// type the destination does not store: a boolean, an integer or a decimal is its own JSON, a
/// float the shortest text that reads back as the float it names, a nested value or JSON the
/// JSON text it is, and any other a JSON string of its text, as the engine writes each.
fn lowered(array: &ArrayRef, logical: &LogicalType) -> Result<Texts, ArrowError> {
    let strings = array.as_string::<i32>();
    match logical {
        LogicalType::Bool
        | LogicalType::Int8
        | LogicalType::Int16
        | LogicalType::Int32
        | LogicalType::Int64
        | LogicalType::Decimal(_)
        | LogicalType::Struct(_)
        | LogicalType::List(_)
        | LogicalType::Json => Ok(each(array, |row, out| out.push_str(strings.value(row)))),
        LogicalType::Float32 => {
            let floats: Float32Array = parsed(strings, logical)?;
            Ok(each(array, |row, out| float(&floats, row, out)))
        }
        LogicalType::Float64 => {
            let floats: Float64Array = parsed(strings, logical)?;
            Ok(each(array, |row, out| float(&floats, row, out)))
        }
        _ => Ok(each(array, |row, out| {
            json::write_string(strings.value(row), out);
        })),
    }
}

/// The floats `strings` are the text of, of `logical`.
fn parsed<T>(strings: &StringArray, logical: &LogicalType) -> Result<PrimitiveArray<T>, ArrowError>
where
    T: ArrowPrimitiveType,
    T::Native: std::str::FromStr,
{
    let float = |text: &str| {
        text.parse::<T::Native>().map_err(|_| {
            let message = format!("{text} is the text of no {logical}");
            refused(VALUE_UNHOLDABLE, message)
        })
    };
    strings
        .iter()
        .map(|text| text.map(float).transpose())
        .collect()
}

/// Appends the float at `row` of `floats` as JSON.
fn float(floats: &dyn Array, row: usize, out: &mut String) {
    let mut bytes = Vec::new();
    json::write_float(floats, row, &mut bytes);
    out.push_str(&String::from_utf8_lossy(&bytes));
}

/// What `write` writes of each value of `array` that is not null.
fn each(array: &ArrayRef, write: impl Fn(usize, &mut String)) -> Texts {
    let nulls = array.logical_nulls();
    (0..array.len())
        .map(|row| {
            nulls
                .as_ref()
                .is_none_or(|nulls| nulls.is_valid(row))
                .then(|| {
                    let mut out = String::new();
                    write(row, &mut out);
                    out
                })
        })
        .collect()
}

/// Each struct of `structs` as a JSON object of every field, in order, a null one as `null`.
fn structs(structs: &StructArray) -> Result<Texts, ArrowError> {
    let columns = structs
        .fields()
        .iter()
        .zip(structs.columns())
        .map(|(field, column)| texts(column, field))
        .collect::<Result<Vec<_>, _>>()?;
    let names: Vec<String> = structs
        .fields()
        .iter()
        .map(|field| {
            let mut name = String::new();
            json::write_string(field.name(), &mut name);
            name
        })
        .collect();
    let array: ArrayRef = Arc::new(structs.clone());
    Ok(each(&array, |row, out| {
        out.push('{');
        for (index, (name, column)) in names.iter().zip(&columns).enumerate() {
            if index != 0 {
                out.push(',');
            }
            out.push_str(name);
            out.push(':');
            out.push_str(column[row].as_deref().unwrap_or("null"));
        }
        out.push('}');
    }))
}

/// Each list of `array`, a list of `item`, as a JSON array, a null item as `null`.
fn lists(array: &ArrayRef, item: &FieldRef) -> Result<Texts, ArrowError> {
    let lists = array.as_list::<i32>();
    let items = texts(lists.values(), item)?;
    let offsets = lists.value_offsets();
    let at = |offset: i32| usize::try_from(offset).unwrap_or_default();
    Ok(each(array, |row, out| {
        out.push('[');
        let held = &items[at(offsets[row])..at(offsets[row + 1])];
        for (index, item) in held.iter().enumerate() {
            if index != 0 {
                out.push(',');
            }
            out.push_str(item.as_deref().unwrap_or("null"));
        }
        out.push(']');
    }))
}

/// Appends `bytes` as a JSON string of lower-case hex; a UUID's 16 bytes in its hyphenated form.
fn quoted_hex(bytes: &[u8], uuid: bool, out: &mut String) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for (index, byte) in bytes.iter().enumerate() {
        if uuid && matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out.push('"');
}

/// The refusal of a column of `kind`, which no `Json` column holds as JSON text.
fn unrendered(kind: &DataType) -> ArrowError {
    refused(
        TYPE_UNCONVERTIBLE,
        format!("no JSON text of {kind} keeps every value"),
    )
}

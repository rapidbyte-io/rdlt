//! Row identity (spec §7.4): each row's `_rdlt_id`, the xxh3-128 of a canonical encoding of its
//! key, of the whole row, or of its parent's id and its index in the parent's array.
//!
//! The encoding tags every value with its kind and renders every number one way, so a value hashes
//! alike whichever batch, chunk or Arrow type carries it: integers as their digits, floats as
//! their shortest round-trip text, which for an integral float is its digits too. JSON text, as
//! a column whose values mix types holds it, encodes as the values it renders. Objects list their
//! non-null fields in name order, so a field missing from a record and a null one encode alike.
//! Every field starts with its own tag and every value with its kind's, and lengths are LEB128, so
//! no encoding is a prefix of another.
//!
//! Rows encode one at a time into one buffer, through encoders worked out once per batch: other
//! string, binary and array types are cast to the plain ones first, maps are arrays of their
//! entries, and dictionaries are their values.

#[cfg(test)]
mod tests;

use std::io::Write;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int64Array, ListArray,
    RecordBatch, StringArray, UInt64Array,
};
use arrow_buffer::NullBuffer;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, FieldRef};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use super::as_list;

/// The ids of `batch`'s rows as roots: of the `key` columns' values in order, or of the whole row
/// where there is no key.
///
/// A key column the batch lacks encodes as null.
pub(crate) fn root_ids(batch: &RecordBatch, key: &[Arc<str>]) -> Result<BinaryArray, ArrowError> {
    let schema = batch.schema();
    let encoders: Vec<Option<Encoder>> = if key.is_empty() {
        let fields = schema.fields().iter().zip(batch.columns());
        vec![Some(Encoder::object(None, fields)?)]
    } else {
        key.iter()
            .map(|column| {
                let Ok(index) = schema.index_of(column) else {
                    return Ok(None);
                };
                Encoder::new(schema.field(index), batch.column(index)).map(Some)
            })
            .collect::<Result<_, _>>()?
    };
    let mut row = Vec::with_capacity(ROW_BYTES);
    let ids: Vec<[u8; 16]> = (0..batch.num_rows())
        .map(|index| {
            row.clear();
            for encoder in &encoders {
                match encoder {
                    Some(encoder) => encoder.write(index, &mut row),
                    None => row.push(NULL),
                }
            }
            hash(&row)
        })
        .collect();
    Ok(BinaryArray::from_iter_values(ids))
}

/// The ids of child rows: of each row's parent's id and its index in the parent's array.
pub(crate) fn child_ids(parents: &BinaryArray, idx: &Int64Array) -> BinaryArray {
    let mut bytes = Vec::with_capacity(24);
    let ids: Vec<[u8; 16]> = parents
        .iter()
        .zip(idx.values())
        .map(|(parent, idx)| {
            bytes.clear();
            bytes.extend_from_slice(parent.unwrap_or_default());
            bytes.extend_from_slice(&idx.to_be_bytes());
            hash(&bytes)
        })
        .collect();
    BinaryArray::from_iter_values(ids)
}

fn hash(bytes: &[u8]) -> [u8; 16] {
    xxhash_rust::xxh3::xxh3_128(bytes).to_be_bytes()
}

/// The bytes a row's encoding is expected to take, which its buffer starts with.
const ROW_BYTES: usize = 512;

const NULL: u8 = b'n';
const TRUE: u8 = b't';
const FALSE: u8 = b'f';
const NUMBER: u8 = b'd';
const STRING: u8 = b's';
const BYTES: u8 = b'b';
const OTHER: u8 = b'x';
const OBJECT: u8 = b'{';
const OBJECT_END: u8 = b'}';
const ARRAY: u8 = b'[';
const ARRAY_END: u8 = b']';
const FIELD: u8 = b'k';

/// The Arrow field metadata key naming an extension type, and the JSON extension's name.
const EXTENSION_NAME: &str = "ARROW:extension:name";
const JSON_EXTENSION: &str = "arrow.json";

/// How one array's values encode, worked out once per batch.
enum Encoder {
    Null,
    Boolean(BooleanArray),
    Integer(Int64Array),
    Unsigned(UInt64Array),
    Float32(Float32Array),
    Float(Float64Array),
    Text(StringArray),
    /// JSON text, encoded as the values it renders.
    Json(StringArray),
    Bytes(BinaryArray),
    /// Decimals, as their text without trailing zeros after the point.
    Decimal(ArrayRef),
    /// An object: where it is null, and its fields in name order.
    Object(Option<NullBuffer>, Vec<(String, Encoder)>),
    /// An array, and its items.
    Array(ListArray, Box<Encoder>),
    /// Values of any other type, as their type and text.
    Other(ArrayRef, String),
}

impl Encoder {
    /// The encoder of `array`, whose field is `field`.
    fn new(field: &ArrowField, array: &ArrayRef) -> Result<Self, ArrowError> {
        let cast = |to: &DataType| arrow_cast::cast(array, to);
        let json = field.metadata().get(EXTENSION_NAME).map(String::as_str) == Some(JSON_EXTENSION);
        Ok(match array.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if json => {
                Self::Json(cast(&DataType::Utf8)?.as_string::<i32>().clone())
            }
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean(array.as_boolean().clone()),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => Self::Integer(cast(&DataType::Int64)?.as_primitive().clone()),
            DataType::UInt64 => Self::Unsigned(array.as_primitive().clone()),
            DataType::Float16 | DataType::Float32 => {
                Self::Float32(cast(&DataType::Float32)?.as_primitive().clone())
            }
            DataType::Float64 => Self::Float(array.as_primitive().clone()),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Self::Text(cast(&DataType::Utf8)?.as_string::<i32>().clone())
            }
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => {
                Self::Bytes(cast(&DataType::Binary)?.as_binary::<i32>().clone())
            }
            DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..) => Self::Decimal(Arc::clone(array)),
            DataType::Struct(_) => {
                let object = array.as_struct();
                let fields = object.fields().iter().zip(object.columns());
                Self::object(object.nulls().cloned(), fields)?
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::Map(..) => {
                let list = as_list(array)?;
                let item = match list.data_type() {
                    DataType::List(item) => Arc::clone(item),
                    other => Arc::new(ArrowField::new("item", other.clone(), true)),
                };
                let items = Self::new(&item, list.values())?;
                Self::Array(list, Box::new(items))
            }
            DataType::Dictionary(_, values) => Self::new(field, &cast(values)?)?,
            other => Self::Other(Arc::clone(array), other.to_string()),
        })
    }

    /// An object whose fields are `fields`, null where `nulls` says.
    fn object<'a>(
        nulls: Option<NullBuffer>,
        fields: impl Iterator<Item = (&'a FieldRef, &'a ArrayRef)>,
    ) -> Result<Self, ArrowError> {
        let mut fields = fields
            .map(|(field, values)| Ok((field.name().clone(), Self::new(field, values)?)))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        fields.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(Self::Object(nulls, fields))
    }

    fn is_null(&self, index: usize) -> bool {
        match self {
            Self::Null => true,
            Self::Boolean(values) => values.is_null(index),
            Self::Integer(values) => values.is_null(index),
            Self::Unsigned(values) => values.is_null(index),
            Self::Float32(values) => values.is_null(index),
            Self::Float(values) => values.is_null(index),
            Self::Text(values) | Self::Json(values) => values.is_null(index),
            Self::Bytes(values) => values.is_null(index),
            Self::Decimal(values) | Self::Other(values, _) => values.is_null(index),
            Self::Object(nulls, _) => nulls.as_ref().is_some_and(|nulls| nulls.is_null(index)),
            Self::Array(list, _) => list.is_null(index),
        }
    }

    /// Appends the encoding of the value at `index` to `out`.
    fn write(&self, index: usize, out: &mut Vec<u8>) {
        if self.is_null(index) {
            out.push(NULL);
            return;
        }
        match self {
            Self::Null => out.push(NULL),
            Self::Boolean(values) => out.push(if values.value(index) { TRUE } else { FALSE }),
            Self::Integer(values) => integer(out, values.value(index).into()),
            Self::Unsigned(values) => integer(out, values.value(index).into()),
            Self::Float32(values) => float32(out, values.value(index)),
            Self::Float(values) => float64(out, values.value(index)),
            Self::Text(values) => {
                out.push(STRING);
                length(out, values.value(index).as_bytes());
            }
            Self::Bytes(values) => {
                out.push(BYTES);
                length(out, values.value(index));
            }
            Self::Decimal(values) => {
                let text = formatted(values, index);
                let text = if text.contains('.') {
                    text.trim_end_matches('0').trim_end_matches('.')
                } else {
                    text.as_str()
                };
                number(out, |row| row.write_all(text.as_bytes()));
            }
            Self::Json(values) => json(values.value(index), out),
            Self::Object(_, fields) => {
                out.push(OBJECT);
                for (name, field) in fields {
                    if !field.is_null(index) {
                        out.push(FIELD);
                        length(out, name.as_bytes());
                        field.write(index, out);
                    }
                }
                out.push(OBJECT_END);
            }
            Self::Array(list, items) => {
                out.push(ARRAY);
                let offsets = list.value_offsets();
                for item in offsets[index]..offsets[index + 1] {
                    items.write(usize::try_from(item).unwrap_or(usize::MAX), out);
                }
                out.push(ARRAY_END);
            }
            Self::Other(values, kind) => {
                out.push(OTHER);
                length(out, kind.as_bytes());
                length(out, formatted(values, index).as_bytes());
            }
        }
    }
}

/// Appends the encoding of the values the JSON `text` renders, or of the text where it is not
/// JSON.
fn json(text: &str, out: &mut Vec<u8>) {
    if let Ok(value) = sonic_rs::from_str::<sonic_rs::Value>(text) {
        json_value(&value, out);
    } else {
        out.push(STRING);
        length(out, text.as_bytes());
    }
}

/// Appends the encoding of `value`, as the encoding of the Arrow value holding it would be.
fn json_value(value: &sonic_rs::Value, out: &mut Vec<u8>) {
    if let Some(boolean) = value.as_bool() {
        out.push(if boolean { TRUE } else { FALSE });
    } else if let Some(integer) = value.as_i64() {
        self::integer(out, integer.into());
    } else if let Some(integer) = value.as_u64() {
        self::integer(out, integer.into());
    } else if let Some(float) = value.as_f64() {
        float64(out, float);
    } else if let Some(text) = value.as_str() {
        out.push(STRING);
        length(out, text.as_bytes());
    } else if let Some(items) = value.as_array() {
        out.push(ARRAY);
        for item in items {
            json_value(item, out);
        }
        out.push(ARRAY_END);
    } else if let Some(object) = value.as_object() {
        let mut fields: Vec<(&str, &sonic_rs::Value)> = object
            .iter()
            .filter(|(_, field)| !field.is_null())
            .collect();
        fields.sort_by_key(|(name, _)| *name);
        out.push(OBJECT);
        for (name, field) in fields {
            out.push(FIELD);
            length(out, name.as_bytes());
            json_value(field, out);
        }
        out.push(OBJECT_END);
    } else {
        out.push(NULL);
    }
}

/// The text of the value at `index` of `values`, as Arrow displays it.
fn formatted(values: &ArrayRef, index: usize) -> String {
    ArrayFormatter::try_new(values.as_ref(), &FormatOptions::default()).map_or_else(
        |error| error.to_string(),
        |format| format.value(index).to_string(),
    )
}

/// Appends an integer.
fn integer(out: &mut Vec<u8>, value: i128) {
    out.push(NUMBER);
    digits(out, value);
    out.push(b';');
}

/// Appends the decimal digits of `value`, with a minus sign where it is negative.
fn digits(row: &mut Vec<u8>, value: i128) {
    let mut buffer = [0_u8; 40];
    let mut at = buffer.len();
    let mut rest = value.unsigned_abs();
    loop {
        at -= 1;
        buffer[at] = b'0' + u8::try_from(rest % 10).unwrap_or(0);
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    if value < 0 {
        at -= 1;
        buffer[at] = b'-';
    }
    row.extend_from_slice(&buffer[at..]);
}

/// Appends a number, which `digits` writes.
fn number(row: &mut Vec<u8>, digits: impl FnOnce(&mut Vec<u8>) -> std::io::Result<()>) {
    row.push(NUMBER);
    digits(row).expect("writing to a vector never fails");
    row.push(b';');
}

/// Floats render as their shortest round-trip text, which for an integral float is the integer;
/// negative zero renders as zero.
fn float64(row: &mut Vec<u8>, value: f64) {
    if value == 0.0 {
        number(row, |row| row.write_all(b"0"));
    } else {
        number(row, |row| write!(row, "{value}"));
    }
}

fn float32(row: &mut Vec<u8>, value: f32) {
    if value == 0.0 {
        number(row, |row| row.write_all(b"0"));
    } else {
        number(row, |row| write!(row, "{value}"));
    }
}

/// Appends `bytes` after their length, in LEB128.
fn length(row: &mut Vec<u8>, bytes: &[u8]) {
    let mut rest = bytes.len();
    while rest >= 0x80 {
        // Seven bits of the length, and the high bit saying more follow.
        row.push(u8::try_from(0x80 + (rest & 0x7f)).unwrap_or(u8::MAX));
        rest >>= 7;
    }
    row.push(u8::try_from(rest).unwrap_or(0));
    row.extend_from_slice(bytes);
}

//! Hashed identity: a merge stream's root row's `_rdlt_id`, the BLAKE3 hash of a canonical
//! encoding of its key, and each history version's `_rdlt_row_hash`, of its whole row.
//!
//! Record authors choose both inputs, so the hash is BLAKE3's whole 256 bits: no two rows anyone
//! can find share one, however their values were chosen. Each kind hashes its own domain's tag
//! first, so hashes of different kinds never come from one input.
//!
//! The encoding tags every value with its kind and renders every number one way, so a value hashes
//! alike whichever batch, chunk or Arrow type carries it: integers as their digits, floats as
//! the shortest round-trip text of the 64-bit float they widen to, which for an integral float is
//! its digits too. JSON text, as a column whose values mix types holds it, encodes as the values
//! it renders. Objects list their non-null fields in name order, so a field missing from a record
//! and a null one encode alike. Every field starts with its own tag and every value with its
//! kind's, and every name, text, byte string and number with its length in LEB128, so no encoding
//! is a prefix of another.
//!
//! Rows encode one at a time into one buffer, through encoders worked out once per batch: other
//! string, binary and array types are cast to the plain ones first, maps are arrays of their
//! entries, and dictionaries and run-end encodings are their values. An interval or a union, which
//! no table holds, has no encoder: a normalized flush holding one is refused before it is split.
//! Every value but a JSON text's is written into the row's buffer where it goes, its digits
//! included.
//! Dates, times, timestamps and durations encode as their kind and nanoseconds, so a value's unit,
//! zone or date type does not change its encoding; a `Date64` holding part of a day is the day it
//! is within.
//!
//! JSON text is read a token at a time, never recursed into, and its numbers are read exactly:
//! each encodes as the canonical text of its value, as an integer, a decimal or a float of that
//! value does. Text that is not JSON fails the encoding; a column of JSON holds none once its
//! push is checked.

mod canonical;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, ListArray, RecordBatch, StringArray,
    UInt64Array,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, FieldRef};
use rdlt_connector::{LogicalType, instants};

use super::as_list;
use crate::json::{JsonError, Reader, Token, write_float, write_number};
use crate::table::convert::decoded;
use canonical::{DIGITS, Stored, decimal_scale, digits, temporal_tag};

/// The ids of `batch`'s rows as a merge stream's roots: of the `key` columns' values in order.
///
/// # Errors
///
/// A key column the batch lacks, which a stream's keys are checked for before, and a value of a
/// column of JSON identity cannot read.
pub(crate) fn root_ids(batch: &RecordBatch, key: &[Arc<str>]) -> Result<BinaryArray, ArrowError> {
    let schema = batch.schema();
    let encoders: Vec<Encoder> = key
        .iter()
        .map(|column| {
            let index = schema.index_of(column)?;
            Encoder::new(schema.field(index), batch.column(index))
        })
        .collect::<Result<_, _>>()?;
    hashes(Domain::Root, batch.num_rows(), &encoders)
}

/// The hashes of `rows` rows as history versions: of each whole row, its values in `columns`,
/// each column beside its field.
pub(crate) fn version_hashes<'a>(
    columns: impl Iterator<Item = (&'a FieldRef, &'a ArrayRef)>,
    rows: usize,
) -> Result<BinaryArray, ArrowError> {
    hashes(Domain::Version, rows, &[Encoder::object(None, columns)?])
}

/// The hashes of `batch`'s rows as history versions.
#[cfg(test)]
pub(crate) fn batch_version_hashes(batch: &RecordBatch) -> Result<BinaryArray, ArrowError> {
    let columns = batch.schema_ref().fields().iter().zip(batch.columns());
    version_hashes(columns, batch.num_rows())
}

/// What a hash identifies, whose tag its input starts with.
#[derive(Clone, Copy, Debug)]
enum Domain {
    /// A merge stream's root row, by its key.
    Root = 0x01,
    /// A history version, by its data.
    Version = 0x03,
}

/// The hashes in `domain` of `rows` rows, each of what `encoders` write of it in turn.
fn hashes(domain: Domain, rows: usize, encoders: &[Encoder]) -> Result<BinaryArray, ArrowError> {
    let mut row = Vec::with_capacity(ROW_BYTES);
    let mut values = Vec::with_capacity(rows * ID_BYTES);
    for index in 0..rows {
        row.clear();
        for encoder in encoders {
            encoder.write(index, &mut row)?;
        }
        values.extend_from_slice(&hash(domain, &row));
    }
    Ok(ids(values))
}

/// The refusal behind `error`, where identity could not read a value of a column of JSON.
pub(crate) fn json_refusal(error: &ArrowError) -> Option<&JsonError> {
    match error {
        ArrowError::ExternalError(source) => source.downcast_ref::<JsonError>(),
        _ => None,
    }
}

/// The error for a batch of `stream` identity could not read, while `doing` something: typed
/// where a value of a column of JSON was refused.
pub(crate) fn unread(
    stream: &rdlt_connector::StreamName,
    doing: &str,
    error: &ArrowError,
) -> crate::Error {
    match json_refusal(error) {
        Some(refused) => crate::Error::new(
            crate::ErrorKind::Source,
            format!("stream {stream}: {doing}: a value of a column of JSON: {refused}"),
        )
        .with_code(refused.code())
        .with_stream(stream),
        None => crate::Error::internal(format!("stream {stream}: {doing}: {error}")),
    }
}

/// Bytes in an id: BLAKE3's whole output.
const ID_BYTES: usize = blake3::OUT_LEN;

/// The BLAKE3 hash of `bytes` in `domain`.
fn hash(domain: Domain, bytes: &[u8]) -> [u8; ID_BYTES] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[domain as u8]);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

/// The ids written one after another into `values`, as an array of them.
fn ids(values: Vec<u8>) -> BinaryArray {
    let offsets = OffsetBuffer::from_repeated_length(ID_BYTES, values.len() / ID_BYTES);
    BinaryArray::new(offsets, Buffer::from_vec(values), None)
}

/// The bytes a row's encoding is expected to take, which its buffer starts with.
const ROW_BYTES: usize = 512;

const NULL: u8 = b'n';
const TRUE: u8 = b't';
const FALSE: u8 = b'f';
const NUMBER: u8 = b'd';
const STRING: u8 = b's';
const BYTES: u8 = b'b';
const INSTANT: u8 = b'i';
const TIME_OF_DAY: u8 = b'c';
const ELAPSED: u8 = b'e';
const OBJECT: u8 = b'{';
const OBJECT_END: u8 = b'}';
const ARRAY: u8 = b'[';
const ARRAY_END: u8 = b']';
const FIELD: u8 = b'k';

/// How one array's values encode, worked out once per batch.
enum Encoder {
    Null,
    Boolean(BooleanArray),
    /// Integers of any type a signed 64-bit one holds, read as their type stores them.
    Integer(ArrayRef, Stored),
    Unsigned(UInt64Array),
    /// Floats of any width, as the 64-bit floats they widen to.
    Float(Float64Array),
    Text(StringArray),
    /// JSON text, encoded as the values it renders.
    Json(StringArray),
    Bytes(BinaryArray),
    /// Decimals of this scale, as their text without trailing zeros after the point.
    Decimal(ArrayRef, Stored, i8),
    /// An object: where it is null, and its fields in name order.
    Object(Option<NullBuffer>, Vec<(String, Encoder)>),
    /// An array, and its items.
    Array(ListArray, Box<Encoder>),
    /// Dates, timestamps, times of day or durations: their kind's tag, and the values as their
    /// type stores them, encoded as the nanoseconds they denote.
    Temporal(u8, ArrayRef, Stored),
}

impl Encoder {
    /// The encoder of `array`, whose field is `field`.
    fn new(field: &ArrowField, array: &ArrayRef) -> Result<Self, ArrowError> {
        let cast = |to: &DataType| arrow_cast::cast(array, to);
        let stored = || {
            Stored::of(array.as_ref()).ok_or_else(|| {
                ArrowError::CastError(format!("{} holds no stored values", array.data_type()))
            })
        };
        let json = rdlt_connector::Field::extension_type(field) == Some(LogicalType::Json);
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
            | DataType::UInt32 => Self::Integer(Arc::clone(array), stored()?),
            DataType::UInt64 => Self::Unsigned(array.as_primitive().clone()),
            // Every 16- and 32-bit float widens to a 64-bit float exactly.
            DataType::Float16 | DataType::Float32 => {
                Self::Float(cast(&DataType::Float64)?.as_primitive().clone())
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
            data_type if decimal_scale(data_type).is_some() => {
                let scale = decimal_scale(data_type).expect("a decimal type");
                Self::Decimal(Arc::clone(array), stored()?, scale)
            }
            data_type if temporal_tag(data_type).is_some() => {
                let tag = temporal_tag(data_type).expect("a temporal type");
                Self::Temporal(tag, Arc::clone(array), stored()?)
            }
            DataType::Struct(_) => {
                let object = array.as_struct();
                let fields = object.fields().iter().zip(object.columns());
                Self::object(object.nulls().cloned(), fields)?
            }
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Map(..) => Self::array(array)?,
            // Decoded by the values the rows name, never the whole dictionary or every run.
            DataType::Dictionary(..) | DataType::RunEndEncoded(..) => {
                Self::new(field, &decoded(array)?)?
            }
            other => return Err(no_table_holds(other)),
        })
    }

    /// An array of the items of `array`, a list of any kind or a map.
    fn array(array: &ArrayRef) -> Result<Self, ArrowError> {
        let list = as_list(array)?;
        let item = match list.data_type() {
            DataType::List(item) => Arc::clone(item),
            other => Arc::new(ArrowField::new("item", other.clone(), true)),
        };
        let items = Self::new(&item, list.values())?;
        Ok(Self::Array(list, Box::new(items)))
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
            Self::Integer(values, _)
            | Self::Decimal(values, ..)
            | Self::Temporal(_, values, ..) => values.is_null(index),
            Self::Unsigned(values) => values.is_null(index),
            Self::Float(values) => values.is_null(index),
            Self::Text(values) | Self::Json(values) => values.is_null(index),
            Self::Bytes(values) => values.is_null(index),
            Self::Object(nulls, _) => nulls.as_ref().is_some_and(|nulls| nulls.is_null(index)),
            Self::Array(list, _) => list.is_null(index),
        }
    }

    /// Appends the encoding of the value at `index` to `out`.
    ///
    /// # Errors
    ///
    /// Where the value is JSON text that is not JSON, or nests deeper than the limit.
    fn write(&self, index: usize, out: &mut Vec<u8>) -> Result<(), ArrowError> {
        if self.is_null(index) {
            out.push(NULL);
            return Ok(());
        }
        match self {
            Self::Null => out.push(NULL),
            Self::Boolean(values) => out.push(if values.value(index) { TRUE } else { FALSE }),
            Self::Integer(_, values) => integer(out, NUMBER, values.value(index)),
            Self::Unsigned(values) => integer(out, NUMBER, values.value(index).into()),
            Self::Float(values) => float64(out, values.value(index)),
            Self::Text(values) => {
                out.push(STRING);
                length(out, values.value(index).as_bytes());
            }
            Self::Bytes(values) => {
                out.push(BYTES);
                length(out, values.value(index));
            }
            Self::Decimal(_, values, scale) => {
                out.push(NUMBER);
                length_of(out, |out| values.write_decimal(*scale, index, out));
            }
            Self::Temporal(tag, array, values) => {
                let nanos = instants::nanos(array.data_type(), values.value(index));
                integer(out, *tag, nanos.unwrap_or_default());
            }
            Self::Json(values) => json(values.value(index), out)
                .map_err(|error| ArrowError::ExternalError(Box::new(error)))?,
            Self::Object(_, fields) => {
                out.push(OBJECT);
                for (name, field) in fields {
                    if !field.is_null(index) {
                        out.push(FIELD);
                        length(out, name.as_bytes());
                        field.write(index, out)?;
                    }
                }
                out.push(OBJECT_END);
            }
            Self::Array(list, items) => {
                out.push(ARRAY);
                let offsets = list.value_offsets();
                for item in offsets[index]..offsets[index + 1] {
                    items.write(usize::try_from(item).unwrap_or(usize::MAX), out)?;
                }
                out.push(ARRAY_END);
            }
        }
        Ok(())
    }
}

/// A container of JSON text being encoded: an array's encoding so far, or an object's members
/// encoded so far and the key of the member being read.
enum Open {
    Array(Vec<u8>),
    Object {
        members: Vec<(String, Vec<u8>)>,
        key: String,
    },
}

/// Appends the encoding of the value the JSON `text` holds, as the encoding of the Arrow value
/// holding it would be: its numbers exactly, as their canonical text.
///
/// The text is read a token at a time, so it is never recursed into however deep it nests.
fn json(text: &str, out: &mut Vec<u8>) -> Result<(), JsonError> {
    let mut reader = Reader::new(text);
    let mut open: Vec<Open> = Vec::new();
    while let Some(token) = reader.next()? {
        match token {
            Token::Null => place(&mut open, out, None),
            Token::Bool(value) => place(&mut open, out, Some(&[if value { TRUE } else { FALSE }])),
            Token::Number(written) => {
                let mut text = Vec::new();
                write_number(written, &mut text)?;
                let mut number = vec![NUMBER];
                length(&mut number, &text);
                place(&mut open, out, Some(&number));
            }
            Token::String(text) => {
                let mut string = vec![STRING];
                length(&mut string, text.unescaped().as_bytes());
                place(&mut open, out, Some(&string));
            }
            Token::Key(name) => {
                if let Some(Open::Object { key, .. }) = open.last_mut() {
                    *key = name.unescaped().into_owned();
                }
            }
            Token::BeginObject => open.push(Open::Object {
                members: Vec::new(),
                key: String::new(),
            }),
            Token::BeginArray => open.push(Open::Array(vec![ARRAY])),
            Token::EndObject | Token::EndArray => {
                let encoded = match open.pop() {
                    Some(Open::Array(mut items)) => {
                        items.push(ARRAY_END);
                        items
                    }
                    Some(Open::Object { mut members, .. }) => object(&mut members),
                    None => return Err(JsonError::Invalid("a container closes that never opened")),
                };
                place(&mut open, out, Some(&encoded));
            }
        }
    }
    Ok(())
}

/// Places the encoding of a value read, `None` for a null, where the container open takes it:
/// an object leaves out a null member, as a struct's encoding leaves out a null field.
fn place(open: &mut [Open], out: &mut Vec<u8>, encoded: Option<&[u8]>) {
    match (open.last_mut(), encoded) {
        (None, encoded) => out.extend_from_slice(encoded.unwrap_or(&[NULL])),
        (Some(Open::Array(items)), encoded) => items.extend_from_slice(encoded.unwrap_or(&[NULL])),
        (Some(Open::Object { .. }), None) => {}
        (Some(Open::Object { members, key }), Some(encoded)) => {
            members.push((std::mem::take(key), encoded.to_vec()));
        }
    }
}

/// The encoding of an object of `members`, in name order.
fn object(members: &mut [(String, Vec<u8>)]) -> Vec<u8> {
    members.sort_by(|(left, _), (right, _)| left.cmp(right));
    let mut encoded = vec![OBJECT];
    for (name, value) in members.iter() {
        encoded.push(FIELD);
        length(&mut encoded, name.as_bytes());
        encoded.extend_from_slice(value);
    }
    encoded.push(OBJECT_END);
    encoded
}

/// The refusal of a value of `data_type`, which identity has no encoder for.
fn no_table_holds(data_type: &DataType) -> ArrowError {
    ArrowError::InvalidArgumentError(format!(
        "identity encodes no value of {data_type}, which no table holds"
    ))
}

/// Appends `tag` and an integer, as its decimal digits after their length, with a minus sign
/// where it is negative.
fn integer(row: &mut Vec<u8>, tag: u8, value: i128) {
    let mut buffer = [0_u8; DIGITS];
    let (negative, digits) = digits(value, &mut buffer);
    row.push(tag);
    leb128(row, usize::from(negative) + digits.len());
    if negative {
        row.push(b'-');
    }
    row.extend_from_slice(digits);
}

/// Floats encode as the canonical text of the shortest text that reads back as them, a tie
/// going to the even one as JSON writers break it, which is the text the engine writes a float
/// into JSON as, so the float and that text hash alike; negative zero is zero.
fn float64(row: &mut Vec<u8>, value: f64) {
    row.push(NUMBER);
    length_of(row, |row| write_float(value, row));
}

/// Appends `bytes` after their length, in LEB128.
fn length(row: &mut Vec<u8>, bytes: &[u8]) {
    leb128(row, bytes.len());
    row.extend_from_slice(bytes);
}

/// Appends what `write` appends after its length, in LEB128: written where it goes, after a
/// byte for its length, which a text of 128 bytes or more widens.
fn length_of(row: &mut Vec<u8>, write: impl FnOnce(&mut Vec<u8>)) {
    let at = row.len();
    row.push(0);
    write(row);
    let written = row.len() - at - 1;
    match u8::try_from(written) {
        Ok(short) if short < 0x80 => row[at] = short,
        // Splicing moves what was written along by the bytes the length grew.
        _ => {
            let mut prefix = [0_u8; 10];
            let mut count = 0;
            let mut rest = written;
            while rest >= 0x80 {
                prefix[count] = u8::try_from(0x80 + (rest & 0x7f)).unwrap_or(u8::MAX);
                rest >>= 7;
                count += 1;
            }
            prefix[count] = u8::try_from(rest).unwrap_or(0);
            row.splice(at..=at, prefix[..=count].iter().copied());
        }
    }
}

/// Appends `count` in LEB128: seven bits a byte, lowest first, the high bit saying more follow.
fn leb128(row: &mut Vec<u8>, mut count: usize) {
    while count >= 0x80 {
        row.push(u8::try_from(0x80 + (count & 0x7f)).unwrap_or(u8::MAX));
        count >>= 7;
    }
    row.push(u8::try_from(count).unwrap_or(0));
}

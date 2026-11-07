//! Masking values: an `HMAC-SHA256` of each in lower-case hex, a redaction, or null; a nested
//! struct field is rebuilt with its siblings shared.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray as _;
use arrow_array::{Array, ArrayRef, NullArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use hmac::{Hmac, Mac as _};
use rdlt_connector::{ColumnPath, Secret};
use sha2::Sha256;

use super::op::{ARRAY_ROOM, fields, text_room};
use super::{Bound, TransformError};

/// How a mask replaces each value.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mask {
    /// The `HMAC-SHA256` of the value's bytes under the key, as 64 lower-case hex digits.
    HmacSha256(Arc<Secret<String>>),
    /// The text, for every value that is not null.
    Redact(String),
    /// Null, in a column of the null type.
    Null,
}

impl Mask {
    /// Masks each value with its `HMAC-SHA256` under `key`.
    pub fn hmac_sha256(key: Secret<String>) -> Self {
        Self::HmacSha256(Arc::new(key))
    }

    /// Masks each value that is not null with `text`.
    pub fn redact(text: impl Into<String>) -> Self {
        Self::Redact(text.into())
    }
}

/// Bytes: an `HMAC-SHA256` in hex, its offset and its validity, a row.
const HEXED_ROW: u64 = 64 + 4 + 1;

/// Where `path` sits in `schema`: its top-level column's index, then each nested field's; none
/// where the schema lacks it.
pub(super) fn place(
    schema: &Schema,
    path: &ColumnPath,
) -> Result<Option<Vec<usize>>, TransformError> {
    let mut segments = path.segments();
    let Some(root) = segments.next() else {
        return Ok(None);
    };
    let Ok(at) = schema.index_of(root) else {
        return Ok(None);
    };
    let mut place = vec![at];
    let mut field = schema.field(at);
    for segment in segments {
        let DataType::Struct(children) = field.data_type() else {
            let refused = TransformError::new("a mask reaches nested fields only through structs");
            return Err(refused.with_column(path.to_string()));
        };
        let Some((index, child)) = children.find(segment) else {
            return Ok(None);
        };
        place.push(index);
        field = child;
    }
    Ok(Some(place))
}

fn hashable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => true,
        DataType::Dictionary(_, values) => hashable(values),
        _ => false,
    }
}

pub(super) fn output(
    input: &Schema,
    columns: &[ColumnPath],
    with: &Mask,
) -> Result<SchemaRef, TransformError> {
    let mut fields = fields(input);
    for path in columns {
        let Some(place) = place(input, path)? else {
            continue;
        };
        let (root, below) = place
            .split_first()
            .expect("a place starts at a top-level column");
        fields[*root] = Arc::new(masked_field(&fields[*root], below, with, path)?);
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input.metadata().clone(),
    )))
}

/// `field` without the extension type its metadata names, which a masked value below it no
/// longer matches.
fn plain(field: &Field) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.remove("ARROW:extension:name");
    metadata.remove("ARROW:extension:metadata");
    field.clone().with_metadata(metadata)
}

/// `field` holding masked values of `data_type`: nullable, and no longer an extension type.
fn masked_leaf(field: &Field, data_type: DataType) -> Field {
    plain(field).with_data_type(data_type).with_nullable(true)
}

fn masked_field(
    field: &Field,
    below: &[usize],
    with: &Mask,
    path: &ColumnPath,
) -> Result<Field, TransformError> {
    let Some((child, rest)) = below.split_first() else {
        let data_type = match with {
            Mask::HmacSha256(_) if !hashable(field.data_type()) => {
                let refused = TransformError::new("an `HMAC-SHA256` mask takes text or bytes");
                return Err(refused.with_column(path.to_string()));
            }
            Mask::HmacSha256(_) | Mask::Redact(_) => DataType::Utf8,
            Mask::Null => DataType::Null,
        };
        return Ok(masked_leaf(field, data_type));
    };
    let DataType::Struct(children) = field.data_type() else {
        let refused = TransformError::new("a mask reaches nested fields only through structs");
        return Err(refused.with_column(path.to_string()));
    };
    let mut children: Vec<FieldRef> = children.iter().cloned().collect();
    children[*child] = Arc::new(masked_field(&children[*child], rest, with, path)?);
    Ok(plain(field).with_data_type(DataType::Struct(Fields::from(children))))
}

pub(super) fn apply(
    batch: &RecordBatch,
    columns: &[ColumnPath],
    with: &Mask,
) -> Result<Vec<ArrayRef>, TransformError> {
    let mut arrays = batch.columns().to_vec();
    for path in columns {
        let Some(place) = place(batch.schema_ref(), path)? else {
            continue;
        };
        let (root, below) = place
            .split_first()
            .expect("a place starts at a top-level column");
        arrays[*root] = masked_at(&arrays[*root], below, with, path)?;
    }
    Ok(arrays)
}

fn masked_at(
    array: &ArrayRef,
    below: &[usize],
    with: &Mask,
    path: &ColumnPath,
) -> Result<ArrayRef, TransformError> {
    let Some((child, rest)) = below.split_first() else {
        return masked(array.as_ref(), with, path);
    };
    let (fields, mut children, nulls) = array.as_struct().clone().into_parts();
    let replaced = masked_at(&children[*child], rest, with, path)?;
    let mut fields: Vec<FieldRef> = fields.iter().cloned().collect();
    let field = fields[*child].as_ref();
    let data_type = replaced.data_type().clone();
    fields[*child] = Arc::new(if rest.is_empty() {
        masked_leaf(field, data_type)
    } else {
        plain(field).with_data_type(data_type)
    });
    children[*child] = replaced;
    let rebuilt = StructArray::try_new(Fields::from(fields), children, nulls).map_err(|_| {
        TransformError::new("a masked struct cannot be rebuilt").with_column(path.to_string())
    })?;
    Ok(Arc::new(rebuilt))
}

fn masked(array: &dyn Array, with: &Mask, path: &ColumnPath) -> Result<ArrayRef, TransformError> {
    match with {
        Mask::HmacSha256(key) => hashed(array, key.expose().as_bytes(), path),
        Mask::Redact(text) => redacted(array, text, path),
        Mask::Null => Ok(Arc::new(NullArray::new(array.len()))),
    }
}

fn hashed(array: &dyn Array, key: &[u8], path: &ColumnPath) -> Result<ArrayRef, TransformError> {
    let room = text_room(array.len(), 64, &path.to_string())?;
    if let Some(dictionary) = array.as_any_dictionary_opt() {
        let values = hashed(dictionary.values().as_ref(), key, path)?;
        return arrow_select::take::take(values.as_ref(), dictionary.keys(), None).map_err(|_| {
            TransformError::new("a masked dictionary cannot be taken").with_column(path.to_string())
        });
    }
    let keyed = Hmac::<Sha256>::new_from_slice(key).expect("an HMAC takes a key of any length");
    let rows = array.len();
    let mut out = StringBuilder::with_capacity(rows, room);
    let mut digits = [0_u8; 64];
    let mut push = |bytes: Option<&[u8]>| match bytes {
        Some(bytes) => {
            let mut mac = keyed.clone();
            mac.update(bytes);
            hex(&mac.finalize().into_bytes(), &mut digits);
            out.append_value(std::str::from_utf8(&digits).expect("hex digits are ASCII"));
        }
        None => out.append_null(),
    };
    each_bytes(array, &mut push, path)?;
    Ok(Arc::new(out.finish()))
}

/// Calls `push` with the bytes of each row of a text or binary `array`, none for a null.
fn each_bytes(
    array: &dyn Array,
    push: &mut impl FnMut(Option<&[u8]>),
    path: &ColumnPath,
) -> Result<(), TransformError> {
    match array.data_type() {
        DataType::Utf8 => array
            .as_string::<i32>()
            .iter()
            .for_each(|value| push(value.map(str::as_bytes))),
        DataType::LargeUtf8 => array
            .as_string::<i64>()
            .iter()
            .for_each(|value| push(value.map(str::as_bytes))),
        DataType::Utf8View => array
            .as_string_view()
            .iter()
            .for_each(|value| push(value.map(str::as_bytes))),
        DataType::Binary => array.as_binary::<i32>().iter().for_each(push),
        DataType::LargeBinary => array.as_binary::<i64>().iter().for_each(push),
        DataType::BinaryView => array.as_binary_view().iter().for_each(push),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().iter().for_each(push),
        _ => {
            let refused = TransformError::new("an `HMAC-SHA256` mask takes text or bytes");
            return Err(refused.with_column(path.to_string()));
        }
    }
    Ok(())
}

/// Writes `digest` into `digits` as lower-case hex.
fn hex(digest: &[u8], digits: &mut [u8; 64]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for (index, byte) in digest.iter().enumerate() {
        digits[2 * index] = DIGITS[usize::from(byte >> 4)];
        digits[2 * index + 1] = DIGITS[usize::from(byte & 0x0f)];
    }
}

fn redacted(array: &dyn Array, text: &str, path: &ColumnPath) -> Result<ArrayRef, TransformError> {
    let rows = array.len();
    let room = text_room(rows, text.len(), &path.to_string())?;
    let nulls = array.logical_nulls();
    let mut out = StringBuilder::with_capacity(rows, room);
    for row in 0..rows {
        if nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row)) {
            out.append_value(text);
        } else {
            out.append_null();
        }
    }
    Ok(Arc::new(out.finish()))
}

pub(super) fn bound(input: &Schema, columns: &[ColumnPath], with: &Mask) -> Bound {
    columns.iter().fold(Bound::new(0), |bound, path| {
        let Ok(Some(place)) = place(input, path) else {
            return bound;
        };
        let mut field = input.field(place[0]);
        for index in &place[1..] {
            let DataType::Struct(children) = field.data_type() else {
                return bound;
            };
            field = &children[*index];
        }
        bound.plus(&leaf_bound(with, place[0], field.data_type()))
    })
}

fn leaf_bound(with: &Mask, root: usize, leaf: &DataType) -> Bound {
    match with {
        Mask::HmacSha256(_) if matches!(leaf, DataType::Dictionary(..)) => {
            Bound::new(2 * ARRAY_ROOM)
                .with_rows(HEXED_ROW)
                .with_column(root, HEXED_ROW)
        }
        Mask::HmacSha256(_) => Bound::new(ARRAY_ROOM).with_rows(HEXED_ROW),
        Mask::Redact(text) => {
            let bytes = u64::try_from(text.len()).unwrap_or(u64::MAX);
            Bound::new(ARRAY_ROOM).with_rows(bytes.saturating_add(5))
        }
        Mask::Null => Bound::new(0),
    }
}

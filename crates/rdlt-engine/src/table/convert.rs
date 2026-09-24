//! Converting arrays between logical types and into the types a destination stores.
//!
//! Every conversion is exact: a column's type is the join of every type it received, so it holds
//! each incoming value as it is, and text or JSON renderings keep every value.

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, StructArray, new_null_array,
};
use arrow_json::writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder};
use arrow_schema::{ArrowError, DataType, FieldRef};
use rdlt_connector::{Field, LogicalType};

/// The Arrow field metadata key naming an extension type.
const EXTENSION_NAME: &str = "ARROW:extension:name";

/// `array`, holding values of `from`, as `to`, which holds every value of `from`.
pub(crate) fn convert(
    array: &ArrayRef,
    from: &LogicalType,
    to: &LogicalType,
) -> Result<ArrayRef, ArrowError> {
    if from == to {
        return normalize(array, to);
    }
    match (from, to) {
        (_, LogicalType::Json) => json(array, from),
        (LogicalType::Struct(from_fields), LogicalType::Struct(to_fields)) => {
            let source = normalize(array, from)?;
            let source = source.as_struct();
            let columns = to_fields
                .iter()
                .map(|field| {
                    match from_fields
                        .iter()
                        .position(|candidate| candidate.name() == field.name())
                    {
                        Some(index) => convert(
                            source.column(index),
                            from_fields
                                .iter()
                                .nth(index)
                                .expect("the position came from these fields")
                                .logical_type(),
                            field.logical_type(),
                        ),
                        None => Ok(new_null_array(
                            &field.logical_type().to_arrow(),
                            source.len(),
                        )),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let DataType::Struct(fields) = to.to_arrow() else {
                unreachable!("struct types are Arrow structs")
            };
            Ok(Arc::new(StructArray::try_new(
                fields,
                columns,
                source.nulls().cloned(),
            )?))
        }
        (LogicalType::List(from_item), LogicalType::List(to_item)) => {
            let source = normalize(array, from)?;
            list(source.as_list::<i32>(), from_item, to_item)
        }
        _ => cast(array, &to.to_arrow()),
    }
}

/// `array` in the plain Arrow type of `logical`: large, view and dictionary encodings, maps and
/// wider integer storage come out as the type the logical type names.
fn normalize(array: &ArrayRef, logical: &LogicalType) -> Result<ArrayRef, ArrowError> {
    let target = logical.to_arrow();
    if *array.data_type() == target {
        Ok(Arc::clone(array))
    } else {
        cast(array, &target)
    }
}

/// `array` as `target`, failing on any value `target` cannot represent instead of nulling it.
fn cast(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ArrowError> {
    let options = arrow_cast::CastOptions {
        safe: false,
        ..arrow_cast::CastOptions::default()
    };
    arrow_cast::cast_with_options(array, target, &options)
}

fn list<O: OffsetSizeTrait>(
    source: &GenericListArray<O>,
    from: &Field,
    to: &Field,
) -> Result<ArrayRef, ArrowError> {
    let values = convert(source.values(), from.logical_type(), to.logical_type())?;
    let DataType::List(field) = LogicalType::List(Box::new(to.clone())).to_arrow() else {
        unreachable!("list types are Arrow lists")
    };
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        source.offsets().clone(),
        values,
        source.nulls().cloned(),
    )?))
}

/// Each value of `array`, of `logical`, as JSON text; nulls stay null.
pub(crate) fn json(array: &ArrayRef, logical: &LogicalType) -> Result<ArrayRef, ArrowError> {
    if *logical == LogicalType::Json {
        return normalize(array, logical);
    }
    let field: FieldRef = Arc::new(Field::new("value", logical.clone(), true).to_arrow());
    let array = normalize(array, logical)?;
    let options = EncoderOptions::default()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(Extensions));
    let mut encoder = make_encoder(&field, array.as_ref(), &options)?;
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 8);
    let mut buffer = Vec::new();
    for row in 0..array.len() {
        if array.is_null(row) {
            builder.append_null();
            continue;
        }
        buffer.clear();
        encoder.encode(row, &mut buffer);
        builder.append_value(String::from_utf8_lossy(&buffer));
    }
    Ok(Arc::new(builder.finish()))
}

/// Each value of `array`, of `logical`, as text: UUIDs hyphenated, bytes in lower-case hex,
/// nested values and JSON as JSON text, anything else as Arrow renders it.
pub(crate) fn text(array: &ArrayRef, logical: &LogicalType) -> Result<ArrayRef, ArrowError> {
    match logical {
        LogicalType::Struct(_) | LogicalType::List(_) | LogicalType::Json => json(array, logical),
        LogicalType::Uuid | LogicalType::Binary => {
            let array = normalize(array, logical)?;
            let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 36);
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    let bytes = match logical {
                        LogicalType::Uuid => array.as_fixed_size_binary().value(row),
                        _ => array.as_binary::<i32>().value(row),
                    };
                    builder.append_value(render_bytes(bytes, *logical == LogicalType::Uuid));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => arrow_cast::cast(array, &DataType::Utf8),
    }
}

/// `bytes` in lower-case hex; a UUID's 16 bytes in its hyphenated form.
fn render_bytes(bytes: &[u8], uuid: bool) -> String {
    let mut rendered = String::with_capacity(bytes.len() * 2 + 4);
    for (index, byte) in bytes.iter().enumerate() {
        if uuid && matches!(index, 4 | 6 | 8 | 10) {
            rendered.push('-');
        }
        write!(rendered, "{byte:02x}").expect("writing to a string cannot fail");
    }
    rendered
}

/// Encodes the canonical extension types: `Json` text as the JSON it holds, a UUID as its
/// hyphenated string.
#[derive(Debug)]
struct Extensions;

impl EncoderFactory for Extensions {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let nulls = array.nulls().cloned();
        let encoder: Box<dyn Encoder + 'a> = match (
            field.metadata().get(EXTENSION_NAME).map(String::as_str),
            array.data_type(),
        ) {
            (Some("arrow.json"), DataType::Utf8) => Box::new(RawJson(array.as_string::<i32>())),
            (Some("arrow.uuid"), DataType::FixedSizeBinary(16)) => {
                Box::new(UuidText(array.as_fixed_size_binary()))
            }
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, nulls)))
    }
}

/// Writes JSON text as the value it holds.
struct RawJson<'a>(&'a arrow_array::StringArray);

impl Encoder for RawJson<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.extend_from_slice(self.0.value(idx).as_bytes());
    }
}

/// Writes a UUID as a JSON string of its hyphenated form.
struct UuidText<'a>(&'a arrow_array::FixedSizeBinaryArray);

impl Encoder for UuidText<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        out.push(b'"');
        out.extend_from_slice(render_bytes(self.0.value(idx), true).as_bytes());
        out.push(b'"');
    }
}

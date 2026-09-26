//! JSON encoders arrow-json lacks or gets wrong: the canonical extension types, non-finite
//! floats, temporal values beyond the years `chrono` holds, and lists of extension-typed items.

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, ListArray};
use arrow_json::writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder};
use arrow_schema::{ArrowError, DataType, FieldRef};

use super::super::temporal;
use super::{EXTENSION_NAME, render_bytes};

/// Encodes the canonical extension types: `Json` text as the JSON it holds, a UUID as its
/// hyphenated string.
#[derive(Debug)]
pub(super) struct Extensions;

impl EncoderFactory for Extensions {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
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
            // JSON has no non-finite numbers, which arrow-json writes as `null`; they are named.
            // arrow-json also writes some finite ones with more digits than they need.
            (_, DataType::Float32 | DataType::Float64) => Box::new(Floats(array)),
            // arrow-json renders temporal values it cannot hold as nothing or `<invalid>`.
            (_, data_type) if temporal::is_temporal(data_type) => {
                Box::new(TemporalText(temporal::Renderer::new(array)?, String::new()))
            }
            // arrow-json encodes a list's items with the list's field, losing the items'
            // extension types; items are encoded with their own field here.
            (_, DataType::List(item)) => {
                let list = array.as_list::<i32>();
                let items = make_encoder(item, list.values().as_ref(), options)?;
                Box::new(Items { list, items })
            }
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, nulls)))
    }
}

/// Writes a finite float as the shortest text that reads back as it, and a non-finite one as a
/// JSON string of its name: `NaN`, `Infinity` or `-Infinity`.
///
/// A tie between two shortest texts goes to the even one, as JSON writers break it.
struct Floats<'a>(&'a dyn Array);

impl Encoder for Floats<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let (value, single) = match self.0.data_type() {
            DataType::Float32 => {
                let value = self.0.as_primitive::<Float32Type>().value(idx);
                (f64::from(value), Some(value))
            }
            _ => (self.0.as_primitive::<Float64Type>().value(idx), None),
        };
        let name: &[u8] = match value {
            value if value.is_nan() => b"\"NaN\"",
            value if value == f64::INFINITY => b"\"Infinity\"",
            value if value == f64::NEG_INFINITY => b"\"-Infinity\"",
            _ => {
                let written = match single {
                    Some(single) => serde_json::to_writer(&mut *out, &single),
                    None => serde_json::to_writer(&mut *out, &value),
                };
                return written.expect("a finite float writes to a vector");
            }
        };
        out.extend_from_slice(name);
    }
}

/// Writes a temporal value as a JSON string of its text, rendered into a reused buffer.
struct TemporalText<'a>(temporal::Renderer<'a>, String);

impl Encoder for TemporalText<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.1.clear();
        self.0.write(idx, &mut self.1);
        out.push(b'"');
        out.extend_from_slice(self.1.as_bytes());
        out.push(b'"');
    }
}

/// Writes a list with its items' own encoder.
struct Items<'a> {
    list: &'a ListArray,
    items: NullableEncoder<'a>,
}

impl Encoder for Items<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        let range = self.list.value_offsets();
        let bound = |offset: i32| usize::try_from(offset).expect("list offsets are not negative");
        let (start, end) = (bound(range[idx]), bound(range[idx + 1]));
        out.push(b'[');
        for item in start..end {
            if item != start {
                out.push(b',');
            }
            if self.items.is_null(item) {
                out.extend_from_slice(b"null");
            } else {
                self.items.encode(item, out);
            }
        }
        out.push(b']');
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

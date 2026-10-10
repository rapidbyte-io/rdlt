//! JSON encoders arrow-json lacks or gets wrong: the canonical extension types, non-finite
//! floats, temporal values beyond the years `chrono` holds, and lists of extension-typed items.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ListArray};
use arrow_json::writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder};
use arrow_schema::{ArrowError, DataType, FieldRef};

use super::super::temporal;
use super::render_bytes;
use rdlt_connector::{Field, LogicalType};

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
        let encoder: Box<dyn Encoder + 'a> = match (Field::extension_type(field), array.data_type())
        {
            (Some(LogicalType::Json), DataType::Utf8) => {
                Box::new(RawJson(array.as_string::<i32>()))
            }
            (Some(LogicalType::Uuid), DataType::FixedSizeBinary(16)) => {
                Box::new(UuidText(array.as_fixed_size_binary()))
            }
            // JSON has no non-finite numbers, which arrow-json writes as `null`; they are named.
            // arrow-json also writes some finite ones with more digits than they need.
            (_, DataType::Float32 | DataType::Float64) => Box::new(Floats(array)),
            // arrow-json renders temporal values it cannot hold as nothing or `<invalid>`.
            (_, data_type) if rdlt_connector::instants::is_temporal(data_type) => {
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

/// Writes each float into JSON text as [`rdlt_connector::json::write_float`] does, the rule
/// connectors share, not the canonical float text of identity.
struct Floats<'a>(&'a dyn Array);

impl Encoder for Floats<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        rdlt_connector::json::write_float(self.0, idx, out);
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

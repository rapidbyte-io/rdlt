//! What a schema takes on the heap, as a batch and a decoder keep it.

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema};

use crate::limits::count;

/// Bytes: a schema in the `Arc` its batches share, beside its fields and metadata.
const SCHEMA: u64 = 80;

/// Bytes: a list of fields, a schema's, a struct's or a union's, beside the places in it.
const LIST: u64 = 16;

/// Bytes: a field's place in a schema's or a struct's list; a union's member takes two, its id
/// beside it.
const PLACE: u64 = 8;

/// Bytes: a field in the `Arc` its list or its type holds, beside its name and metadata.
const FIELD: u64 = 128;

/// Bytes: each of the two boxes a dictionary's type holds its keys' and its values' types in.
const BOXED: u64 = 24;

/// Bytes: a time zone's `Arc` beside its text, which it pads to a multiple of eight bytes.
const ZONE: u64 = 16;

/// Bytes: a map of metadata's table beside what [`SLOT`] counts, once it has room for any entry.
const MAP: u64 = 44;

/// Bytes: what a map of metadata's table takes for each entry it has room for, beside the entry's
/// text: the entry, its control byte and the slots a table keeps free.
const SLOT: u64 = 56;

/// Bytes: what `schema` takes in the `Arc` a batch or a decoder holds it in: the schema, its list
/// of fields and every list below, each field at any depth with its place in its list, its name,
/// its metadata and what its type allocates, and the schema's metadata.
///
/// Text is counted by its capacity, and a map of metadata by the entries it has room for.
pub fn schema_held_bytes(schema: &Schema) -> u64 {
    let fields = schema.fields();
    let mut bytes = SCHEMA
        .saturating_add(metadata(schema.metadata()))
        .saturating_add(list(fields.len(), PLACE));
    // A stack of the fields still to count, so a deep type costs no call stack.
    let mut pending: Vec<&Field> = fields.iter().map(AsRef::as_ref).collect();
    while let Some(field) = pending.pop() {
        let own = FIELD
            .saturating_add(count(field.name().capacity()))
            .saturating_add(metadata(field.metadata()));
        bytes = bytes.saturating_add(own);
        let mut data_type = field.data_type();
        while let DataType::Dictionary(_, values) = data_type {
            bytes = bytes.saturating_add(2 * BOXED);
            data_type = values;
        }
        match data_type {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => pending.push(item),
            DataType::Struct(fields) => {
                bytes = bytes.saturating_add(list(fields.len(), PLACE));
                pending.extend(fields.iter().map(AsRef::as_ref));
            }
            DataType::Union(fields, _) => {
                bytes = bytes.saturating_add(list(fields.len(), 2 * PLACE));
                pending.extend(fields.iter().map(|(_, field)| field.as_ref()));
            }
            DataType::RunEndEncoded(ends, values) => {
                pending.extend([ends.as_ref(), values.as_ref()]);
            }
            DataType::Timestamp(_, Some(zone)) => {
                let text = count(zone.len()).next_multiple_of(8);
                bytes = bytes.saturating_add(ZONE.saturating_add(text));
            }
            _ => {}
        }
    }
    bytes
}

/// Bytes: a list of `fields` fields, each taking `place` in it.
fn list(fields: usize, place: u64) -> u64 {
    LIST.saturating_add(count(fields).saturating_mul(place))
}

/// Bytes: what a map of metadata takes: its table, and the text of its keys and values.
fn metadata(map: &HashMap<String, String>) -> u64 {
    let room = count(map.capacity());
    let table = if room == 0 {
        0
    } else {
        MAP.saturating_add(room.saturating_mul(SLOT))
    };
    let text = map.iter().map(|(key, value)| {
        let capacity = key.capacity().saturating_add(value.capacity());
        count(capacity)
    });
    text.fold(table, u64::saturating_add)
}

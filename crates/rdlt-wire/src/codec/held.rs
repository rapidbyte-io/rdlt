//! What a schema takes on the heap, as a batch and a decoder keep it.

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema};

use crate::limits::count;

/// Bytes: what one field of a schema takes beside its name and its metadata.
const FIELD: u64 = 128;

/// Bytes: what one entry of a schema's or a field's metadata takes beside its text.
const ENTRY: u64 = 48;

/// Bytes: about what `schema` takes: each of its fields, nested ones too, with its name, its
/// metadata and its time zone, and the schema's own metadata.
pub fn schema_held_bytes(schema: &Schema) -> u64 {
    let text = |metadata: &HashMap<String, String>| {
        let entries = metadata.iter();
        let bytes = entries.map(|(key, value)| count(key.len() + value.len()) + ENTRY);
        bytes.fold(0, u64::saturating_add)
    };
    let mut bytes = text(schema.metadata());
    // A stack of the fields still to count, so a deep type costs no call stack.
    let mut pending: Vec<&Field> = schema.fields().iter().map(AsRef::as_ref).collect();
    while let Some(field) = pending.pop() {
        let own = FIELD + count(field.name().len()) + text(field.metadata());
        bytes = bytes.saturating_add(own);
        let mut data_type = field.data_type();
        while let DataType::Dictionary(_, values) = data_type {
            data_type = values;
        }
        match data_type {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => pending.push(item),
            DataType::Struct(fields) => pending.extend(fields.iter().map(AsRef::as_ref)),
            DataType::Union(fields, _) => {
                pending.extend(fields.iter().map(|(_, field)| field.as_ref()));
            }
            DataType::RunEndEncoded(ends, values) => {
                pending.extend([ends.as_ref(), values.as_ref()]);
            }
            DataType::Timestamp(_, Some(zone)) => bytes = bytes.saturating_add(count(zone.len())),
            _ => {}
        }
    }
    bytes
}

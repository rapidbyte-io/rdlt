//! The reference shredder the parallel one is checked against: `serde_json`, recursion, a type
//! read off each column's whole set of values, and arrow-json to build the batch.

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::{DecimalType, Field, Fields, LogicalType, TableSchema};
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

/// The machine code of the error shredding fails with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Code(pub(crate) &'static str);

/// A parsed JSON value, keeping objects' key order and repeated keys.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Wide(u64),
    Float(f64),
    Text(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(JsonVisitor)
    }
}

struct JsonVisitor;

impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Json, E> {
        Ok(Json::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Json, E> {
        Ok(Json::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Json, E> {
        Ok(Json::Int(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Json, E> {
        Ok(i64::try_from(value).map_or(Json::Wide(value), Json::Int))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Json, E> {
        // Negative zero reads as zero, as sonic-rs reads it.
        Ok(Json::Float(if value == 0.0 { 0.0 } else { value }))
    }

    fn visit_str<E>(self, value: &str) -> Result<Json, E> {
        Ok(Json::Text(value.to_owned()))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Json, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Json::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Json, A::Error> {
        let mut pairs = Vec::new();
        while let Some(pair) = map.next_entry()? {
            pairs.push(pair);
        }
        Ok(Json::Object(pairs))
    }
}

impl Json {
    /// The value as `serde_json` holds it, for rendering and for arrow-json.
    fn to_serde(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => (*value).into(),
            Self::Int(value) => (*value).into(),
            Self::Wide(value) => (*value).into(),
            Self::Float(value) => {
                serde_json::Number::from_f64(*value).map_or(serde_json::Value::Null, Into::into)
            }
            Self::Text(value) => value.clone().into(),
            Self::Array(items) => items.iter().map(Self::to_serde).collect(),
            Self::Object(pairs) => pairs
                .iter()
                .map(|(key, value)| (key.clone(), value.to_serde()))
                .collect(),
        }
    }

    /// How deep the value nests, a scalar being one level.
    fn depth(&self) -> usize {
        match self {
            Self::Array(items) => 1 + items.iter().map(Self::depth).max().unwrap_or(0),
            Self::Object(pairs) => {
                1 + pairs
                    .iter()
                    .map(|(_, value)| value.depth())
                    .max()
                    .unwrap_or(0)
            }
            _ => 1,
        }
    }

    /// Whether an object in the value repeats a key.
    fn repeats_a_key(&self) -> bool {
        match self {
            Self::Array(items) => items.iter().any(Self::repeats_a_key),
            Self::Object(pairs) => {
                pairs
                    .iter()
                    .enumerate()
                    .any(|(i, (key, _))| pairs[..i].iter().any(|(other, _)| other == key))
                    || pairs.iter().any(|(_, value)| value.repeats_a_key())
            }
            _ => false,
        }
    }
}

/// The records of `pushes`, each a JSON array of objects or objects on their own lines.
fn records(pushes: &[Bytes]) -> Result<Vec<Json>, Code> {
    let mut records = Vec::new();
    for push in pushes {
        let text = std::str::from_utf8(push).map_err(|_| Code("json_invalid"))?;
        if text.trim_start().starts_with('[') {
            let Json::Array(items) =
                serde_json::from_str(text).map_err(|_| Code("json_invalid"))?
            else {
                return Err(Code("json_invalid"));
            };
            records.extend(items);
        } else {
            for line in text
                .split('\n')
                .map(str::trim)
                .filter(|line| !line.is_empty())
            {
                records.push(serde_json::from_str(line).map_err(|_| Code("json_invalid"))?);
            }
        }
    }
    Ok(records)
}

/// The type of a column holding `values`, absent ones being null.
fn type_of(values: &[&Json]) -> LogicalType {
    let present: Vec<&Json> = values
        .iter()
        .copied()
        .filter(|value| **value != Json::Null)
        .collect();
    let all = |test: fn(&Json) -> bool| present.iter().all(|value| test(value));
    if present.is_empty() {
        LogicalType::Null
    } else if all(|value| matches!(value, Json::Bool(_))) {
        LogicalType::Bool
    } else if all(|value| matches!(value, Json::Text(_))) {
        LogicalType::Utf8
    } else if all(|value| matches!(value, Json::Int(_))) {
        LogicalType::Int64
    } else if all(|value| matches!(value, Json::Int(_) | Json::Wide(_))) {
        LogicalType::Decimal(DecimalType::new(20, 0).expect("20 digits fit a decimal"))
    } else if all(|value| match value {
        Json::Float(_) => true,
        Json::Int(integer) => integer.unsigned_abs() <= 1 << 53,
        _ => false,
    }) {
        LogicalType::Float64
    } else if all(|value| matches!(value, Json::Array(_))) {
        let items: Vec<&Json> = present
            .iter()
            .flat_map(|value| match value {
                Json::Array(items) => items.iter().collect(),
                _ => Vec::new(),
            })
            .collect();
        LogicalType::List(Box::new(Field::new("item", type_of(&items), true)))
    } else if all(|value| matches!(value, Json::Object(_))) {
        LogicalType::Struct(Fields::new(fields_of(&present)).expect("keys are distinct"))
    } else {
        LogicalType::Json
    }
}

/// The fields of objects `objects`, in the order their keys first appear.
fn fields_of(objects: &[&Json]) -> Vec<Field> {
    let mut names: Vec<&str> = Vec::new();
    for object in objects {
        if let Json::Object(pairs) = object {
            for (key, _) in pairs {
                if !names.contains(&key.as_str()) {
                    names.push(key);
                }
            }
        }
    }
    names
        .into_iter()
        .map(|name| {
            let values: Vec<&Json> = objects
                .iter()
                .map(|object| match object {
                    Json::Object(pairs) => pairs
                        .iter()
                        .find(|(key, _)| key == name)
                        .map_or(&Json::Null, |(_, value)| value),
                    _ => &Json::Null,
                })
                .collect();
            Field::new(name, type_of(&values), true)
        })
        .collect()
}

/// `value`, of type `logical`, as arrow-json should read it: values stored as JSON become their
/// JSON text.
fn prepare(value: &Json, logical: &LogicalType) -> serde_json::Value {
    match (value, logical) {
        (Json::Null, _) => serde_json::Value::Null,
        (_, LogicalType::Json) => value.to_serde().to_string().into(),
        (Json::Array(items), LogicalType::List(item)) => items
            .iter()
            .map(|value| prepare(value, item.logical_type()))
            .collect(),
        (Json::Object(pairs), LogicalType::Struct(fields)) => pairs
            .iter()
            .map(|(key, value)| {
                let field = fields.get(key).expect("every key is a field");
                (key.clone(), prepare(value, field.logical_type()))
            })
            .collect(),
        _ => value.to_serde(),
    }
}

/// The batch of every record of `pushes`, or the code of the error shredding them fails with.
pub(crate) fn shred(pushes: &[Bytes]) -> Result<RecordBatch, Code> {
    let records = records(pushes)?;
    if records
        .iter()
        .any(|record| !matches!(record, Json::Object(_)))
    {
        return Err(Code("json_not_object"));
    }
    if records.iter().any(|record| record.depth() > 64) {
        return Err(Code("limit_exceeded"));
    }
    if records.iter().any(Json::repeats_a_key) {
        return Err(Code("json_duplicate_key"));
    }
    let fields = fields_of(&records.iter().collect::<Vec<_>>());
    let schema = TableSchema::new(fields.clone()).expect("keys are distinct");
    let struct_type = LogicalType::Struct(Fields::new(fields).expect("keys are distinct"));
    let rows: Vec<serde_json::Value> = records
        .iter()
        .map(|record| prepare(record, &struct_type))
        .collect();
    let arrow = Arc::new(schema.to_arrow());
    if arrow.fields().is_empty() {
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return Ok(
            RecordBatch::try_new_with_options(arrow, Vec::new(), &options)
                .expect("rows without columns"),
        );
    }
    let mut decoder = arrow_json::ReaderBuilder::new(Arc::clone(&arrow))
        .with_batch_size(rows.len().max(1))
        .build_decoder()
        .expect("the schema decodes");
    decoder
        .serialize(&rows)
        .expect("prepared rows fit their schema");
    Ok(decoder
        .flush()
        .expect("rows decode")
        .unwrap_or_else(|| RecordBatch::new_empty(arrow)))
}

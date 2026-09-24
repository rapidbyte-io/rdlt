//! The parallel shredder against the reference one, on generated pushes (§20.4).

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field as ArrowField};
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::{Field, LogicalType};

use super::reference::{self, Code, Json};
use super::shred;
use crate::compute::{Inline, ready};

/// The batches of `pushes`, shredded in chunks of `chunk_bytes`, as one, or the error's code.
pub(super) fn shredded(pushes: &[Bytes], chunk_bytes: usize) -> Result<Option<RecordBatch>, Code> {
    let batches = ready(shred(&Inline, pushes, chunk_bytes)).map_err(|error| Code(error.code()))?;
    let Some(first) = batches.first() else {
        return Ok(None);
    };
    Ok(Some(
        arrow_select::concat::concat_batches(&first.schema(), &batches)
            .expect("the batches share a schema"),
    ))
}

/// `batch` with its JSON text normalized, so renderings that differ only in formatting compare
/// equal.
pub(super) fn normalized(batch: &RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| normalize(field, column))
        .collect();
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &options)
        .expect("normalizing keeps the schema")
}

fn normalize(field: &ArrowField, array: &ArrayRef) -> ArrayRef {
    let logical = Field::from_arrow(field)
        .expect("a shredded field")
        .logical_type()
        .clone();
    match (&logical, field.data_type()) {
        (LogicalType::Json, _) => Arc::new(
            array
                .as_string::<i32>()
                .iter()
                .map(|text| {
                    text.map(|text| {
                        serde_json::from_str::<serde_json::Value>(text)
                            .expect("JSON text")
                            .to_string()
                    })
                })
                .collect::<StringArray>(),
        ),
        (_, DataType::Struct(fields)) => {
            let array = array.as_struct();
            let columns = fields
                .iter()
                .zip(array.columns())
                .map(|(field, column)| normalize(field, column))
                .collect();
            if fields.is_empty() {
                return Arc::new(StructArray::new_empty_fields(
                    array.len(),
                    array.nulls().cloned(),
                ));
            }
            Arc::new(StructArray::new(
                fields.clone(),
                columns,
                array.nulls().cloned(),
            ))
        }
        (_, DataType::List(item)) => {
            let array = array.as_list::<i32>();
            let values = normalize(item, array.values());
            Arc::new(ListArray::new(
                Arc::clone(item),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            ))
        }
        _ => Arc::clone(array),
    }
}

/// A JSON rendering of `value` that keeps its key order and repeated keys.
pub(super) fn render(value: &Json) -> String {
    match value {
        Json::Object(pairs) => {
            let pairs: Vec<String> = pairs
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("a key renders"),
                        render(value)
                    )
                })
                .collect();
            format!("{{{}}}", pairs.join(","))
        }
        Json::Array(items) => format!(
            "[{}]",
            items.iter().map(render).collect::<Vec<_>>().join(",")
        ),
        Json::Float(value) => serde_json::to_string(value).expect("a finite float renders"),
        Json::Null => "null".to_owned(),
        Json::Bool(value) => value.to_string(),
        Json::Int(value) => value.to_string(),
        Json::Wide(value) => value.to_string(),
        Json::Text(value) => serde_json::to_string(value).expect("a string renders"),
    }
}

fn scalar() -> impl Strategy<Value = Json> {
    prop_oneof![
        2 => Just(Json::Null),
        1 => any::<bool>().prop_map(Json::Bool),
        3 => (-1000_i64..1000).prop_map(Json::Int),
        1 => prop_oneof![Just(i64::MIN), Just(i64::MAX), Just((1 << 53) + 1), Just(-(1 << 53))].prop_map(Json::Int),
        1 => ((i64::MAX as u64 + 1)..=u64::MAX).prop_map(Json::Wide),
        2 => prop_oneof![(-1e6_f64..1e6), Just(0.5), Just(-0.0), Just(1e300), Just(5e-324)].prop_map(Json::Float),
        2 => prop_oneof![Just(String::new()), "[a-c]{1,3}", "\\PC{0,6}", Just("q\"\\\n\u{1}é😀".to_owned())].prop_map(Json::Text),
    ]
}

fn key() -> impl Strategy<Value = String> {
    prop_oneof![8 => "[a-e]", 1 => Just("k\"é".to_owned())]
}

fn object(value: impl Strategy<Value = Json>) -> impl Strategy<Value = Json> {
    prop::collection::vec((key(), value), 0..5).prop_map(|pairs| {
        let mut seen = Vec::new();
        Json::Object(
            pairs
                .into_iter()
                .filter(|(key, _)| {
                    !seen.contains(key) && {
                        seen.push(key.clone());
                        true
                    }
                })
                .collect(),
        )
    })
}

fn value() -> impl Strategy<Value = Json> {
    scalar().prop_recursive(4, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Json::Array),
            object(inner)
        ]
    })
}

/// A value nesting `levels` levels deep.
fn nested(levels: usize) -> Json {
    (1..levels).fold(Json::Int(1), |value, _| Json::Array(vec![value]))
}

/// What goes wrong in a generated push, if anything.
#[derive(Clone, Debug)]
enum Fault {
    NotObject(Json),
    RepeatedKey,
    TooDeep,
    Truncated,
}

fn fault() -> impl Strategy<Value = Option<Fault>> {
    prop_oneof![
        12 => Just(None),
        1 => scalar().prop_map(|value| Some(Fault::NotObject(value))),
        1 => Just(Some(Fault::RepeatedKey)),
        1 => Just(Some(Fault::TooDeep)),
        1 => Just(Some(Fault::Truncated)),
    ]
}

/// Records rendered into pushes, some JSON arrays and some JSON lines.
fn pushes(
    records: &[Json],
    cuts: &[(usize, bool)],
    fault: Option<&Fault>,
    at: usize,
) -> Vec<Bytes> {
    let mut rendered: Vec<String> = records.iter().map(render).collect();
    if !rendered.is_empty() {
        let at = at % rendered.len();
        match fault {
            None => {}
            Some(Fault::NotObject(value)) => rendered[at] = render(value),
            Some(Fault::RepeatedKey) => rendered[at] = r#"{"a":1,"b":2,"a":3}"#.to_owned(),
            Some(Fault::TooDeep) => rendered[at] = format!(r#"{{"a":{}}}"#, render(&nested(64))),
            Some(Fault::Truncated) => rendered[at] = r#"{"a":[1,"#.to_owned(),
        }
    }
    let mut pushes = Vec::new();
    let mut start = 0;
    for (index, &(size, array)) in cuts.iter().enumerate() {
        let end = if index + 1 == cuts.len() {
            rendered.len()
        } else {
            (start + size).min(rendered.len())
        };
        let part = &rendered[start..end];
        let text = if array {
            format!("[{}]", part.join(","))
        } else {
            part.join("\n") + "\n"
        };
        pushes.push(Bytes::from(text));
        start = end;
    }
    pushes
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn the_shredder_agrees_with_the_reference(
        records in prop::collection::vec(object(value()), 0..24),
        cuts in prop::collection::vec((0_usize..10, any::<bool>()), 1..4),
        fault in fault(),
        at in any::<usize>(),
        chunk_bytes in 1_usize..400,
    ) {
        let pushes = pushes(&records, &cuts, fault.as_ref(), at);
        let expected = reference::shred(&pushes);
        let actual = shredded(&pushes, chunk_bytes);
        match (expected, actual) {
            (Ok(expected), Ok(Some(actual))) => prop_assert_eq!(normalized(&actual), normalized(&expected)),
            (Ok(expected), Ok(None)) => prop_assert_eq!(expected.num_rows(), 0),
            (Err(expected), Err(actual)) => prop_assert_eq!(actual, expected),
            (expected, actual) => prop_assert!(false, "reference {expected:?}, shredder {actual:?}"),
        }
    }
}

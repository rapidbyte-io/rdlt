//! The parallel shredder against the reference one, on generated pushes.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field as ArrowField};
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::{Exclusion, Field, LogicalType};

use super::reference::{self, Json};
use super::{observe, shred};
use crate::compute::{Pool, ready};
use crate::error::Code;

/// The batches of `pushes`, shredded in chunks of `chunk_bytes`, as one, or the error's code.
pub(super) fn shredded(pushes: &[Bytes], chunk_bytes: usize) -> Result<Option<RecordBatch>, Code> {
    shredded_within(pushes, chunk_bytes, super::tests::limits())
}

/// The batches of `pushes`, shredded as [`shredded`] does within `limits`.
pub(super) fn shredded_within(
    pushes: &[Bytes],
    chunk_bytes: usize,
    limits: super::ShredLimits,
) -> Result<Option<RecordBatch>, Code> {
    let batches =
        ready(shred(&Pool::inline(), pushes, chunk_bytes, limits)).map_err(|error| error.code())?;
    Ok(concatenated(&batches))
}

/// The batches of `pushes`, shredded as [`shredded_within`] does without the values of the keys
/// `excluded` names.
pub(super) fn shredded_excluding(
    pushes: &[Bytes],
    chunk_bytes: usize,
    limits: super::ShredLimits,
    excluded: &Exclusion,
) -> Result<Option<RecordBatch>, Code> {
    let pool = Pool::inline();
    let observing = observe(
        &pool,
        pushes.to_vec(),
        chunk_bytes,
        limits,
        excluded.clone(),
    );
    let built = async { observing.await?.build(&pool).await };
    let batches = ready(built).map_err(|error| error.code())?;
    Ok(concatenated(&batches))
}

/// `batches`, which share a schema, as one; none where there are none.
fn concatenated(batches: &[RecordBatch]) -> Option<RecordBatch> {
    let first = batches.first()?;
    Some(
        arrow_select::concat::concat_batches(&first.schema(), batches)
            .expect("the batches share a schema"),
    )
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
    /// Too deep after its column became JSON in place: an integer, then a string.
    TooDeepAfterDrift,
    Truncated,
}

fn fault() -> impl Strategy<Value = Option<Fault>> {
    prop_oneof![
        12 => Just(None),
        1 => scalar().prop_map(|value| Some(Fault::NotObject(value))),
        1 => Just(Some(Fault::RepeatedKey)),
        1 => Just(Some(Fault::TooDeep)),
        1 => Just(Some(Fault::TooDeepAfterDrift)),
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
            Some(Fault::TooDeepAfterDrift) => {
                let deep = format!(r#"{{"a":{}}}"#, render(&nested(64)));
                rendered.splice(
                    at..=at,
                    [r#"{"a":1}"#.to_owned(), r#"{"a":"x"}"#.to_owned(), deep],
                );
            }
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
        // Chunks whose builders pass their small allowance are observed and built again; with
        // room for them, each is built as it is parsed, its drifted columns converted in place.
        let roomy = super::ShredLimits {
            admitted: 1 << 20,
            ..super::tests::limits()
        };
        for limits in [super::tests::limits(), roomy] {
            let expected = reference::shred(&pushes);
            let actual = shredded_within(&pushes, chunk_bytes, limits);
            match (expected, actual) {
                (Ok(expected), Ok(Some(actual))) => prop_assert_eq!(normalized(&actual), normalized(&expected)),
                (Ok(expected), Ok(None)) => prop_assert_eq!(expected.num_rows(), 0),
                (Err(expected), Err(actual)) => prop_assert_eq!(actual, expected),
                (expected, actual) => prop_assert!(false, "reference {expected:?}, shredder {actual:?}"),
            }
        }
    }
}

/// `record`, an object, without its top-level keys `excluded` names; any other value as it is.
fn without(record: &Json, excluded: &std::collections::BTreeSet<String>) -> Json {
    match record {
        Json::Object(pairs) => Json::Object(
            pairs
                .iter()
                .filter(|(key, _)| !excluded.contains(key))
                .cloned()
                .collect(),
        ),
        other => other.clone(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn shredding_without_excluded_keys_is_shredding_records_that_lack_them(
        records in prop::collection::vec(object(value()), 0..24),
        cuts in prop::collection::vec((0_usize..10, any::<bool>()), 1..4),
        excluded in prop::collection::btree_set(key(), 0..3),
        chunk_bytes in 1_usize..400,
    ) {
        let lacking: Vec<Json> = records.iter().map(|record| without(record, &excluded)).collect();
        let exclusion = Exclusion::new(excluded.iter().cloned());
        let roomy = super::ShredLimits {
            admitted: 1 << 20,
            ..super::tests::limits()
        };
        let exactly = super::ShredLimits {
            exact: true,
            ..roomy
        };
        for limits in [super::tests::limits(), roomy, exactly] {
            let expected = shredded_within(&pushes(&lacking, &cuts, None, 0), chunk_bytes, limits);
            let actual =
                shredded_excluding(&pushes(&records, &cuts, None, 0), chunk_bytes, limits, &exclusion);
            match (expected, actual) {
                (Ok(Some(expected)), Ok(Some(actual))) => {
                    prop_assert_eq!(normalized(&actual), normalized(&expected));
                }
                (expected, actual) => prop_assert_eq!(actual, expected),
            }
        }
    }
}

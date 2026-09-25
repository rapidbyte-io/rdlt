use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, MapBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, LargeListArray, RecordBatch, StringArray, UInt8Array,
};
use bytes::Bytes;
use proptest::prelude::*;
use serde_json::{Value as Json, json};

use super::reference::{self, Row, canonical_text};
use super::{Part, Shape, normalize};
use crate::compute::{Inline, ready};
use crate::shred::shred;

fn shape(max_depth: u8) -> Shape {
    Shape {
        max_depth,
        whole: BTreeSet::new(),
        key: Vec::new(),
    }
}

/// `records` shredded as one push into batches of about `chunk_bytes` each.
fn shredded(records: &[Json], chunk_bytes: usize) -> Vec<RecordBatch> {
    let lines: Vec<String> = records.iter().map(Json::to_string).collect();
    let push = Bytes::from(lines.join("\n"));
    ready(shred(&Inline, &[push], chunk_bytes)).expect("the records shred")
}

/// The parts of `records`, shredded as one batch and normalized as `shape`.
fn parts(records: &[Json], shape: &Shape) -> Vec<Part> {
    shredded(records, 1 << 20)
        .iter()
        .flat_map(|batch| normalize(batch, shape).expect("the batch normalizes"))
        .collect()
}

/// The value at `row` of `array` as JSON: objects without their null fields, integral floats as
/// integers.
fn json_of(array: &ArrayRef, row: usize) -> Json {
    if array.is_null(row) {
        return Json::Null;
    }
    match array.data_type() {
        arrow_schema::DataType::Null => Json::Null,
        arrow_schema::DataType::Boolean => Json::from(array.as_boolean().value(row)),
        arrow_schema::DataType::Int64 => Json::from(array.as_primitive::<Int64Type>().value(row)),
        arrow_schema::DataType::Float64 => {
            let value = array
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(row);
            serde_json::Number::from_f64(value).map_or(Json::Null, Json::Number)
        }
        arrow_schema::DataType::Utf8 => Json::from(array.as_string::<i32>().value(row)),
        arrow_schema::DataType::Struct(_) => {
            let object = array.as_struct();
            Json::Object(
                object
                    .fields()
                    .iter()
                    .zip(object.columns())
                    .map(|(field, column)| (field.name().clone(), json_of(column, row)))
                    .filter(|(_, value)| !value.is_null())
                    .collect(),
            )
        }
        arrow_schema::DataType::List(_) => {
            let items = array.as_list::<i32>().value(row);
            Json::Array((0..items.len()).map(|item| json_of(&items, item)).collect())
        }
        other => panic!("no JSON form for {other}"),
    }
}

/// The rows of `part` as the reference writes them.
fn rows_of(part: &Part) -> Vec<Row> {
    let ids = part.lineage.id.as_binary::<i32>();
    (0..part.batch.num_rows())
        .map(|row| {
            let parent = part.lineage.parent.as_ref().map(|parent| {
                (
                    parent.id.as_binary::<i32>().value(row).to_vec(),
                    parent.root.as_binary::<i32>().value(row).to_vec(),
                    parent.idx.as_primitive::<Int64Type>().value(row),
                )
            });
            let schema = part.batch.schema();
            let columns = part
                .columns
                .iter()
                .zip(schema.fields().iter().zip(part.batch.columns()))
                .map(|(path, (field, column))| {
                    let value = json_of(column, row);
                    // A column whose values mix types holds them as JSON text.
                    let json = field
                        .metadata()
                        .get("ARROW:extension:name")
                        .map(String::as_str)
                        == Some("arrow.json");
                    match (json, value) {
                        (true, Json::String(text)) => (
                            path,
                            serde_json::from_str(&text).expect("JSON columns hold JSON"),
                        ),
                        (_, value) => (path, value),
                    }
                })
                .filter(|(_, value)| !value.is_null())
                .map(|(path, value)| {
                    let path = path.segments().map(str::to_owned).collect();
                    (path, canonical_text(&value))
                })
                .collect();
            Row {
                id: ids.value(row).to_vec(),
                parent,
                columns,
            }
        })
        .collect()
}

/// Every table's rows, sorted, by the table's path.
fn tables_of(parts: &[Part]) -> reference::Tables {
    let mut tables = reference::Tables::new();
    for part in parts {
        let path = part.path.iter().map(ToString::to_string).collect();
        tables.entry(path).or_default().extend(rows_of(part));
    }
    for rows in tables.values_mut() {
        rows.sort();
    }
    tables
}

fn sorted(mut tables: reference::Tables) -> reference::Tables {
    for rows in tables.values_mut() {
        rows.sort();
    }
    tables
}

fn path(segments: &[&str]) -> Vec<String> {
    segments
        .iter()
        .map(|segment| (*segment).to_owned())
        .collect()
}

#[test]
fn objects_flatten_into_a_column_per_field() {
    let parts = parts(
        &[json!({"id": 1, "meta": {"a": 2, "b": {"c": "x"}}})],
        &shape(8),
    );
    assert_eq!(parts.len(), 1);
    let columns: Vec<String> = parts[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(columns, ["id", "meta.a", "meta.b.c"]);
    assert!(parts[0].lineage.parent.is_none());
}

#[test]
fn arrays_of_objects_become_child_tables_that_name_their_parents() {
    let parts = parts(
        &[json!({"id": 1, "items": [{"sku": "a"}, {"sku": "b", "tags": ["x", "y"]}]})],
        &shape(8),
    );
    let paths: Vec<Vec<Arc<str>>> = parts.iter().map(|part| part.path.clone()).collect();
    assert_eq!(
        paths,
        [
            Vec::new(),
            vec![Arc::from("items")],
            vec![Arc::from("items"), Arc::from("tags")],
        ]
    );
    let root = parts[0].lineage.id.as_binary::<i32>().value(0).to_vec();
    let items = parts[1]
        .lineage
        .parent
        .as_ref()
        .expect("a child has parents");
    assert!(
        items
            .id
            .as_binary::<i32>()
            .iter()
            .all(|parent| parent == Some(root.as_slice()))
    );
    assert_eq!(items.idx.as_primitive::<Int64Type>().values(), &[0, 1]);
    let second = parts[1].lineage.id.as_binary::<i32>().value(1).to_vec();
    let tags = parts[2]
        .lineage
        .parent
        .as_ref()
        .expect("a grandchild has parents");
    assert!(
        tags.id
            .as_binary::<i32>()
            .iter()
            .all(|parent| parent == Some(second.as_slice()))
    );
    assert!(
        tags.root
            .as_binary::<i32>()
            .iter()
            .all(|parent| parent == Some(root.as_slice()))
    );
    let values: Vec<String> = parts[2].columns.iter().map(ToString::to_string).collect();
    assert_eq!(
        values,
        ["value"],
        "arrays of values hold them in a value column"
    );
}

#[test]
fn null_and_empty_arrays_hold_no_rows_and_null_items_are_rows() {
    let parts = parts(
        &[
            json!({"id": 1, "scores": null}),
            json!({"id": 2, "scores": []}),
            json!({"id": 3, "scores": [null, 4]}),
        ],
        &shape(8),
    );
    let scores = &parts[1];
    assert_eq!(scores.batch.num_rows(), 2);
    let idx = scores.lineage.parent.as_ref().expect("child").idx.clone();
    assert_eq!(idx.as_primitive::<Int64Type>().values(), &[0, 1]);
    assert!(scores.batch.column(0).is_null(0));
}

#[test]
fn containers_deeper_than_max_depth_stay_whole() {
    let record = json!({"meta": {"b": {"c": 1}}, "items": [{"sku": "a"}]});
    let flat = parts(std::slice::from_ref(&record), &shape(1));
    let columns: Vec<String> = flat[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(
        columns,
        ["meta.b"],
        "the object inside meta is past depth 1"
    );
    assert_eq!(flat.len(), 2);
    let item_columns: Vec<String> = flat[1].columns.iter().map(ToString::to_string).collect();
    assert_eq!(item_columns, ["value"], "items are objects past depth 1");
    let none = parts(&[record], &shape(0));
    assert_eq!(none.len(), 1);
    let columns: Vec<String> = none[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(columns, ["items", "meta"]);
}

#[test]
fn whole_columns_stay_whole() {
    let shape = Shape {
        whole: BTreeSet::from([Arc::from("meta")]),
        ..shape(8)
    };
    let parts = parts(&[json!({"meta": {"a": [1]}, "b": 2})], &shape);
    assert_eq!(parts.len(), 1);
    let columns: Vec<String> = parts[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(columns, ["b", "meta"]);
}

#[test]
fn ids_do_not_depend_on_types_field_order_or_missing_fields() {
    let ids = |records: &[Json]| -> Vec<Vec<u8>> {
        parts(records, &shape(8))[0]
            .lineage
            .id
            .as_binary::<i32>()
            .iter()
            .map(|id| id.expect("ids are set").to_vec())
            .collect()
    };
    let as_integer = ids(&[json!({"a": 1, "b": null})]);
    let as_float = ids(&[json!({"b": null, "a": 1.0}), json!({"a": 2.5})]);
    assert_eq!(as_integer[0], as_float[0]);
    let missing = ids(&[json!({"a": 1}), json!({"a": 3, "b": "x"})]);
    assert_eq!(as_integer[0], missing[0]);
    assert_ne!(as_float[0], as_float[1]);
}

#[test]
fn keyed_roots_hash_their_key_and_keyless_roots_their_whole_row() {
    let keyed = Shape {
        key: vec![Arc::from("id")],
        ..shape(8)
    };
    let records = [json!({"id": 7, "v": 1}), json!({"id": 7, "v": 2})];
    let parts = parts(&records, &keyed);
    let ids = parts[0].lineage.id.as_binary::<i32>();
    assert_eq!(ids.value(0), ids.value(1), "one key, one id");
    let whole = normalize(&shredded(&records, 1 << 20)[0], &shape(8)).unwrap();
    let ids = whole[0].lineage.id.as_binary::<i32>();
    assert_ne!(ids.value(0), ids.value(1), "keyless rows differ by content");
}

#[test]
fn maps_and_large_lists_normalize_like_lists() {
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    map.keys().append_value("k");
    map.values().append_value(1);
    map.append(true).unwrap();
    let large = LargeListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(5)])]);
    let batch = RecordBatch::try_from_iter([
        ("m", Arc::new(map.finish()) as ArrayRef),
        ("l", Arc::new(large) as ArrayRef),
    ])
    .unwrap();
    let parts = normalize(&batch, &shape(8)).unwrap();
    let paths: Vec<String> = parts.iter().map(|part| part.path.join(".")).collect();
    assert_eq!(paths, ["", "m", "l"]);
    let map_columns: Vec<String> = parts[1].columns.iter().map(ToString::to_string).collect();
    assert_eq!(map_columns, ["keys", "values"]);
    assert_eq!(
        parts[2]
            .batch
            .column(0)
            .as_primitive::<Int64Type>()
            .values(),
        &[5]
    );
}

#[test]
fn other_arrow_types_hash_by_their_type_and_value() {
    let batch = |values: ArrayRef| RecordBatch::try_from_iter([("v", values)]).unwrap();
    let ids = |batch: RecordBatch| {
        normalize(&batch, &shape(8)).unwrap()[0]
            .lineage
            .id
            .as_binary::<i32>()
            .value(0)
            .to_vec()
    };
    let integer = ids(batch(Arc::new(Int64Array::from(vec![3]))));
    let float = ids(batch(Arc::new(Float64Array::from(vec![3.0]))));
    let text = ids(batch(Arc::new(StringArray::from(vec!["3"]))));
    let byte = ids(batch(Arc::new(UInt8Array::from(vec![3]))));
    assert_eq!(integer, float);
    assert_eq!(integer, byte);
    assert_ne!(integer, text);
}

fn number() -> impl Strategy<Value = Json> {
    prop_oneof![
        (0_i64..5).prop_map(Json::from),
        (0_i32..20).prop_map(|quarters| Json::from(f64::from(quarters) / 4.0)),
    ]
}

fn text() -> impl Strategy<Value = Json> {
    "[a-c]{0,3}".prop_map(Json::from)
}

fn maybe<S: Strategy<Value = Json>>(strategy: S) -> impl Strategy<Value = Option<Json>> {
    prop::option::weighted(0.7, strategy)
}

fn object(fields: Vec<(&'static str, Option<Json>)>) -> Json {
    Json::Object(
        fields
            .into_iter()
            .filter_map(|(name, value)| Some((name.to_owned(), value?)))
            .collect(),
    )
}

/// An array of up to two values of `item`, some of them null.
fn array<S: Strategy<Value = Json>>(item: S, null_share: f64) -> impl Strategy<Value = Json> {
    prop::collection::vec(
        prop::option::weighted((1.0 - null_share).min(0.99), item),
        0..3,
    )
    .prop_map(|items| {
        Json::Array(
            items
                .into_iter()
                .map(|item| item.unwrap_or(Json::Null))
                .collect(),
        )
    })
}

fn item() -> impl Strategy<Value = Json> {
    (
        text(),
        maybe((0_i64..5).prop_map(Json::from)),
        maybe(array(text(), 0.2)),
    )
        .prop_map(|(sku, qty, tags)| object(vec![("sku", Some(sku)), ("qty", qty), ("tags", tags)]))
}

fn record() -> impl Strategy<Value = Json> {
    let inner = maybe(text()).prop_map(|c| object(vec![("c", c)]));
    let a = prop_oneof![(0_i64..5).prop_map(Json::from), Just(Json::Null)];
    let meta = (maybe(a), maybe(inner)).prop_map(|(a, b)| object(vec![("a", a), ("b", b)]));
    let matrix = array(array((0_i64..3).prop_map(Json::from), 0.0), 0.0);
    let nested_key = array((0_i64..3).prop_map(Json::from), 0.0)
        .prop_map(|values| object(vec![("b", Some(values))]));
    (
        maybe((0_i64..4).prop_map(Json::from)),
        maybe(prop_oneof![text(), Just(Json::Null)]),
        maybe(number()),
        maybe(meta),
        maybe(array(item(), 0.15)),
        maybe(array((0_i64..5).prop_map(Json::from), 0.2)),
        maybe(matrix),
        maybe((0_i64..3).prop_map(Json::from)),
        maybe(nested_key),
        maybe(prop_oneof![(0_i64..3).prop_map(Json::from), text()]),
    )
        .prop_map(
            |(id, name, amount, meta, items, scores, matrix, flat, nested, mixed)| {
                object(vec![
                    ("mixed", mixed),
                    ("id", id),
                    ("name", name),
                    ("amount", amount),
                    ("meta", meta),
                    ("items", items),
                    ("scores", scores),
                    ("matrix", matrix),
                    ("a__b", flat),
                    ("a", nested),
                ])
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Normalizing shredded JSON agrees with the reference normalizer, ids included, however the
    /// records chunk into batches (§20.4).
    #[test]
    fn normalizing_agrees_with_the_reference(
        records in prop::collection::vec(record(), 1..10),
        max_depth in 0_u8..5,
        keyed in any::<bool>(),
        whole_meta in any::<bool>(),
        chunk_bytes in 16_usize..400,
    ) {
        let key: Vec<&str> = if keyed { vec!["id"] } else { Vec::new() };
        let whole: Vec<&str> = if whole_meta { vec!["meta"] } else { Vec::new() };
        let shape = Shape {
            max_depth,
            whole: whole.iter().map(|column| Arc::from(*column)).collect(),
            key: key.iter().map(|column| Arc::from(*column)).collect(),
        };
        let parts: Vec<Part> = shredded(&records, chunk_bytes)
            .iter()
            .flat_map(|batch| normalize(batch, &shape).expect("the batch normalizes"))
            .collect();
        let actual: BTreeMap<_, _> = tables_of(&parts)
            .into_iter()
            .filter(|(_, rows)| !rows.is_empty())
            .collect();
        let expected = sorted(reference::normalize(&records, max_depth, &key, &whole));
        prop_assert_eq!(actual, expected);
    }
}

#[test]
fn the_reference_names_child_tables_by_their_arrays_paths() {
    let tables = reference::normalize(&[json!({"a": {"b": [1]}, "a__b": [2]})], 8, &[], &[]);
    let paths: Vec<&Vec<String>> = tables.keys().collect();
    assert_eq!(paths, [&path(&[]), &path(&["a", "b"]), &path(&["a__b"])]);
}

#[test]
fn fields_of_a_null_object_are_null_whatever_the_array_holds_beneath() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let fields = arrow_schema::Fields::from(vec![arrow_schema::Field::new(
        "a",
        arrow_schema::DataType::Int64,
        true,
    )]);
    let nulls = arrow_buffer::NullBuffer::from(vec![true, false]);
    let object = arrow_array::StructArray::new(fields, vec![values], Some(nulls));
    let batch = RecordBatch::try_from_iter([("o", Arc::new(object) as ArrayRef)]).unwrap();
    let parts = normalize(&batch, &shape(8)).unwrap();
    let a = parts[0].batch.column(0);
    assert_eq!(a.as_primitive::<Int64Type>().value(0), 1);
    assert!(a.is_null(1), "the null object's field is null, not 2");
}

#[test]
fn a_null_field_of_an_object_null_in_other_rows_normalizes() {
    let records = [
        json!({"id": 1, "o": {"a": null, "b": 1}}),
        json!({"id": 2, "o": null}),
    ];
    let parts = parts(&records, &shape(8));
    let columns: Vec<String> = parts[0].columns.iter().map(ToString::to_string).collect();
    assert_eq!(columns, ["id", "o.a", "o.b"]);
}

#[test]
fn columns_keep_the_arrow_types_their_fields_declare() {
    let records = [
        json!({"a": 1, "o": {"m": 1}}),
        json!({"a": "x", "o": {"m": [2]}}),
    ];
    let parts = parts(&records, &shape(8));
    let schema = rdlt_connector::TableSchema::from_arrow(&parts[0].batch.schema()).unwrap();
    let types: Vec<&rdlt_connector::LogicalType> = schema
        .fields()
        .iter()
        .map(rdlt_connector::Field::logical_type)
        .collect();
    assert_eq!(
        types,
        [
            &rdlt_connector::LogicalType::Json,
            &rdlt_connector::LogicalType::Json
        ],
        "mixed values stay JSON, flattened or not"
    );
}

#[test]
fn a_rows_id_does_not_depend_on_the_rows_beside_it() {
    let alone = parts(&[json!({"id": 1, "a": 1, "o": {"b": 2}})], &shape(0));
    let beside = parts(
        &[
            json!({"id": 1, "a": 1, "o": {"b": 2}}),
            json!({"a": "x", "o": [3]}),
        ],
        &shape(0),
    );
    let id = |parts: &[Part]| parts[0].lineage.id.as_binary::<i32>().value(0).to_vec();
    assert_eq!(
        id(&alone),
        id(&beside),
        "values widened to JSON hash as themselves"
    );
}

#[test]
fn rows_that_differ_never_encode_alike() {
    let text = format!("{}t}}", "x".repeat(121));
    let key = format!("\u{1}bs{{{}", "x".repeat(121));
    let first = json!({"a": {}, "b": text});
    let second = json!({"a": {key: true}});
    let ids: Vec<Vec<u8>> = [first, second]
        .iter()
        .map(|record| {
            parts(std::slice::from_ref(record), &shape(0))[0]
                .lineage
                .id
                .as_binary::<i32>()
                .value(0)
                .to_vec()
        })
        .collect();
    assert_ne!(ids[0], ids[1]);
}

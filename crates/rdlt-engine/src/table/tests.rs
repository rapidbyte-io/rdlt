use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::TimestampMicrosecondArray;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int16Array, Int32Array, Int64Array,
    ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field as ArrowField, TimeUnit};
use proptest::prelude::*;
use rdlt_connector::{
    Capabilities, ColumnKey, ColumnPath, DecimalType, Field, GenerationId, LoadId, LogicalType,
    SchemaChanges, SchemaVersion, SegmentId, StreamName, TablePath, TableRef, TableSchema,
    TypeKind,
};

use super::TableView;
use super::convert::{convert, json};
use super::lower::MetaNames;
use super::lowering::{LoweringPlan, Prepared, Stamp};
use super::model::Model;
use super::resolve::{Change, Incoming, Resolution, Resolver, Route, Settings};
use crate::error::ErrorKind;
use crate::naming::Naming;
use crate::plan::StreamPlan;
use crate::policy::{Nested, OnUnsupported, SchemaPolicy, SchemaSettings};

fn capabilities() -> Capabilities {
    let mut capabilities = Capabilities::minimal();
    capabilities.schema_changes = SchemaChanges::all();
    capabilities
}

fn plan() -> StreamPlan {
    StreamPlan::new(StreamName::new("s").unwrap())
}

fn resolver(capabilities: Capabilities, stream: StreamPlan, key: &[&str]) -> Resolver {
    Resolver {
        stream: StreamName::new("s").unwrap(),
        settings: Settings {
            pipeline: SchemaSettings::default(),
            stream,
            key: key.iter().map(|column| ColumnPath::from(*column)).collect(),
        },
        naming: Naming::new(capabilities.identifiers.clone()),
        capabilities: Arc::new(capabilities),
        meta: MetaNames {
            load_id: "_rdlt_load_id".into(),
            loaded_at: "_rdlt_loaded_at".into(),
            seq: (!key.is_empty()).then(|| "_rdlt_seq".into()),
            id: None,
            parent: None,
        },
    }
}

fn schema(fields: &[(&str, LogicalType)]) -> Incoming {
    Incoming::from(
        TableSchema::new(
            fields
                .iter()
                .map(|(name, logical)| Field::new(*name, logical.clone(), true))
                .collect(),
        )
        .unwrap(),
    )
}

/// The model once `resolver` creates the table from `fields`.
fn created(resolver: &Resolver, fields: &[(&str, LogicalType)]) -> Model {
    resolver
        .resolve(&Model::default(), &schema(fields))
        .unwrap()
        .model
}

fn columns(model: &Model) -> Vec<(String, LogicalType)> {
    model
        .columns
        .iter()
        .map(|field| (field.name().to_owned(), field.logical_type().clone()))
        .collect()
}

fn decimal(precision: u8, scale: u8) -> LogicalType {
    LogicalType::Decimal(DecimalType::new(precision, scale).unwrap())
}

#[test]
fn a_new_table_takes_every_column_of_its_first_batch() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let first = schema(&[("Id", LogicalType::Int64), ("name", LogicalType::Utf8)]);
    let resolution = resolver.resolve(&Model::default(), &first).unwrap();
    assert_eq!(resolution.routes, [Route::Column(0), Route::Column(1)]);
    assert_eq!(resolution.changes.len(), 2);
    assert_eq!(resolution.model.version, 1);
    assert_eq!(
        columns(&resolution.model),
        [
            ("Id".to_owned(), LogicalType::Int64),
            ("name".to_owned(), LogicalType::Utf8)
        ]
    );
    assert_eq!(
        resolution.model.names.get(&ColumnPath::from("Id").into()),
        Some("Id")
    );
}

#[test]
fn a_batch_that_fits_its_table_changes_nothing() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(
        &resolver,
        &[("id", LogicalType::Int64), ("note", LogicalType::Utf8)],
    );
    let narrower = schema(&[("note", LogicalType::Null), ("id", LogicalType::Int32)]);
    let resolution = resolver.resolve(&model, &narrower).unwrap();
    assert!(resolution.changes.is_empty());
    assert_eq!(resolution.routes, [Route::Column(1), Route::Column(0)]);
    assert_eq!(resolution.model, model);
}

#[test]
fn new_columns_are_added_and_wider_types_widen_their_column() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("id", LogicalType::Int32)]);
    let wider = schema(&[("id", LogicalType::Int64), ("extra", LogicalType::Utf8)]);
    let resolution = resolver.resolve(&model, &wider).unwrap();
    assert_eq!(resolution.routes, [Route::Column(0), Route::Column(1)]);
    assert_eq!(
        resolution.changes,
        [
            Change::Widen {
                column: 0,
                from: LogicalType::Int32
            },
            Change::Add {
                key: ColumnPath::from("extra").into()
            }
        ]
    );
    assert_eq!(resolution.model.version, 2);
    assert_eq!(
        columns(&resolution.model),
        [
            ("id".to_owned(), LogicalType::Int64),
            ("extra".to_owned(), LogicalType::Utf8)
        ]
    );
}

#[test]
fn changes_the_destination_cannot_apply_go_to_variant_columns() {
    let mut fixed = Capabilities::minimal();
    fixed.schema_changes = SchemaChanges {
        add_column: true,
        widenings: BTreeSet::new(),
    };
    fixed.types.insert(TypeKind::Json);
    let resolver = resolver(fixed, plan(), &[]);
    let model = created(&resolver, &[("amount", LogicalType::Int32)]);
    let wider = resolver
        .resolve(&model, &schema(&[("amount", LogicalType::Int64)]))
        .unwrap();
    assert_eq!(wider.routes, [Route::Column(1)]);
    assert_eq!(
        columns(&wider.model)[1],
        ("amount__int64".to_owned(), LogicalType::Int64)
    );
    let text = resolver
        .resolve(&wider.model, &schema(&[("amount", LogicalType::Utf8)]))
        .unwrap();
    assert_eq!(text.routes, [Route::Column(2)]);
    assert_eq!(
        columns(&text.model)[2],
        ("amount__json".to_owned(), LogicalType::Json)
    );
    let key = ColumnKey::Variant {
        column: ColumnPath::from("amount"),
        kind: TypeKind::Json,
    };
    assert_eq!(text.model.names.get(&key), Some("amount__json"));
    let small = resolver
        .resolve(&text.model, &schema(&[("amount", LogicalType::Int8)]))
        .unwrap();
    assert!(
        small.changes.is_empty(),
        "values the original holds stay in it"
    );
    assert_eq!(small.routes, [Route::Column(0)]);
}

#[test]
fn incompatible_values_go_to_the_json_variant() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("amount", LogicalType::Int64)]);
    let resolution = resolver
        .resolve(&model, &schema(&[("amount", LogicalType::Float64)]))
        .unwrap();
    assert_eq!(resolution.routes, [Route::Column(1)]);
    assert_eq!(
        columns(&resolution.model)[1],
        ("amount__json".to_owned(), LogicalType::Json)
    );
}

#[test]
fn policies_refuse_or_discard_changes() {
    let incoming = schema(&[("id", LogicalType::Utf8), ("extra", LogicalType::Int64)]);
    let cases: [(SchemaSettings, Result<[Route; 2], &str>); 4] = [
        (
            SchemaSettings::new().policy(SchemaPolicy::Freeze),
            Err("schema_frozen"),
        ),
        (
            SchemaSettings::new().policy(SchemaPolicy::DiscardRow),
            Ok([Route::DiscardRows, Route::DiscardRows]),
        ),
        (
            SchemaSettings::new().policy(SchemaPolicy::DiscardValue),
            Ok([Route::DiscardValues, Route::DiscardValues]),
        ),
        (
            SchemaSettings::new().on_unsupported(OnUnsupported::Refuse),
            Err("schema_change_unsupported"),
        ),
    ];
    for (settings, expected) in cases {
        let resolver = resolver(capabilities(), plan().schema(settings), &[]);
        let model = created(&resolver, &[("id", LogicalType::Int64)]);
        match (resolver.resolve(&model, &incoming), expected) {
            (Ok(resolution), Ok(routes)) => {
                assert_eq!(resolution.routes, routes);
                assert!(resolution.changes.is_empty());
            }
            (Err(error), Err(code)) => {
                assert_eq!(error.kind(), ErrorKind::Schema);
                assert_eq!(error.code(), Some(code));
            }
            (result, expected) => panic!("{settings:?}: {result:?}, expected {expected:?}"),
        }
    }
}

#[test]
fn a_frozen_table_still_takes_values_its_columns_hold() {
    let settings = SchemaSettings::new().policy(SchemaPolicy::Freeze);
    let resolver = resolver(capabilities(), plan().schema(settings), &[]);
    let model = created(&resolver, &[("id", LogicalType::Int64)]);
    let resolution = resolver
        .resolve(&model, &schema(&[("id", LogicalType::Int16)]))
        .unwrap();
    assert_eq!(resolution.routes, [Route::Column(0)]);
}

#[test]
fn a_column_policy_overrides_the_stream_policy() {
    let stream = plan()
        .schema(SchemaSettings::new().policy(SchemaPolicy::Freeze))
        .column("loose", SchemaSettings::new().policy(SchemaPolicy::Evolve));
    let resolver = resolver(capabilities(), stream, &[]);
    let model = created(&resolver, &[("id", LogicalType::Int64)]);
    let resolution = resolver
        .resolve(&model, &schema(&[("loose", LogicalType::Utf8)]))
        .unwrap();
    assert_eq!(resolution.routes, [Route::Column(1)]);
}

#[test]
fn a_hint_pins_its_column_type() {
    let stream = plan().hint("amount", decimal(18, 2));
    let resolver = resolver(capabilities(), stream, &[]);
    let model = created(&resolver, &[("amount", LogicalType::Int32)]);
    assert_eq!(columns(&model)[0], ("amount".to_owned(), decimal(18, 2)));
    let wider = resolver
        .resolve(&model, &schema(&[("amount", decimal(30, 2))]))
        .unwrap();
    assert_eq!(
        wider.routes,
        [Route::Column(1)],
        "a hinted column never widens"
    );
    assert_eq!(
        columns(&wider.model)[1],
        ("amount__decimal".to_owned(), decimal(30, 2))
    );
}

#[test]
fn key_columns_never_take_variants() {
    let resolver = resolver(capabilities(), plan(), &["id"]);
    let model = created(&resolver, &[("id", LogicalType::Int32)]);
    let widened = resolver
        .resolve(&model, &schema(&[("id", LogicalType::Int64)]))
        .unwrap();
    assert_eq!(widened.routes, [Route::Column(0)]);
    let error = resolver
        .resolve(&widened.model, &schema(&[("id", LogicalType::Utf8)]))
        .unwrap_err();
    assert_eq!(error.code(), Some("merge_key_changed"));
    assert!(!model.columns[0].is_nullable(), "key columns hold no nulls");
}

#[test]
fn columns_of_only_nulls_create_nothing() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(
        &resolver,
        &[("id", LogicalType::Int64), ("unknown", LogicalType::Null)],
    );
    assert_eq!(model.columns.len(), 1);
    let resolution = resolver
        .resolve(&model, &schema(&[("unknown", LogicalType::Null)]))
        .unwrap();
    assert_eq!(resolution.routes, [Route::Skip]);
    assert!(resolution.changes.is_empty());
}

#[test]
fn added_names_do_not_depend_on_column_order() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("id", LogicalType::Int64)]);
    let forward = schema(&[("A", LogicalType::Utf8), ("a", LogicalType::Utf8)]);
    let backward = schema(&[("a", LogicalType::Utf8), ("A", LogicalType::Utf8)]);
    let names = |resolution: Resolution| resolution.model.names;
    assert_eq!(
        names(resolver.resolve(&model, &forward).unwrap()),
        names(resolver.resolve(&model, &backward).unwrap())
    );
}

fn table(name: &str) -> TableRef {
    TableRef {
        path: TablePath::new([name]).unwrap(),
        name: name.into(),
        version: SchemaVersion(1),
        generation: Some(GenerationId(3)),
        merge: None,
    }
}

fn stamp() -> Stamp {
    Stamp {
        load_id: LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(1_000), 5),
        loaded_at: UNIX_EPOCH + Duration::from_secs(1_000),
        segment: SegmentId(7),
        first_row: 10,
    }
}

/// The values of `batch`'s column `index`, a dictionary of one value per batch encoding values of
/// `values`: the load id and load start (spec §8.5).
fn constant(batch: &RecordBatch, index: usize, values: &DataType) -> ArrayRef {
    let column = batch.column(index);
    assert_eq!(
        column.data_type(),
        &DataType::Dictionary(Box::new(DataType::Int8), Box::new(values.clone()))
    );
    let dictionary = column.as_dictionary::<Int8Type>();
    assert_eq!(
        dictionary.values().len(),
        1,
        "one value for the whole batch"
    );
    assert!(dictionary.keys().iter().all(|key| key == Some(0)));
    arrow_cast::cast(column, values).unwrap()
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

/// Resolves and prepares `batch` for `model`, as a partition does.
fn prepared(resolver: &Resolver, model: &Model, batch: &RecordBatch) -> Prepared {
    let incoming = Incoming::from(TableSchema::from_arrow(&batch.schema()).unwrap());
    let resolution = resolver.resolve(model, &incoming).unwrap();
    let view = Arc::new(TableView::new(&table("t"), resolution.model, resolver));
    LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .prepare(batch, None, &stamp())
        .unwrap()
}

#[test]
fn prepared_batches_convert_exactly_lower_to_the_destination_and_carry_metadata() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(
        &resolver,
        &[("id", LogicalType::Int64), ("amount", decimal(10, 2))],
    );
    let tags =
        ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1), Some(2)]), None]);
    let batch = batch(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as _),
        (
            "amount",
            Arc::new(Int16Array::from(vec![Some(3), None])) as _,
        ),
        ("tags", Arc::new(tags) as _),
    ]);
    let prepared = prepared(&resolver, &model, &batch);
    let out = prepared.batch;
    let schema = out.schema();
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(
        names,
        ["id", "amount", "tags", "_rdlt_load_id", "_rdlt_loaded_at"]
    );
    assert_eq!(out.column(0).as_primitive::<Int64Type>().values(), &[1, 2]);
    assert_eq!(out.column(1).data_type(), &DataType::Decimal128(10, 2));
    let tags = out.column(2).as_string::<i32>();
    assert_eq!(
        tags.value(0),
        "[1,2]",
        "minimal destinations store lists as text"
    );
    assert!(tags.is_null(1));
    let load_ids = constant(&out, 3, &DataType::Utf8);
    assert_eq!(
        load_ids.as_string::<i32>().value(0).len(),
        36,
        "UUIDs lower to hyphenated text"
    );
    let loaded_at = constant(
        &out,
        4,
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    );
    assert_eq!(
        loaded_at
            .as_primitive::<TimestampMicrosecondType>()
            .values(),
        &[1_000_000_000; 2]
    );
    assert_eq!((prepared.discarded_rows, prepared.discarded_values), (0, 0));
}

#[test]
fn types_the_destination_lacks_land_as_text() {
    let mut plain = capabilities();
    plain.types.remove(&TypeKind::Timestamp);
    plain.types.remove(&TypeKind::Decimal);
    let resolver = resolver(plain, plan(), &[]);
    let at = TimestampMicrosecondArray::from(vec![1_500_000]).with_timezone("Europe/Warsaw");
    let amounts = arrow_array::Decimal128Array::from(vec![1234])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let batch = batch(vec![
        ("at", Arc::new(at) as _),
        ("amount", Arc::new(amounts) as _),
    ]);
    let out = prepared(&resolver, &Model::default(), &batch).batch;
    assert_eq!(
        out.column(0).as_string::<i32>().value(0),
        "1970-01-01T01:00:01.500+01:00"
    );
    assert_eq!(out.column(1).as_string::<i32>().value(0), "12.34");
    let loaded_at = constant(&out, 3, &DataType::Utf8);
    let loaded_at = loaded_at.as_string::<i32>().value(0);
    assert!(loaded_at.starts_with("1970-01-01T00:16:40"), "{loaded_at}");
}

#[test]
fn nested_values_stay_native_where_the_destination_stores_them() {
    let mut native = capabilities();
    native.nested.structs = true;
    native.nested.lists = true;
    native
        .types
        .extend([TypeKind::Struct, TypeKind::List, TypeKind::Uuid]);
    let child = ArrowField::new("n", DataType::Int64, true);
    let structs = StructArray::from(vec![(
        Arc::new(child),
        Arc::new(Int64Array::from(vec![4])) as ArrayRef,
    )]);
    let batch = batch(vec![("s", Arc::new(structs) as _)]);
    let natively = resolver(native.clone(), plan(), &[]);
    let out = prepared(&natively, &Model::default(), &batch).batch;
    assert!(matches!(out.column(0).data_type(), DataType::Struct(_)));
    assert_eq!(constant(&out, 1, &DataType::FixedSizeBinary(16)).len(), 1);
    let as_json = plan().column("s", SchemaSettings::new().nested(Nested::Json));
    let as_json = resolver(native, as_json, &[]);
    let out = prepared(&as_json, &Model::default(), &batch).batch;
    assert_eq!(out.column(0).as_string::<i32>().value(0), r#"{"n":4}"#);
}

#[test]
fn discarded_rows_and_values_are_counted() {
    let settings = SchemaSettings::new().policy(SchemaPolicy::DiscardRow);
    let stream = plan().schema(settings).column(
        "drop",
        SchemaSettings::new().policy(SchemaPolicy::DiscardValue),
    );
    let resolver = resolver(capabilities(), stream, &[]);
    let model = created(&resolver, &[("id", LogicalType::Int64)]);
    let batch = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _),
        (
            "new",
            Arc::new(StringArray::from(vec![None, Some("x"), None])) as _,
        ),
        (
            "drop",
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as _,
        ),
    ]);
    let prepared = prepared(&resolver, &model, &batch);
    assert_eq!(prepared.batch.num_rows(), 2);
    assert_eq!(
        prepared
            .batch
            .column(0)
            .as_primitive::<Int64Type>()
            .values(),
        &[1, 3]
    );
    assert_eq!(prepared.discarded_rows, 1);
    assert_eq!(
        prepared.discarded_values, 1,
        "the value in a dropped row is not counted"
    );
}

#[test]
fn a_batch_whose_rows_are_all_discarded_prepares_empty() {
    let settings = SchemaSettings::new().policy(SchemaPolicy::DiscardRow);
    let resolver = resolver(capabilities(), plan().schema(settings), &[]);
    let model = created(&resolver, &[("id", LogicalType::Int64)]);
    let batch = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as _),
        ("new", Arc::new(StringArray::from(vec!["x", "y"])) as _),
    ]);
    let prepared = prepared(&resolver, &model, &batch);
    assert_eq!(prepared.batch.num_rows(), 0);
    assert_eq!(
        prepared.batch.num_columns(),
        3,
        "id and the metadata columns"
    );
    assert_eq!(prepared.discarded_rows, 2);
}

#[test]
fn merge_batches_keep_the_last_row_of_each_key_in_sequence_order() {
    let resolver = resolver(capabilities(), plan(), &["id"]);
    let model = created(
        &resolver,
        &[("id", LogicalType::Int64), ("v", LogicalType::Utf8)],
    );
    let batch = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2, 1])) as _),
        ("v", Arc::new(StringArray::from(vec!["a", "b", "c"])) as _),
    ]);
    let out = prepared(&resolver, &model, &batch).batch;
    assert_eq!(out.column(0).as_primitive::<Int64Type>().values(), &[2, 1]);
    assert_eq!(out.column(1).as_string::<i32>().value(1), "c");
    let seq = out.column_by_name("_rdlt_seq").unwrap().as_binary::<i32>();
    let decode = |row: usize| {
        let bytes = seq.value(row);
        (
            u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            u64::from_be_bytes(bytes[8..].try_into().unwrap()),
        )
    };
    assert_eq!([decode(0), decode(1)], [(7, 11), (7, 12)]);
}

#[test]
fn merge_batches_without_a_whole_key_are_refused() {
    let resolver = resolver(capabilities(), plan(), &["id"]);
    let model = created(
        &resolver,
        &[("id", LogicalType::Int64), ("v", LogicalType::Utf8)],
    );
    let cases = [
        (
            batch(vec![("v", Arc::new(StringArray::from(vec!["a"])) as _)]),
            "merge_key_missing",
        ),
        (
            batch(vec![(
                "id",
                Arc::new(Int64Array::from(vec![None, Some(1)])) as _,
            )]),
            "merge_key_null",
        ),
    ];
    for (batch, code) in cases {
        let incoming = Incoming::from(TableSchema::from_arrow(&batch.schema()).unwrap());
        let resolution = resolver.resolve(&model, &incoming).unwrap();
        let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver));
        let error = LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
            .prepare(&batch, None, &stamp())
            .unwrap_err();
        assert_eq!(error.code(), Some(code));
    }
}

#[test]
fn views_name_their_merge_key_and_version() {
    let resolver = resolver(capabilities(), plan(), &["id"]);
    let model = created(
        &resolver,
        &[("v", LogicalType::Utf8), ("id", LogicalType::Int64)],
    );
    let view = TableView::new(&table("t"), model, &resolver);
    let merge = view.table.merge.clone().unwrap();
    assert_eq!(merge.columns, [Arc::from("id")]);
    assert_eq!(merge.seq.as_ref(), "_rdlt_seq");
    assert_eq!(view.key, [1]);
    assert_eq!(view.table.version, SchemaVersion(1));
    assert_eq!(view.table.generation, Some(GenerationId(3)));
}

#[test]
fn structs_widen_field_by_field_and_json_embeds_its_text() {
    let from = LogicalType::Struct(
        rdlt_connector::Fields::new(vec![Field::new("a", LogicalType::Int32, true)]).unwrap(),
    );
    let to = from.join(&LogicalType::Struct(
        rdlt_connector::Fields::new(vec![Field::new("b", LogicalType::Utf8, true)]).unwrap(),
    ));
    let child = ArrowField::new("a", DataType::Int32, true);
    let array: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(child),
        Arc::new(Int32Array::from(vec![5])) as ArrayRef,
    )]));
    let widened = convert(&array, &from, &to).unwrap();
    assert_eq!(widened.data_type(), &to.to_arrow());
    assert_eq!(
        json(&widened, &to).unwrap().as_string::<i32>().value(0),
        r#"{"a":5,"b":null}"#
    );
    let payload = LogicalType::Struct(
        rdlt_connector::Fields::new(vec![
            Field::new("j", LogicalType::Json, true),
            Field::new("u", LogicalType::Uuid, true),
        ])
        .unwrap(),
    );
    let DataType::Struct(fields) = payload.to_arrow() else {
        unreachable!("struct types are Arrow structs")
    };
    let nested: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(StringArray::from(vec![r#"{"k":[1]}"#])),
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0_u8; 16]].into_iter()).unwrap()),
        ],
        None,
    ));
    assert_eq!(
        json(&nested, &payload).unwrap().as_string::<i32>().value(0),
        r#"{"j":{"k":[1]},"u":"00000000-0000-0000-0000-000000000000"}"#
    );
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.5]));
    assert_eq!(
        json(&floats, &LogicalType::Float64)
            .unwrap()
            .as_string::<i32>()
            .value(0),
        "1.5"
    );
}

/// The rows a merge keeps, the slow way: the last row of each key, in row order.
fn fold(keys: &[i64]) -> Vec<i64> {
    let mut last: BTreeMap<i64, usize> = BTreeMap::new();
    for (row, key) in keys.iter().enumerate() {
        last.insert(*key, row);
    }
    let mut rows: Vec<usize> = last.into_values().collect();
    rows.sort_unstable();
    rows.into_iter().map(|row| keys[row]).collect()
}

proptest! {
    #[test]
    fn compaction_keeps_what_a_sort_and_fold_keeps(keys in proptest::collection::vec(0_i64..6, 1..40)) {
        let resolver = resolver(capabilities(), plan(), &["id"]);
        let model = created(&resolver, &[("id", LogicalType::Int64)]);
        let batch = batch(vec![("id", Arc::new(Int64Array::from(keys.clone())) as _)]);
        let out = prepared(&resolver, &model, &batch).batch;
        let kept = out.column(0).as_primitive::<Int64Type>().values().to_vec();
        prop_assert_eq!(kept, fold(&keys));
    }
}

#[test]
fn models_come_from_committed_state() {
    use rdlt_connector::{NameMap, TableState};
    assert_eq!(Model::from_state(None).unwrap(), Model::default());
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("id", LogicalType::Int64)]);
    let state = TableState {
        schema: Some((SchemaVersion(model.version), model.schema())),
        physical: Some("t".into()),
        names: model.names.clone(),
    };
    assert_eq!(Model::from_state(Some(&state)).unwrap(), model);
    let named_only = TableState {
        schema: None,
        ..state.clone()
    };
    let unnamed = Model::from_state(Some(&named_only)).unwrap();
    assert!(!unnamed.created() && unnamed.columns.is_empty());
    assert_eq!(unnamed.names, model.names);
    let orphan = TableState {
        names: NameMap::default(),
        ..state
    };
    let error = Model::from_state(Some(&orphan)).unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Destination, Some("state_invalid"))
    );
}

#[test]
fn a_views_physical_schema_is_its_lowered_columns_then_metadata() {
    let resolver = resolver(capabilities(), plan(), &["id"]);
    let model = created(
        &resolver,
        &[("id", LogicalType::Int64), ("tags", LogicalType::Json)],
    );
    let view = TableView::new(&table("t"), model, &resolver);
    let physical = view.physical_schema();
    let columns: Vec<(&str, &LogicalType, bool)> = physical
        .fields()
        .iter()
        .map(|field| (field.name(), field.logical_type(), field.is_nullable()))
        .collect();
    assert_eq!(
        columns,
        [
            ("id", &LogicalType::Int64, false),
            ("tags", &LogicalType::Utf8, true),
            ("_rdlt_load_id", &LogicalType::Utf8, false),
            (
                "_rdlt_loaded_at",
                &LogicalType::Timestamp(rdlt_connector::TimeUnit::Microsecond, Some("UTC".into())),
                false
            ),
            ("_rdlt_seq", &LogicalType::Binary, false),
        ]
    );
}

#[test]
fn a_source_column_named_like_a_metadata_column_is_renamed() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("_rdlt_load_id", LogicalType::Int64)]);
    let name = model.columns[0].name();
    assert_ne!(name, "_rdlt_load_id");
    assert!(name.starts_with("_rdlt_load_id_"), "{name}");
}

#[test]
fn json_lowers_to_json_where_either_capability_declares_it() {
    use super::lower::lower;
    let mut none = Capabilities::minimal();
    none.types.remove(&TypeKind::Json);
    let mut typed = none.clone();
    typed.types.insert(TypeKind::Json);
    let mut nested = none.clone();
    nested.nested.json = true;
    for (capabilities, expected) in [
        (&none, LogicalType::Utf8),
        (&typed, LogicalType::Json),
        (&nested, LogicalType::Json),
    ] {
        assert_eq!(
            lower(&LogicalType::Json, Nested::Native, capabilities),
            expected
        );
    }
    let object = LogicalType::Struct(
        rdlt_connector::Fields::new(vec![Field::new("n", LogicalType::Int64, true)]).unwrap(),
    );
    let mut structs = typed.clone();
    structs.nested.structs = true;
    structs.types.insert(TypeKind::Struct);
    assert_eq!(lower(&object, Nested::Native, &structs), object);
    assert_eq!(lower(&object, Nested::Json, &structs), LogicalType::Json);
    assert_eq!(lower(&object, Nested::Native, &typed), LogicalType::Json);
}

#[test]
fn a_merge_table_created_without_its_key_column_refuses_batches() {
    let resolver = resolver(capabilities(), plan(), &["id"]);
    let model = created(&resolver, &[("v", LogicalType::Utf8)]);
    let batch = batch(vec![("v", Arc::new(StringArray::from(vec!["a"])) as _)]);
    let incoming = Incoming::from(TableSchema::from_arrow(&batch.schema()).unwrap());
    let resolution = resolver.resolve(&model, &incoming).unwrap();
    let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver));
    assert!(view.key.is_empty());
    let error = LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .prepare(&batch, None, &stamp())
        .unwrap_err();
    assert_eq!(error.code(), Some("merge_key_missing"));
}

#[test]
fn lists_of_structs_widen_item_by_item() {
    let item = |fields: Vec<Field>| {
        LogicalType::List(Box::new(Field::new(
            "item",
            LogicalType::Struct(rdlt_connector::Fields::new(fields).unwrap()),
            true,
        )))
    };
    let from = item(vec![Field::new("a", LogicalType::Int32, true)]);
    let to = item(vec![
        Field::new("a", LogicalType::Int64, true),
        Field::new("b", LogicalType::Utf8, true),
    ]);
    let DataType::List(item_field) = from.to_arrow() else {
        unreachable!("list types are Arrow lists")
    };
    let DataType::Struct(fields) = item_field.data_type().clone() else {
        unreachable!("the item is a struct")
    };
    let items = StructArray::new(fields, vec![Arc::new(Int32Array::from(vec![1, 2]))], None);
    let two = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(0), Some(0)])]);
    let offsets = two.offsets().clone();
    let list: ArrayRef = Arc::new(ListArray::new(item_field, offsets, Arc::new(items), None));
    let widened = convert(&list, &from, &to).unwrap();
    assert_eq!(widened.data_type(), &to.to_arrow());
    assert_eq!(
        json(&widened, &to).unwrap().as_string::<i32>().value(0),
        r#"[{"a":1,"b":null},{"a":2,"b":null}]"#
    );
}

#[test]
fn large_text_normalizes_to_its_plain_type() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("note", LogicalType::Utf8)]);
    let large = arrow_array::LargeStringArray::from(vec!["x"]);
    let batch = batch(vec![("note", Arc::new(large) as _)]);
    let out = prepared(&resolver, &model, &batch).batch;
    assert_eq!(out.column(0).data_type(), &DataType::Utf8);
    assert_eq!(out.column(0).as_string::<i32>().value(0), "x");
}

#[test]
fn an_existing_variant_widens_where_the_destination_can_and_else_values_go_to_json() {
    let decimal_only = |widens: bool| {
        let mut capabilities = Capabilities::minimal();
        capabilities.types.insert(TypeKind::Json);
        capabilities.schema_changes = SchemaChanges {
            add_column: true,
            widenings: if widens {
                BTreeSet::from([(TypeKind::Decimal, TypeKind::Decimal)])
            } else {
                BTreeSet::new()
            },
        };
        capabilities
    };
    for (widens, route, column) in [
        (true, 1, ("amount__decimal".to_owned(), decimal(20, 2))),
        (false, 2, ("amount__json".to_owned(), LogicalType::Json)),
    ] {
        let resolver = resolver(decimal_only(widens), plan(), &[]);
        let model = created(&resolver, &[("amount", LogicalType::Int8)]);
        let first = resolver
            .resolve(&model, &schema(&[("amount", decimal(10, 2))]))
            .unwrap();
        assert_eq!(
            columns(&first.model)[1],
            ("amount__decimal".to_owned(), decimal(10, 2))
        );
        let wider = resolver
            .resolve(&first.model, &schema(&[("amount", decimal(20, 2))]))
            .unwrap();
        assert_eq!(wider.routes, [Route::Column(route)], "widens: {widens}");
        assert_eq!(columns(&wider.model)[route], column, "widens: {widens}");
    }
}

#[test]
fn values_a_variant_holds_go_there_and_only_to_their_own_columns_variants() {
    let mut fixed = Capabilities::minimal();
    fixed.types.insert(TypeKind::Json);
    fixed.schema_changes.widenings.clear();
    let resolver = resolver(fixed, plan(), &[]);
    let model = created(
        &resolver,
        &[("a", LogicalType::Int32), ("b", LogicalType::Int32)],
    );
    let a_wide = resolver
        .resolve(&model, &schema(&[("a", LogicalType::Int64)]))
        .unwrap();
    assert_eq!(a_wide.routes, [Route::Column(2)]);
    let again = resolver
        .resolve(&a_wide.model, &schema(&[("a", LogicalType::Int64)]))
        .unwrap();
    assert!(again.changes.is_empty(), "a__int64 already holds them");
    assert_eq!(again.routes, [Route::Column(2)]);
    let b_wide = resolver
        .resolve(&a_wide.model, &schema(&[("b", LogicalType::Int64)]))
        .unwrap();
    assert_eq!(b_wide.routes, [Route::Column(3)], "never a's variant");
    assert_eq!(columns(&b_wide.model)[3].0, "b__int64");
}

#[test]
fn a_value_its_column_cannot_represent_fails_the_batch() {
    use arrow_array::TimestampSecondArray;
    let resolver = resolver(capabilities(), plan(), &[]);
    let nanos = LogicalType::Timestamp(rdlt_connector::TimeUnit::Nanosecond, None);
    let model = created(&resolver, &[("at", nanos)]);
    let year_3000 = TimestampSecondArray::from(vec![32_503_680_000]);
    let batch = batch(vec![("at", Arc::new(year_3000) as _)]);
    let incoming = Incoming::from(TableSchema::from_arrow(&batch.schema()).unwrap());
    let resolution = resolver.resolve(&model, &incoming).unwrap();
    assert!(
        resolution.changes.is_empty(),
        "the column's type holds the batch's"
    );
    let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver));
    let error = LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
        .prepare(&batch, None, &stamp())
        .unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Schema, Some("value_unrepresentable"))
    );
}

#[test]
fn constant_metadata_columns_are_built_once_and_sliced_per_batch() {
    let mut uuids = capabilities();
    uuids.types.insert(TypeKind::Uuid);
    let resolver = resolver(uuids, plan(), &[]);
    let ids = |rows: i64| {
        batch(vec![(
            "id",
            Arc::new(Int64Array::from_iter_values(0..rows)) as _,
        )])
    };
    let incoming = Incoming::from(TableSchema::from_arrow(&ids(1).schema()).unwrap());
    let resolution = resolver.resolve(&Model::default(), &incoming).unwrap();
    let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver));
    let lowering = LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes);
    // The keys of the load id column, and the buffer they sit in.
    let keys = |rows: i64, stamp: &Stamp| {
        let prepared = lowering.prepare(&ids(rows), None, stamp).unwrap().batch;
        let column = prepared.column(1).as_dictionary::<Int8Type>().clone();
        assert_eq!(column.len(), usize::try_from(rows).unwrap());
        assert!(column.keys().iter().all(|key| key == Some(0)));
        column.keys().values().inner().as_ptr()
    };
    let stamp = stamp();
    let hundred = keys(100, &stamp);
    assert_eq!(keys(100, &stamp), hundred, "the same size reuses them");
    assert_eq!(
        keys(60, &stamp),
        hundred,
        "a batch over half their size reuses them"
    );
    assert_ne!(
        keys(40, &stamp),
        hundred,
        "a much smaller batch gets its own"
    );
    let forty = keys(40, &stamp);
    assert_ne!(keys(41, &stamp), forty, "a larger batch gets its own");
    let later = Stamp {
        load_id: LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(2_000), 6),
        ..stamp
    };
    let forty_one = keys(41, &stamp);
    assert_ne!(keys(41, &later), forty_one, "another load gets its own");
    let prepared = lowering.prepare(&ids(3), None, &later).unwrap().batch;
    let load_ids = constant(&prepared, 1, &DataType::FixedSizeBinary(16));
    assert_eq!(
        load_ids.as_fixed_size_binary().value(2),
        later.load_id.as_bytes()
    );
}

/// `batch` with its metadata columns decoded to the values they encode.
fn decoded(batch: &RecordBatch) -> RecordBatch {
    let fields: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, values) => {
                field.as_ref().clone().with_data_type(*values.clone())
            }
            _ => field.as_ref().clone(),
        })
        .collect();
    let columns = batch
        .columns()
        .iter()
        .zip(&fields)
        .map(|(column, field)| arrow_cast::cast(column, field.data_type()).unwrap())
        .collect();
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap()
}

proptest! {
    #[test]
    fn a_plan_reused_across_batches_lowers_each_as_a_fresh_plan_does(
        batches in proptest::collection::vec((1_i64..300, 0_u128..3, 0_u64..3), 1..12),
    ) {
        let mut uuids = capabilities();
        uuids.types.insert(TypeKind::Uuid);
        let resolver = resolver(uuids, plan(), &[]);
        let ids = |rows: i64| {
            batch(vec![("id", Arc::new(Int64Array::from_iter_values(0..rows)) as _)])
        };
        let incoming = Incoming::from(TableSchema::from_arrow(&ids(1).schema()).unwrap());
        let resolution = resolver.resolve(&Model::default(), &incoming).unwrap();
        let view = Arc::new(TableView::new(&table("t"), resolution.model, &resolver));
        let fresh = || {
            LoweringPlan::new(resolver.stream.clone(), Arc::clone(&view), incoming.clone(), resolution.routes.clone())
        };
        let reused = fresh();
        // The load id and load start vary apart, so each is checked on its own.
        for (rows, load, start) in batches {
            let stamp = Stamp {
                load_id: LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(1_000), load),
                loaded_at: UNIX_EPOCH + Duration::from_secs(1_000 + start),
                ..stamp()
            };
            let batch = ids(rows);
            let lowered = decoded(&reused.prepare(&batch, None, &stamp).unwrap().batch);
            prop_assert_eq!(&lowered, &decoded(&fresh().prepare(&batch, None, &stamp).unwrap().batch));
            let load_ids = lowered.column(1).as_fixed_size_binary();
            prop_assert!(load_ids.iter().all(|id| id == Some(stamp.load_id.as_bytes().as_slice())));
            let loaded_at = lowered.column(2).as_primitive::<TimestampMicrosecondType>();
            let micros = i64::try_from(stamp.loaded_at.duration_since(UNIX_EPOCH).unwrap().as_micros()).unwrap();
            prop_assert!(loaded_at.iter().all(|at| at == Some(micros)));
        }
    }
}

#[test]
fn only_a_normalized_table_is_created_by_a_batch_without_values() {
    let plain = resolver(capabilities(), plan(), &[]);
    let nulls = schema(&[("unknown", LogicalType::Null)]);
    let resolution = plain.resolve(&Model::default(), &nulls).unwrap();
    assert!(
        !resolution.model.created(),
        "a plain table waits for a value"
    );
    let mut normalized = resolver(capabilities(), plan(), &[]);
    normalized.meta.id = Some("_rdlt_id".into());
    let first = normalized.resolve(&Model::default(), &nulls).unwrap();
    assert_eq!(first.model.version, 1, "its rows' lineage creates it");
    let wider = normalized
        .resolve(&first.model, &schema(&[("a", LogicalType::Int64)]))
        .unwrap();
    assert_eq!(wider.model.version, 2);
    let again = normalized
        .resolve(&wider.model, &schema(&[("a", LogicalType::Int64)]))
        .unwrap();
    assert_eq!(again.model.version, 2, "a batch that fits changes nothing");
}

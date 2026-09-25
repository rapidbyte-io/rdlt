//! Schema resolution and evolution through whole runs: tables created from batches or declared
//! schemas, changes applied, refused or discarded, variant columns, names and nested values.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::{Field, IdentifierCase, LogicalType, TableSchema, TypeKind};
use rdlt_connector_reference::schema;
use rdlt_engine::{ErrorKind, OnUnsupported, RunStatus, SchemaPolicy, SchemaSettings};
use serde_json::json;

use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::limited;
use crate::support::{commit_every, engine, memory, pipeline, published_json, stream};

/// A batch of `columns`.
pub(crate) fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).expect("the test batch is valid")
}

pub(crate) fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn small(values: &[i32]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

pub(crate) fn text(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// The column names and types of `table` in `store`.
fn columns(store: &str, table: &str) -> Vec<(String, LogicalType)> {
    schema(store, table)
        .expect("the table exists")
        .fields()
        .iter()
        .filter(|field| !field.name().starts_with("_rdlt_"))
        .map(|field| (field.name().to_owned(), field.logical_type().clone()))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_stream_without_a_declared_schema_creates_its_table_from_its_first_batch() {
    let first = batch(vec![("id", ints(&[1, 2])), ("name", text(&["a", "b"]))]);
    let source = batches("created", vec![BatchStream::new("events", vec![first])]).await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline("created", [stream("events")]),
            source,
            memory("created").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(
        columns("created", "events"),
        [
            ("id".to_owned(), LogicalType::Int64),
            ("name".to_owned(), LogicalType::Utf8)
        ]
    );
    let with_meta = schema("created", "events").unwrap();
    let meta: Vec<&str> = with_meta
        .fields()
        .iter()
        .map(Field::name)
        .filter(|name| name.starts_with("_rdlt_"))
        .collect();
    assert_eq!(meta, ["_rdlt_load_id", "_rdlt_loaded_at"]);
    assert_eq!(
        published_json("created", "events"),
        [json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})]
    );
}

#[tokio::test(start_paused = true)]
async fn batches_that_change_the_table_evolve_it() {
    let stream_batches = vec![
        batch(vec![("id", small(&[1]))]),
        batch(vec![("id", ints(&[1 << 40])), ("note", text(&["n"]))]),
        batch(vec![("id", text(&["x"]))]),
    ];
    let source = batches("evolve", vec![BatchStream::new("events", stream_batches)]).await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline("evolve", [stream("events")]),
            source,
            memory("evolve").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(
        columns("evolve", "events"),
        [
            ("id".to_owned(), LogicalType::Int64),
            ("note".to_owned(), LogicalType::Utf8),
            ("id__json".to_owned(), LogicalType::Json)
        ]
    );
    assert_eq!(
        published_json("evolve", "events"),
        [
            json!({"id": 1_i64 << 40, "note": "n"}),
            json!({"id": 1}),
            json!({"id__json": "\"x\""}),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_destination_that_cannot_widen_a_column_gets_a_variant_column() {
    let destination = limited(memory("variant").await, |capabilities| {
        capabilities.schema_changes.widenings.clear();
    });
    let stream_batches = vec![
        batch(vec![("id", small(&[1]))]),
        batch(vec![("id", ints(&[2]))]),
    ];
    let source = batches("variant", vec![BatchStream::new("events", stream_batches)]).await;
    let outcome = engine(commit_every(10))
        .run(pipeline("variant", [stream("events")]), source, destination)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(
        columns("variant", "events"),
        [
            ("id".to_owned(), LogicalType::Int32),
            ("id__int64".to_owned(), LogicalType::Int64)
        ]
    );
    assert_eq!(
        published_json("variant", "events"),
        [json!({"id": 1}), json!({"id__int64": 2})]
    );
}

#[tokio::test(start_paused = true)]
async fn refused_changes_fail_the_run_without_retrying() {
    let changing = || {
        vec![
            batch(vec![("id", ints(&[1]))]),
            batch(vec![("id", ints(&[2])), ("extra", text(&["e"]))]),
            batch(vec![("id", text(&["x"]))]),
        ]
    };
    let freeze = SchemaSettings::new().policy(SchemaPolicy::Freeze);
    let refuse = SchemaSettings::new().on_unsupported(OnUnsupported::Refuse);
    let cases = [
        ("frozen", freeze, "schema_frozen"),
        ("refusing", refuse, "schema_change_unsupported"),
    ];
    for (name, settings, code) in cases {
        let source = batches(name, vec![BatchStream::new("events", changing())]).await;
        let outcome = engine(commit_every(1))
            .run(
                pipeline(name, [stream("events").schema(settings)]),
                source,
                memory(name).await,
            )
            .await;
        let error = outcome.error.expect("the run fails");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Schema, Some(code)),
            "{name}"
        );
        assert_eq!(
            outcome.report.attempts.len(),
            1,
            "schema errors are not retried"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn discarded_rows_and_values_are_counted_in_the_report() {
    let stream_batches = vec![
        batch(vec![("id", ints(&[1]))]),
        batch(vec![
            ("id", ints(&[2, 3])),
            (
                "extra",
                Arc::new(StringArray::from(vec![Some("e"), None])) as _,
            ),
        ]),
    ];
    let discard_rows = SchemaSettings::new().policy(SchemaPolicy::DiscardRow);
    let discard_values = SchemaSettings::new().policy(SchemaPolicy::DiscardValue);
    let cases = [
        (
            "discard_rows",
            discard_rows,
            (1, 0),
            vec![json!({"id": 1}), json!({"id": 3})],
        ),
        (
            "discard_values",
            discard_values,
            (0, 1),
            vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})],
        ),
    ];
    for (name, settings, discarded, rows) in cases {
        let source = batches(
            name,
            vec![BatchStream::new("events", stream_batches.clone())],
        )
        .await;
        let outcome = engine(commit_every(10))
            .run(
                pipeline(name, [stream("events").schema(settings)]),
                source,
                memory(name).await,
            )
            .await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{name}");
        let report = &outcome.report.streams["events"];
        assert_eq!(
            (report.discarded_rows, report.discarded_values),
            discarded,
            "{name}"
        );
        assert_eq!(published_json(name, "events"), rows, "{name}");
        assert_eq!(
            columns(name, "events").len(),
            1,
            "{name}: no column was added"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn identifiers_follow_the_destination_rules() {
    let destination = limited(memory("identifiers").await, |capabilities| {
        capabilities.identifiers.case = IdentifierCase::Lower;
        capabilities.identifiers.reserved = BTreeSet::from(["select".to_owned()]);
    });
    let first = batch(vec![("Order Id", ints(&[1])), ("SELECT", ints(&[2]))]);
    let source = batches(
        "identifiers",
        vec![BatchStream::new("Events.Daily", vec![first])],
    )
    .await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline("identifiers", [stream("Events.Daily")]),
            source,
            destination,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    let names: Vec<String> = columns("identifiers", "events_daily")
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(names[0], "order_id");
    assert!(
        names[1].starts_with("select_") && names[1].len() == "select_".len() + 6,
        "{names:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn nested_values_land_natively_or_as_json() {
    let child = ArrowField::new("n", DataType::Int64, true);
    let structs: ArrayRef = Arc::new(StructArray::from(vec![(Arc::new(child), ints(&[4]))]));
    for (name, native) in [("nested_native", true), ("nested_json", false)] {
        let destination = limited(memory(name).await, |capabilities| {
            capabilities.nested.structs = native;
            if !native {
                capabilities.types.remove(&TypeKind::Struct);
            }
        });
        let first = batch(vec![("payload", Arc::clone(&structs))]);
        let source = batches(name, vec![BatchStream::new("events", vec![first])]).await;
        let outcome = engine(commit_every(10))
            .run(pipeline(name, [stream("events")]), source, destination)
            .await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{name}");
        let stored = columns(name, "events")[0].1.clone();
        let expected = if native {
            json!({"payload": {"n": 4}})
        } else {
            json!({"payload": "{\"n\":4}"})
        };
        assert_eq!(
            stored.kind() == TypeKind::Struct,
            native,
            "{name}: {stored}"
        );
        assert_eq!(published_json(name, "events"), [expected], "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_declared_schema_creates_the_table_before_any_row_and_changes_evolve_it_across_runs() {
    let declared = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap();
    let empty = BatchStream::new("events", Vec::new()).declared(declared);
    let engine = engine(commit_every(10));
    let first = engine
        .run(
            pipeline("declared", [stream("events")]),
            batches("declared", vec![empty]).await,
            memory("declared").await,
        )
        .await;
    assert_eq!(first.report.status, RunStatus::Succeeded);
    assert_eq!(
        columns("declared", "events"),
        [("id".to_owned(), LogicalType::Int64)]
    );
    let wider = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
    ])
    .unwrap();
    let changed = BatchStream::new("events", Vec::new()).declared(wider.clone());
    let frozen = engine
        .run(
            pipeline(
                "declared",
                [stream("events").schema(SchemaSettings::new().policy(SchemaPolicy::Freeze))],
            ),
            batches("declared", vec![changed.clone()]).await,
            memory("declared").await,
        )
        .await;
    assert_eq!(
        frozen.error.expect("freeze refuses").code(),
        Some("schema_frozen")
    );
    let evolved = engine
        .run(
            pipeline("declared", [stream("events")]),
            batches("declared", vec![changed]).await,
            memory("declared").await,
        )
        .await;
    assert_eq!(evolved.report.status, RunStatus::Succeeded);
    assert_eq!(columns("declared", "events").len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_read_whose_rows_after_its_last_checkpoint_are_all_discarded_counts_them() {
    let name = "discard_every_row";
    let discard_rows = SchemaSettings::new().policy(SchemaPolicy::DiscardRow);
    let run = |stream_batches: Vec<RecordBatch>| async move {
        let events = BatchStream::new("events", stream_batches).unchecked();
        engine(commit_every(10))
            .run(
                pipeline(name, [stream("events").schema(discard_rows)]),
                batches(name, vec![events]).await,
                memory(name).await,
            )
            .await
    };
    let created = run(vec![batch(vec![("id", ints(&[1]))])]).await;
    assert_eq!(created.report.status, RunStatus::Succeeded);
    let extra: ArrayRef = Arc::new(StringArray::from(vec![Some("e"), Some("f")]));
    let discarding = run(vec![batch(vec![("id", ints(&[2, 3])), ("extra", extra)])]).await;
    assert_eq!(discarding.report.status, RunStatus::Succeeded);
    let report = discarding
        .report
        .streams
        .get("events")
        .cloned()
        .unwrap_or_default();
    assert_eq!((report.rows, report.discarded_rows), (0, 2));
    assert_eq!(published_json(name, "events"), vec![json!({"id": 1})]);
}

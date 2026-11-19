//! Schema resolution and evolution through whole runs: tables created from batches or declared
//! schemas, changes applied, refused or discarded, variant columns, names and nested values.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int32Array, Int64Array, NullArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::{
    Destination, Field, IdentifierCase, LogicalType, OpenContext, PipelineId, PipelineState,
    TablePath, TableSchema, TypeKind,
};
use rdlt_connector_reference::schema;
use rdlt_engine::{
    ErrorKind, OnUnsupported, RunOutcome, RunStatus, SchemaPolicy, SchemaSettings, WriteMode, codes,
};
use serde_json::json;

use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::{limited, null};
use crate::support::targets::Target;
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
    assert_eq!(meta, ["_rdlt_load_epoch", "_rdlt_loaded_at"]);
    assert_eq!(
        published_json("created", "events"),
        [json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})]
    );
}

/// Whether the state `destination` holds for pipeline `name` records the schema of `table`.
async fn recorded(destination: &dyn Destination, name: &str, table: &str) -> bool {
    let context = OpenContext {
        pipeline: PipelineId::parse(name).expect("a valid pipeline"),
    };
    let opened = destination.open(&context).await.expect("it opens");
    let state = PipelineState::from_records(&opened.state).expect("the state reads");
    let path = TablePath::new([table]).expect("a valid path");
    state
        .tables
        .get(&path)
        .is_some_and(|table| table.schema.is_some())
}

/// The stream `events`, whose one flush holds two rows of nothing but nulls, as an Arrow batch of
/// the null type or as JSON.
fn only_nulls(json: bool) -> BatchStream {
    if json {
        BatchStream::json("events", &["{\"deleted_at\":null}\n{\"deleted_at\":null}"])
    } else {
        let nulls: ArrayRef = Arc::new(NullArray::new(2));
        BatchStream::new("events", vec![batch(vec![("deleted_at", nulls)])])
    }
}

#[tokio::test(start_paused = true)]
async fn a_first_flush_of_nothing_but_nulls_creates_its_table_and_records_it() {
    for target in Target::IN_PROCESS {
        for json in [false, true] {
            let store = format!("only_nulls_{json}");
            let source = batches(&store, vec![only_nulls(json)]).await;
            let destination = target.destination(&store).await;
            let plan = pipeline(&store, [stream("events")]);
            let outcome = engine(commit_every(10))
                .run(plan, source, Arc::clone(&destination))
                .await;
            let case = format!("{target:?}, json {json}: {:?}", outcome.error);
            assert_eq!(outcome.report.status, RunStatus::Succeeded, "{case}");
            let published = target.published(&store, "events");
            let rows: usize = published.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(rows, 2, "{case}");
            assert!(
                recorded(destination.as_ref(), &store, "events").await,
                "{case}"
            );
        }
    }
    // A destination refusing a table writer for a table no schema change created.
    let source = batches("only_nulls_null", vec![only_nulls(false)]).await;
    let plan = pipeline("only_nulls_null", [stream("events")]);
    let outcome = engine(commit_every(10))
        .run(plan, source, null().await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    // A merge stream whose key holds nothing but nulls is refused, its rows loaded nowhere.
    let keyed = BatchStream::json("events", &["{\"id\":null}"]).primary_key(&["id"]);
    let source = batches("only_null_keys", vec![keyed]).await;
    let plan = pipeline("only_null_keys", [stream("events").write(WriteMode::Merge)]);
    let destination = memory("only_null_keys").await;
    let outcome = engine(commit_every(10))
        .run(plan, source, destination)
        .await;
    let refused = outcome.error.expect("a key of nulls is refused");
    let refusal = (refused.kind(), refused.code());
    assert_eq!(
        refusal,
        (ErrorKind::Schema, Some(codes::MERGE_KEY_MISSING)),
        "{refused:?}"
    );
    assert!(published_json("only_null_keys", "events").is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_widen_the_destination_stores_alike_writes_under_its_new_version_with_no_change() {
    // The null destination stores a struct as text, so the struct gaining a field widens it
    // in place to the same storage: a new version, with no change sent.
    let a = || Arc::new(ArrowField::new("a", DataType::Int64, true));
    let first: ArrayRef = Arc::new(StructArray::from(vec![(a(), ints(&[1]))]));
    let b = Arc::new(ArrowField::new("b", DataType::Utf8, true));
    let second: ArrayRef = Arc::new(StructArray::from(vec![
        (a(), ints(&[2])),
        (b, text(&["x"])),
    ]));
    let pushes = vec![batch(vec![("s", first)]), batch(vec![("s", second)])];
    let source = batches("widened_alike", vec![BatchStream::new("events", pushes)]).await;
    let plan = pipeline("widened_alike", [stream("events")]);
    let outcome = engine(commit_every(1))
        .run(plan, source, null().await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test(start_paused = true)]
async fn a_column_that_held_only_nulls_is_a_change_once_it_holds_a_value() {
    let pushes = || {
        let nulls: ArrayRef = Arc::new(NullArray::new(2));
        vec![
            batch(vec![("deleted_at", nulls)]),
            batch(vec![("deleted_at", ints(&[7]))]),
        ]
    };
    let freeze = SchemaSettings::new().policy(SchemaPolicy::Freeze);
    for (name, settings) in [
        ("nulls_evolve", SchemaSettings::new()),
        ("nulls_freeze", freeze),
    ] {
        let source = batches(name, vec![BatchStream::new("events", pushes())]).await;
        let plan = pipeline(name, [stream("events").schema(settings)]);
        let outcome = engine(commit_every(1))
            .run(plan, source, memory(name).await)
            .await;
        if name == "nulls_freeze" {
            let error = outcome.error.expect("a frozen table takes no new column");
            assert_eq!(error.code(), Some(codes::SCHEMA_FROZEN), "{error:?}");
            continue;
        }
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
        let added = [("deleted_at".to_owned(), LogicalType::Int64)];
        assert_eq!(columns(name, "events"), added);
        let rows = [json!({"deleted_at": 7}), json!({}), json!({})];
        assert_eq!(published_json(name, "events"), rows);
    }
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
        ("frozen", freeze, codes::SCHEMA_FROZEN),
        ("refusing", refuse, codes::SCHEMA_CHANGE_UNSUPPORTED),
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
        Some(codes::SCHEMA_FROZEN)
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

/// Loads into `name` a batch of `id` and `until`, each with one value, under `settings`.
async fn until(name: &str, id: i64, until: ArrayRef, settings: SchemaSettings) -> RunOutcome {
    let pushed = vec![batch(vec![("id", ints(&[id])), ("until", until)])];
    let source = batches(name, vec![BatchStream::new("events", pushed)]).await;
    engine(commit_every(1))
        .run(
            pipeline(name, [stream("events").schema(settings)]),
            source,
            memory(name).await,
        )
        .await
}

/// Loads a column `until` of seconds holding the last day of the year 9999, then in another run
/// one of nanoseconds: the destination refuses to widen the column, since nanoseconds do not
/// reach the value it holds.
async fn beyond_nanoseconds(name: &str, settings: SchemaSettings) -> RunOutcome {
    use arrow_array::{TimestampNanosecondArray, TimestampSecondArray};
    let seconds: ArrayRef = Arc::new(TimestampSecondArray::from(vec![253_402_214_400]));
    let first = until(name, 1, seconds, settings).await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    let nanoseconds: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![5]));
    until(name, 2, nanoseconds, settings).await
}

#[tokio::test(start_paused = true)]
async fn a_widen_the_destination_refuses_routes_the_values_to_a_variant() {
    let name = "widen_refused";
    let outcome = beyond_nanoseconds(name, SchemaSettings::new()).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let held = columns(name, "events");
    assert_eq!(held.len(), 3, "{held:?}");
    assert_eq!(
        held[1],
        (
            "until".to_owned(),
            LogicalType::Timestamp(rdlt_connector::TimeUnit::Second, None)
        )
    );
    assert_eq!(
        held[2].1,
        LogicalType::Timestamp(rdlt_connector::TimeUnit::Nanosecond, None)
    );
    assert_eq!(published_json(name, "events").len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_widen_the_destination_refuses_where_no_variant_may_take_it_is_refused_typed() {
    let refuse = SchemaSettings::new().on_unsupported(OnUnsupported::Refuse);
    let outcome = beyond_nanoseconds("widen_refused_typed", refuse).await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Schema, Some(codes::SCHEMA_CHANGE_UNSUPPORTED))
    );
    assert!(!error.is_retryable());
}

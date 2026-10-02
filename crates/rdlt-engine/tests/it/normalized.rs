//! Normalized streams against every reference destination.

use rdlt_engine::{Nested, RunStatus, SchemaPolicy, SchemaSettings, StreamPlan, WriteMode};
use serde_json::json;

use crate::support::batches::{BatchStream, batches};
use crate::support::targets::Target;
use crate::support::{commit_every, each, engine, pipeline, stream};

fn normalized(name: &str) -> StreamPlan {
    stream(name).schema(SchemaSettings::new().nested(Nested::normalize()))
}

/// Loads `streams` into `target`'s `store` as `plans` say, and checks the run succeeded.
async fn load(target: Target, store: &str, streams: Vec<BatchStream>, plans: Vec<StreamPlan>) {
    let source = batches(&target.name(store), streams).await;
    let outcome = engine(commit_every(1))
        .run(
            pipeline(store, plans),
            source,
            target.destination(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{target:?}: {:?}",
        outcome.error
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_takes_tables_of_arrays_that_hold_only_arrays() {
    each(
        Target::IN_PROCESS,
        takes_tables_of_arrays_that_hold_only_arrays,
    )
    .await;
}

pub(crate) async fn takes_tables_of_arrays_that_hold_only_arrays(target: Target) {
    let events = r#"{"id":1,"m":[[1,2],[3]]}"#;
    let only = r#"{"items":[{"sku":"x"}]}"#;
    load(
        target,
        "arrays_of_arrays",
        vec![
            BatchStream::json("events", &[events]),
            BatchStream::json("only", &[only]),
        ],
        vec![normalized("events"), normalized("only")],
    )
    .await;
    let store = "arrays_of_arrays";
    assert_eq!(target.rows(store, "events__m"), 2, "{target:?}");
    assert_eq!(
        target.json(store, "events__m__value"),
        [
            json!({"value": 1}),
            json!({"value": 2}),
            json!({"value": 3})
        ],
        "{target:?}"
    );
    assert_eq!(target.rows(store, "only"), 1, "{target:?}");
    assert_eq!(
        target.json(store, "only__items"),
        [json!({"sku": "x"})],
        "{target:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_takes_normalize_turned_on_for_an_existing_stream() {
    each(
        Target::IN_PROCESS,
        takes_normalize_turned_on_for_an_existing_stream,
    )
    .await;
}

pub(crate) async fn takes_normalize_turned_on_for_an_existing_stream(target: Target) {
    let store = "turned_on";
    load(
        target,
        store,
        vec![BatchStream::json("events", &[r#"{"id":1}"#])],
        vec![stream("events")],
    )
    .await;
    load(
        target,
        store,
        vec![BatchStream::json(
            "events",
            &[r#"{"id":2,"items":[{"sku":"x"}]}"#],
        )],
        vec![normalized("events")],
    )
    .await;
    assert_eq!(target.rows(store, "events"), 2, "{target:?}");
    assert_eq!(
        target.json(store, "events__items"),
        [json!({"sku": "x"})],
        "{target:?}"
    );
}

fn merged(name: &str) -> StreamPlan {
    normalized(name).write(WriteMode::Merge).key(["id"])
}

#[tokio::test(start_paused = true)]
async fn every_destination_replaces_a_roots_children_when_it_merges() {
    each(Target::IN_PROCESS, replaces_a_roots_children_when_it_merges).await;
}

pub(crate) async fn replaces_a_roots_children_when_it_merges(target: Target) {
    let store = "merged_children";
    let first = [
        r#"{"id":1,"items":[{"sku":"a"},{"sku":"b","tags":["t"]}]}"#,
        r#"{"id":2,"items":[{"sku":"c"}]}"#,
        r#"{"id":3,"items":[{"sku":"d"}]}"#,
    ]
    .join("\n");
    load(
        target,
        store,
        vec![BatchStream::json("events", &[&first])],
        vec![merged("events")],
    )
    .await;
    let second = [
        r#"{"id":1,"items":[{"sku":"e"}]}"#,
        r#"{"id":2,"items":[]}"#,
    ]
    .join("\n");
    load(
        target,
        store,
        vec![BatchStream::json("events", &[&second])],
        vec![merged("events")],
    )
    .await;
    assert_eq!(target.rows(store, "events"), 3, "{target:?}");
    assert_eq!(
        target.json(store, "events__items"),
        [json!({"sku": "d"}), json!({"sku": "e"})],
        "{target:?}: merging 1 and 2 replaced their items; 3 kept its own"
    );
    assert_eq!(
        target.rows(store, "events__items__tags"),
        0,
        "{target:?}: grandchildren follow their root"
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_keeps_the_children_of_a_keys_last_row_in_a_commit() {
    each(
        Target::IN_PROCESS,
        keeps_the_children_of_a_keys_last_row_in_a_commit,
    )
    .await;
}

pub(crate) async fn keeps_the_children_of_a_keys_last_row_in_a_commit(target: Target) {
    let store = "last_row";
    let push = [
        r#"{"id":1,"items":[{"sku":"old"}]}"#,
        r#"{"id":1,"items":[]}"#,
        r#"{"id":2,"items":[{"sku":"x"}]}"#,
        r#"{"id":2,"items":[{"sku":"y"}]}"#,
    ]
    .join("\n");
    load(
        target,
        store,
        vec![BatchStream::json("events", &[&push])],
        vec![merged("events")],
    )
    .await;
    assert_eq!(target.rows(store, "events"), 2, "{target:?}");
    assert_eq!(
        target.json(store, "events__items"),
        [json!({"sku": "y"})],
        "{target:?}"
    );
}

fn dropping_merge() -> StreamPlan {
    stream("events")
        .schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(SchemaPolicy::DiscardRow),
        )
        .write(WriteMode::Merge)
        .key(["id"])
}

#[tokio::test(start_paused = true)]
async fn every_destination_keeps_the_children_of_rows_after_a_dropped_one() {
    each(
        Target::IN_PROCESS,
        keeps_the_children_of_rows_after_a_dropped_one,
    )
    .await;
}

pub(crate) async fn keeps_the_children_of_rows_after_a_dropped_one(target: Target) {
    let plan = dropping_merge;
    let store = "after_dropped";
    let first = r#"{"id":1,"items":[{"sku":"z"}]}"#;
    load(
        target,
        store,
        vec![BatchStream::json("events", &[first])],
        vec![plan()],
    )
    .await;
    let second = [
        r#"{"id":1,"items":[{"sku":"a"}]}"#,
        r#"{"id":2,"extra":1,"items":[{"sku":"b"}]}"#,
        r#"{"id":3,"items":[{"sku":"c"}]}"#,
    ]
    .join("\n");
    load(
        target,
        store,
        vec![BatchStream::json("events", &[&second])],
        vec![plan()],
    )
    .await;
    assert_eq!(target.rows(store, "events"), 2, "{target:?}");
    assert_eq!(
        target.json(store, "events__items"),
        [json!({"sku": "a"}), json!({"sku": "c"})],
        "{target:?}: row 2 went with its item; row 3 kept its own"
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_drops_only_the_rows_carrying_a_change_among_rows_sharing_a_key() {
    each(
        Target::IN_PROCESS,
        drops_only_the_rows_carrying_a_change_among_rows_sharing_a_key,
    )
    .await;
}

pub(crate) async fn drops_only_the_rows_carrying_a_change_among_rows_sharing_a_key(target: Target) {
    let store = "shared_key";
    let first = r#"{"id":1,"v":0,"items":[{"sku":"z"}]}"#;
    let plan = || vec![dropping_merge()];
    load(
        target,
        store,
        vec![BatchStream::json("events", &[first])],
        plan(),
    )
    .await;
    let second = [
        r#"{"id":1,"v":1,"extra":1,"items":[{"sku":"a"}]}"#,
        r#"{"id":1,"v":2,"tags":["t"],"items":[{"sku":"b"}]}"#,
        r#"{"id":1,"v":3,"items":[{"sku":"c"}]}"#,
    ]
    .join("\n");
    load(
        target,
        store,
        vec![BatchStream::json("events", &[&second])],
        plan(),
    )
    .await;
    assert_eq!(
        target.json(store, "events"),
        [json!({"id": 1, "v": 3})],
        "{target:?}: rows 1 and 2 carried a new column and a new array"
    );
    assert_eq!(
        target.json(store, "events__items"),
        [json!({"sku": "c"})],
        "{target:?}: the kept row keeps its children"
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_takes_a_normalized_stream_through_one_writer_open() {
    each(
        Target::IN_PROCESS,
        takes_a_normalized_stream_through_one_writer_open,
    )
    .await;
}

/// `values`, sorted as text.
fn sorted(mut values: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    values.sort_by_key(ToString::to_string);
    values
}

/// The values of `column` in `rows`, sorted as text.
fn values(rows: &[serde_json::Value], column: &str) -> Vec<serde_json::Value> {
    sorted(rows.iter().map(|row| row[column].clone()).collect())
}

pub(crate) async fn takes_a_normalized_stream_through_one_writer_open(target: Target) {
    // Every push writes the stream's table and its two child tables, so one writer open closes
    // one of them for each other it writes.
    let pushes: Vec<String> = (0..20)
        .map(|push| {
            json!({ "id": push % 5, "n": push, "items": [{ "sku": format!("s{push}") }], "tags": [push] })
                .to_string()
        })
        .collect();
    let pushes: Vec<&str> = pushes.iter().map(String::as_str).collect();
    for (store, plan) in [
        ("one_writer_append", normalized("events")),
        ("one_writer_merge", merged("events")),
    ] {
        let growth = rdlt_engine::GrowthLimits::new(1024, 1).expect("valid limits");
        let config = commit_every(7).lanes(1).growth(growth);
        let source = batches(
            &target.name(store),
            vec![BatchStream::json("events", &pushes)],
        )
        .await;
        let outcome = engine(config)
            .run(
                pipeline(store, [plan]),
                source,
                target.destination(store).await,
            )
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{target:?}: {:?}",
            outcome.error
        );
        let (kept, from): (usize, i64) = if store.ends_with("merge") {
            (5, 15)
        } else {
            (20, 0)
        };
        let numbers = sorted((from..20).map(|push| json!(push)).collect());
        let skus = sorted((from..20).map(|push| json!(format!("s{push}"))).collect());
        assert_eq!(target.rows(store, "events"), kept, "{target:?} {store}");
        assert_eq!(
            values(&target.json(store, "events"), "n"),
            numbers,
            "{target:?} {store}"
        );
        assert_eq!(
            values(&target.json(store, "events__items"), "sku"),
            skus,
            "{target:?} {store}"
        );
        assert_eq!(
            values(&target.json(store, "events__tags"), "value"),
            numbers,
            "{target:?} {store}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_merge_key_stored_as_json_on_a_normalized_stream_is_refused_before_any_row_lands() {
    // Ids that mix kinds make the key a column of JSON, whose values `1` and `1.0` merge apart
    // at the root while their children follow one root id.
    let store = "json_key";
    let first = [
        r#"{"id":1,"items":[{"sku":"victim1"},{"sku":"victim2"}]}"#,
        r#"{"id":"x","items":[{"sku":"sx"}]}"#,
    ]
    .join("\n");
    let source = batches(store, vec![BatchStream::json("events", &[&first])]).await;
    let outcome = engine(commit_every(1))
        .run(
            pipeline(store, vec![merged("events")]),
            source,
            crate::support::memory(store).await,
        )
        .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (rdlt_engine::ErrorKind::Schema, Some("merge_key_json"))
    );
    assert_eq!(crate::support::published_rows(store, "events"), 0);
}

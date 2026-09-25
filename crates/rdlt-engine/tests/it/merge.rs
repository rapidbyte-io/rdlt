//! Merge streams through whole runs: one row per key, the newest winning, across commits, runs
//! and retries.

use std::sync::Arc;

use arrow_array::{Int64Array, StringArray};
use rdlt_connector::WriteModes;
use rdlt_engine::{ErrorKind, RunStatus, WriteMode};
use serde_json::json;

use crate::schema::{batch, ints, text};
use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::{Step, failing, limited};
use crate::support::{commit_every, engine, memory, pipeline, published_json, retrying, stream};

fn merging(name: &str) -> rdlt_engine::StreamPlan {
    stream(name).write(WriteMode::Merge)
}

#[tokio::test(start_paused = true)]
async fn a_merge_keeps_the_newest_row_of_each_key_across_commits_and_runs() {
    let first = vec![
        batch(vec![("id", ints(&[1, 2])), ("v", text(&["a", "b"]))]),
        batch(vec![("id", ints(&[1])), ("v", text(&["c"]))]),
    ];
    let keyed = |batches| BatchStream::new("events", batches).primary_key(&["id"]);
    let engine = engine(commit_every(1));
    let outcome = engine
        .run(
            pipeline("merge", [merging("events")]),
            batches("merge", vec![keyed(first)]).await,
            memory("merge").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(
        published_json("merge", "events"),
        [json!({"id": 1, "v": "c"}), json!({"id": 2, "v": "b"})]
    );
    let second = vec![batch(vec![
        ("id", ints(&[2, 3, 2])),
        ("v", text(&["x", "y", "z"])),
    ])];
    let outcome = engine
        .run(
            pipeline("merge", [merging("events")]),
            batches("merge", vec![keyed(second)]).await,
            memory("merge").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(
        published_json("merge", "events"),
        [
            json!({"id": 1, "v": "c"}),
            json!({"id": 2, "v": "z"}),
            json!({"id": 3, "v": "y"})
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn the_plan_key_wins_over_the_primary_key() {
    let rows = vec![batch(vec![
        ("id", ints(&[1, 2])),
        ("group", ints(&[7, 7])),
        ("v", text(&["a", "b"])),
    ])];
    let source = batches(
        "plan_key",
        vec![BatchStream::new("events", rows).primary_key(&["id"])],
    )
    .await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline("plan_key", [merging("events").key(["group"])]),
            source,
            memory("plan_key").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(
        published_json("plan_key", "events"),
        [json!({"group": 7, "id": 2, "v": "b"})]
    );
}

#[tokio::test(start_paused = true)]
async fn a_merge_is_idempotent_when_a_commit_response_is_lost() {
    let rows = vec![
        batch(vec![("id", ints(&[1, 2])), ("v", text(&["a", "b"]))]),
        batch(vec![("id", ints(&[2])), ("v", text(&["c"]))]),
    ];
    let source = batches(
        "merge_retry",
        vec![BatchStream::new("events", rows).primary_key(&["id"])],
    )
    .await;
    let destination = failing(memory("merge_retry").await, Step::LoseResponse);
    let outcome = engine(retrying(3))
        .run(
            pipeline("merge-retry", [merging("events")]),
            source,
            destination,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(
        published_json("merge_retry", "events"),
        [json!({"id": 1, "v": "a"}), json!({"id": 2, "v": "c"})]
    );
}

#[tokio::test(start_paused = true)]
async fn merge_streams_the_run_cannot_load_are_refused() {
    let unkeyed = batches(
        "unkeyed",
        vec![BatchStream::new(
            "events",
            vec![batch(vec![("id", ints(&[1]))])],
        )],
    )
    .await;
    let no_key = engine(commit_every(10))
        .run(
            pipeline("unkeyed", [merging("events")]),
            unkeyed,
            memory("unkeyed").await,
        )
        .await;
    let appending = limited(memory("no_merge").await, |capabilities| {
        capabilities.write_modes = WriteModes {
            append: true,
            ..WriteModes::default()
        };
    });
    let source = batches(
        "no_merge",
        vec![
            BatchStream::new("events", vec![batch(vec![("id", ints(&[1]))])]).primary_key(&["id"]),
        ],
    )
    .await;
    let unsupported = engine(commit_every(10))
        .run(pipeline("no-merge", [merging("events")]), source, appending)
        .await;
    for (outcome, code) in [
        (no_key, "merge_key_missing"),
        (unsupported, "write_mode_unsupported"),
    ] {
        let error = outcome.error.expect("the run fails");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some(code))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn merge_rows_without_a_whole_key_fail_the_stream() {
    let nulls = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![Some(1), None])) as _),
        ("v", Arc::new(StringArray::from(vec!["a", "b"])) as _),
    ]);
    let source = batches(
        "null_key",
        vec![BatchStream::new("events", vec![nulls]).primary_key(&["id"])],
    )
    .await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline("null-key", [merging("events")]),
            source,
            memory("null_key").await,
        )
        .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Schema, Some("merge_key_null"))
    );
}

#[tokio::test(start_paused = true)]
async fn the_later_batch_of_one_segment_wins_its_key() {
    let rows = vec![
        batch(vec![("id", ints(&[1, 2])), ("v", text(&["a", "b"]))]),
        batch(vec![("id", ints(&[1])), ("v", text(&["c"]))]),
    ];
    let stream = BatchStream::new("events", rows)
        .primary_key(&["id"])
        .one_segment();
    let outcome = engine(commit_every(10))
        .run(
            pipeline("one-segment", [merging("events")]),
            batches("one_segment", vec![stream]).await,
            memory("one_segment").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(
        published_json("one_segment", "events"),
        [json!({"id": 1, "v": "c"}), json!({"id": 2, "v": "b"})]
    );
}

/// Loads `pushes` of `events` into `target`'s `store` as `plan` says, and checks the run
/// succeeded.
async fn load_into(
    target: crate::support::targets::Target,
    store: &str,
    pushes: &[&str],
    plan: rdlt_engine::StreamPlan,
) {
    let source = batches(
        &target.name(store),
        vec![BatchStream::json("events", pushes)],
    )
    .await;
    let outcome = engine(commit_every(1))
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
}

#[tokio::test(start_paused = true)]
async fn every_destination_merges_into_a_table_it_appended_to_and_appends_again() {
    let keyed = || merging("events").key(["id"]);
    for target in crate::support::targets::Target::ALL {
        let store = "switched";
        let appended = [r#"{"id":1,"v":"a"}"#, r#"{"id":1,"v":"b"}"#];
        load_into(target, store, &appended, stream("events")).await;
        let merged = [r#"{"id":1,"v":"c"}"#, r#"{"id":2,"v":"d"}"#];
        load_into(target, store, &merged, keyed()).await;
        let mut rows = target.json(store, "events");
        rows.sort_by_key(ToString::to_string);
        assert_eq!(
            rows,
            [json!({"id": 1, "v": "c"}), json!({"id": 2, "v": "d"})],
            "{target:?}: the merge replaced both appended rows of key 1"
        );
        load_into(target, store, &[r#"{"id":1,"v":"e"}"#], stream("events")).await;
        assert_eq!(target.rows(store, "events"), 3, "{target:?}");
    }
}

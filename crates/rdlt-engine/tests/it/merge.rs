//! Merge streams through whole runs: one row per key, the newest winning, across commits, runs
//! and retries.

use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use rdlt_connector::WriteModes;
use rdlt_engine::{ErrorKind, RunStatus, WriteMode};
use serde_json::json;

use crate::schema::{batch, ints, text};
use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::{Step, failing, limited};
use crate::support::targets::Target;
use crate::support::{
    commit_every, each, engine, memory, pipeline, published_json, retrying, stream,
};

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
async fn a_merge_key_other_than_the_one_a_table_was_merged_by_is_refused() {
    let rows = |ids: &[i64], tenants: &[i64]| {
        vec![batch(vec![("id", ints(ids)), ("tenant", ints(tenants))])]
    };
    let run = |events: BatchStream, plan: rdlt_engine::StreamPlan| async move {
        engine(commit_every(1))
            .run(
                pipeline("rekeyed", [plan]),
                batches("rekeyed", vec![events]).await,
                memory("rekeyed").await,
            )
            .await
    };
    let first = BatchStream::new("events", rows(&[1, 2, 3, 4], &[7, 7, 7, 7])).primary_key(&["id"]);
    let outcome = run(first, merging("events")).await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    // The catalog's key changes, or the plan names another: either would merge the next rows
    // by a key the stored rows were not merged by.
    let catalog = BatchStream::new("events", rows(&[5], &[7])).primary_key(&["tenant"]);
    let planned = BatchStream::new("events", rows(&[5], &[7])).primary_key(&["id"]);
    for (events, plan) in [
        (catalog, merging("events")),
        (planned, merging("events").key(["tenant"])),
    ] {
        let outcome = run(events, plan).await;
        assert_eq!(outcome.report.status, RunStatus::Failed);
        let error = outcome.error.expect("the run failed");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some("table_key_mismatch"))
        );
    }
    assert_eq!(published_json("rekeyed", "events").len(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_keyed_write_by_an_empty_catalog_key_is_refused() {
    for write in [WriteMode::Merge, WriteMode::History] {
        let store = format!("keyless_{write:?}").to_lowercase();
        let rows = vec![batch(vec![("id", ints(&[1, 1])), ("v", text(&["a", "b"]))])];
        let events = BatchStream::new("events", rows).primary_key(&[]);
        let outcome = engine(commit_every(1))
            .run(
                pipeline(&store.replace('_', "-"), [stream("events").write(write)]),
                batches(&store, vec![events]).await,
                memory(&store).await,
            )
            .await;
        assert_eq!(outcome.report.status, RunStatus::Failed, "{write:?}");
        let error = outcome.error.expect("the run failed");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some("merge_key_missing")),
            "{write:?}"
        );
    }
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
async fn load_into(target: Target, store: &str, pushes: &[&str], plan: rdlt_engine::StreamPlan) {
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
    each(
        Target::IN_PROCESS,
        merges_into_a_table_it_appended_to_and_appends_again,
    )
    .await;
}

pub(crate) async fn merges_into_a_table_it_appended_to_and_appends_again(target: Target) {
    let keyed = || merging("events").key(["id"]);
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

fn decimals(values: &[i128], precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .expect("a valid decimal"),
    )
}

#[tokio::test(start_paused = true)]
async fn a_key_whose_stored_rendering_its_new_type_changes_is_refused() {
    // SQLite stores decimals as text, so 1.50 at scale 4 would store as 1.5000 and match nothing.
    let target = Target::Sqlite;
    let store = target.name("merge_key_widened");
    let run = |pushed| {
        let store = store.clone();
        async move {
            let keyed = BatchStream::new("events", vec![pushed]).primary_key(&["k"]);
            engine(commit_every(1))
                .run(
                    pipeline("merge-key-widened", [merging("events")]),
                    batches(&store, vec![keyed]).await,
                    target.destination("merge_key_widened").await,
                )
                .await
        }
    };
    let first = run(batch(vec![
        ("k", decimals(&[150], 10, 2)),
        ("v", ints(&[1])),
    ]))
    .await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    let second = run(batch(vec![
        ("k", decimals(&[15_000], 12, 4)),
        ("v", ints(&[2])),
    ]))
    .await;
    assert_eq!(second.report.status, RunStatus::Failed);
    let error = second.error.expect("the run failed");
    assert_eq!(error.kind(), ErrorKind::Schema, "{error}");
    assert_eq!(error.code(), Some("merge_key_changed"), "{error}");
    assert_eq!(target.json("merge_key_widened", "events").len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_key_stored_by_value_widens_and_keeps_one_row_per_key() {
    // Integers store alike at any width, and the memory destination keeps decimals as decimals.
    let run = |pushed, store: &'static str| async move {
        let keyed = BatchStream::new("events", vec![pushed]).primary_key(&["k"]);
        engine(commit_every(1))
            .run(
                pipeline("merge-key-kept", [merging("events")]),
                batches(store, vec![keyed]).await,
                memory(store).await,
            )
            .await
    };
    let narrow: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![7]));
    let wide: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let scaled = [decimals(&[150], 10, 2), decimals(&[15_000], 12, 4)];
    for (store, [before, after]) in [
        ("merge_key_ints", [narrow, wide]),
        ("merge_key_decimals", scaled),
    ] {
        let first = run(batch(vec![("k", before), ("v", ints(&[1]))]), store).await;
        assert_eq!(
            first.report.status,
            RunStatus::Succeeded,
            "{:?}",
            first.error
        );
        let second = run(batch(vec![("k", after), ("v", ints(&[2]))]), store).await;
        assert_eq!(
            second.report.status,
            RunStatus::Succeeded,
            "{:?}",
            second.error
        );
        assert_eq!(published_json(store, "events").len(), 1, "{store}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_column_named_as_the_merge_ranks_its_rows_keeps_every_key_on_sqlite() {
    let target = Target::Sqlite;
    let store = target.name("merge_rank_column");
    let pushed = batch(vec![
        ("id", ints(&[1, 2, 3])),
        ("_rdlt_rank", ints(&[7, 1, 5])),
    ]);
    let keyed = BatchStream::new("events", vec![pushed]).primary_key(&["id"]);
    let outcome = engine(commit_every(10))
        .run(
            pipeline("merge-rank-column", [merging("events")]),
            batches(&store, vec![keyed]).await,
            target.destination("merge_rank_column").await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(target.ids("merge_rank_column", "events"), [1, 2, 3]);
}

/// Runs `pushed`, keyed by `k`, into the SQLite store `store`, merged.
async fn sqlite_keyed(store: &'static str, pushed: RecordBatch) -> rdlt_engine::RunOutcome {
    let target = Target::Sqlite;
    let keyed = BatchStream::new("events", vec![pushed]).primary_key(&["k"]);
    engine(commit_every(1))
        .run(
            pipeline(store, [merging("events")]),
            batches(&target.name(store), vec![keyed]).await,
            target.destination(store).await,
        )
        .await
}

#[tokio::test(start_paused = true)]
async fn an_integer_key_widens_on_sqlite_to_a_wider_integer() {
    let narrow: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![7]));
    let first = sqlite_keyed(
        "key_int_wider",
        batch(vec![("k", narrow), ("v", ints(&[1]))]),
    )
    .await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    let wide: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let wider = sqlite_keyed("key_int_wider", batch(vec![("k", wide), ("v", ints(&[2]))])).await;
    assert_eq!(
        wider.report.status,
        RunStatus::Succeeded,
        "{:?}",
        wider.error
    );
    assert_eq!(Target::Sqlite.json("key_int_wider", "events").len(), 1);
}

#[tokio::test(start_paused = true)]
async fn an_integer_key_never_changes_on_sqlite_to_a_type_it_cannot_widen_to_or_renders() {
    // A decimal SQLite stores as text; a float it stores by value, but cannot widen to.
    let changed = [
        ("key_int_decimal", decimals(&[70_000], 12, 4)),
        (
            "key_int_float",
            Arc::new(arrow_array::Float64Array::from(vec![7.5])) as ArrayRef,
        ),
    ];
    for (store, after) in changed {
        let int: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![7]));
        let first = sqlite_keyed(store, batch(vec![("k", int), ("v", ints(&[1]))])).await;
        assert_eq!(
            first.report.status,
            RunStatus::Succeeded,
            "{:?}",
            first.error
        );
        let second = sqlite_keyed(store, batch(vec![("k", after), ("v", ints(&[2]))])).await;
        let error = second.error.expect("the run failed");
        assert_eq!(error.code(), Some("merge_key_changed"), "{store}: {error}");
    }
}

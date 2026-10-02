//! History streams: every version of each key, a change closing its key's version
//! where the next begins, across commits, runs, retries and destinations.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use rdlt_connector::{CommitMeta, ReadMode, WriteModes};
use rdlt_connector_reference::changes::{ChangedStream, Version, history};
use rdlt_engine::{DeleteMode, ErrorKind, ResetScope, RunStatus, WriteMode};

use crate::changes::{changes, orders};
use crate::schema::{batch, ints, text};
use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::limited;
use crate::support::faults::{Fault, Rule, failing_commits};
use crate::support::targets::Target;
use crate::support::{commit_every, each, engine, memory, pipeline, retrying, stream};

/// A change stream whose changes carry when they happened and set whole rows, as history needs,
/// truncated at 90.
fn timed() -> ChangedStream {
    ChangedStream {
        captured: 10,
        changed_at: true,
        partial: false,
        keys: 20,
        ..orders(&[90])
    }
}

/// `column` of `batch` in microseconds, cast from however the destination stores a timestamp.
fn micros(batch: &RecordBatch, column: &str) -> Vec<Option<i64>> {
    let values = batch.column_by_name(column).expect("a history column");
    let at = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let at = arrow_cast::cast(values, &at).expect("a timestamp");
    let at = arrow_cast::cast(&at, &DataType::Int64).expect("microseconds");
    at.as_primitive::<Int64Type>().iter().collect()
}

/// The versions `batches` publish, as the reference model writes them, sorted.
fn versions(batches: &[RecordBatch]) -> Vec<Version> {
    let mut versions = Vec::new();
    for batch in batches {
        let column = |name: &str, logical: &DataType| -> ArrayRef {
            let values = batch
                .column_by_name(name)
                .expect("a history table's column");
            arrow_cast::cast(values, logical).expect("a castable column")
        };
        let ids = column("id", &DataType::Int64);
        let values = column("value", &DataType::Utf8);
        let n = column("n", &DataType::Int64);
        let current = column("_rdlt_is_current", &DataType::Boolean);
        let (ids, values) = (ids.as_primitive::<Int64Type>(), values.as_string::<i32>());
        let (n, current) = (n.as_primitive::<Int64Type>(), current.as_boolean());
        let (from, to) = (
            micros(batch, "_rdlt_valid_from"),
            micros(batch, "_rdlt_valid_to"),
        );
        let deleted = batch.column_by_name("_rdlt_deleted_at");
        for row in 0..batch.num_rows() {
            versions.push(Version {
                id: ids.value(row),
                from: from[row]
                    .and_then(|at| u64::try_from(at).ok())
                    .unwrap_or(u64::MAX),
                value: values.is_valid(row).then(|| values.value(row).to_owned()),
                n: n.value(row),
                to: to[row].and_then(|at| u64::try_from(at).ok()),
                current: current.value(row),
                deleted: deleted.is_some_and(|deleted| deleted.is_valid(row)),
            });
        }
    }
    versions.sort();
    versions
}

/// A version as a test reads it back: its key, value, whether it is current, and when it began
/// and ended, in microseconds.
type Span = (i64, String, bool, Option<i64>, Option<i64>);

/// Each version `table` of `store` publishes, sorted.
fn timeline(store: &str, table: &str) -> Vec<Span> {
    let mut rows = Vec::new();
    for batch in &rdlt_connector_reference::published(store, table) {
        let column = |name: &str| batch.column_by_name(name).expect("a published column");
        let ids = arrow_cast::cast(column("id"), &DataType::Int64).expect("integer ids");
        let values = column("v").as_string::<i32>().clone();
        let current = column("_rdlt_is_current").as_boolean().clone();
        let (from, to) = (
            micros(batch, "_rdlt_valid_from"),
            micros(batch, "_rdlt_valid_to"),
        );
        for row in 0..batch.num_rows() {
            let id = ids.as_primitive::<Int64Type>().value(row);
            rows.push((
                id,
                values.value(row).to_owned(),
                current.value(row),
                from[row],
                to[row],
            ));
        }
    }
    rows.sort();
    rows
}

/// A run keeping `events` as history in a pipeline of `name`, into `destination`.
async fn kept(
    name: &str,
    events: BatchStream,
    destination: Arc<dyn rdlt_connector::Destination>,
) -> rdlt_engine::RunOutcome {
    let plan = stream("events").write(WriteMode::History);
    let plan = if name.starts_with("normalized") {
        plan.schema(normalize())
    } else {
        plan
    };
    engine(commit_every(10))
        .run(
            pipeline(&name.replace('_', "-"), [plan]),
            batches(name, vec![events]).await,
            destination,
        )
        .await
}

fn history_of(name: &str, deletes: DeleteMode) -> rdlt_engine::StreamPlan {
    stream(name)
        .read(ReadMode::Cdc)
        .write(WriteMode::History)
        .deletes(deletes)
}

#[tokio::test]
async fn every_destination_keeps_each_version_a_change_stream_makes() {
    each(Target::IN_PROCESS, |target| async move {
        for (deletes, soft) in [(DeleteMode::Hard, false), (DeleteMode::Soft, true)] {
            let store = format!("history_changes_{soft}");
            let spec = timed();
            let plan = pipeline("history", [history_of("orders", deletes)]);
            // Over two hundred changes in commits of sixty-four: several commits, each of
            // which a files destination syncs to disk, which is what the test's time is.
            let outcome = engine(commit_every(64))
                .run(
                    plan,
                    changes(8, &spec).await,
                    target.destination(&store).await,
                )
                .await;
            assert_eq!(
                outcome.report.status,
                RunStatus::Succeeded,
                "{target:?}: {:?}",
                outcome.error
            );
            let published = versions(&target.published(&store, "orders"));
            assert_eq!(
                published,
                history(8, &spec, soft),
                "{target:?} soft: {soft}"
            );
        }
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_history_keeps_each_version_once_through_lost_and_failed_commits_and_later_runs() {
    let spec = timed();
    // The third commit lands but its response is lost; the fifth fails before it lands.
    let rule: Arc<Rule> = Arc::new(|n: usize, _: &CommitMeta| match n {
        3 => Fault::After,
        5 => Fault::Before,
        _ => Fault::None,
    });
    let destination = failing_commits(memory("history_retried").await, rule);
    let plan = || pipeline("history-retried", [history_of("orders", DeleteMode::Hard)]);
    let outcome = engine(retrying(4))
        .run(plan(), changes(9, &spec).await, destination)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let expected = history(9, &spec, false);
    let published = versions(&rdlt_connector_reference::published(
        "history_retried",
        "orders",
    ));
    assert_eq!(published, expected);
    // A later run reads nothing new, and changes nothing.
    let again = engine(commit_every(16))
        .run(
            plan(),
            changes(9, &spec).await,
            memory("history_retried").await,
        )
        .await;
    assert_eq!(
        again.report.status,
        RunStatus::Succeeded,
        "{:?}",
        again.error
    );
    let published = versions(&rdlt_connector_reference::published(
        "history_retried",
        "orders",
    ));
    assert_eq!(published, expected);
}

#[tokio::test(start_paused = true)]
async fn a_full_read_kept_as_history_versions_only_the_rows_that_changed_between_runs() {
    let keyed = |rows| BatchStream::new("events", rows).primary_key(&["id"]);
    let runs = [
        vec![batch(vec![("id", ints(&[1, 2])), ("v", text(&["a", "b"]))])],
        vec![batch(vec![
            ("id", ints(&[1, 2, 3])),
            ("v", text(&["a", "c", "d"])),
        ])],
    ];
    for rows in runs {
        let outcome = engine(commit_every(10))
            .run(
                pipeline("full-history", [stream("events").write(WriteMode::History)]),
                batches("full_history", vec![keyed(rows)]).await,
                memory("full_history").await,
            )
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
    }
    let rows = timeline("full_history", "events");
    let shape: Vec<(i64, &str, bool, bool)> = rows
        .iter()
        .map(|(id, value, current, _, to)| (*id, value.as_str(), *current, to.is_some()))
        .collect();
    // Key 1 is unchanged, key 2's first version closes when the second run's begins.
    assert_eq!(
        shape,
        [
            (1, "a", true, false),
            (2, "b", false, true),
            (2, "c", true, false),
            (3, "d", true, false)
        ]
    );
    assert_eq!(rows[1].4, rows[2].3, "a version ends where the next begins");
    assert!(rows[1].3 < rows[2].3);
}

#[tokio::test(start_paused = true)]
async fn history_streams_the_run_cannot_keep_are_refused() {
    let rows = || vec![batch(vec![("id", ints(&[1])), ("v", text(&["a"]))])];
    let keyed = || BatchStream::new("events", rows()).primary_key(&["id"]);
    let appending = limited(memory("no_history").await, |capabilities| {
        capabilities.write_modes = WriteModes {
            append: true,
            merge: true,
            ..WriteModes::default()
        };
    });
    let unsupported = kept("no_history", keyed(), appending).await;
    let declared = rdlt_connector::TableSchema::new(vec![
        rdlt_connector::Field::new("id", rdlt_connector::LogicalType::Int64, false),
        rdlt_connector::Field::new("v", rdlt_connector::LogicalType::Utf8, true),
    ])
    .expect("a valid schema");
    let text_time = keyed().declared(declared).change_time("v");
    let text_time = kept("text_time", text_time, memory("text_time").await).await;
    let normalized = kept("normalized_history", keyed(), memory("normalized").await).await;
    for (outcome, code) in [
        (unsupported, "write_mode_unsupported"),
        (text_time, "change_time_invalid"),
        (normalized, "history_normalize_unsupported"),
    ] {
        let error = outcome.error.expect("the run fails");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some(code))
        );
    }
    // A change time the catalog declares no type for is refused once its batches show text, or
    // lack it.
    let untimed = kept("untimed", keyed().change_time("v"), memory("untimed").await).await;
    let missing = kept(
        "missing",
        keyed().change_time("at"),
        memory("missing").await,
    )
    .await;
    for (outcome, code) in [
        (untimed, "change_time_invalid"),
        (missing, "change_time_missing"),
    ] {
        let error = outcome.error.expect("the run fails");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Schema, Some(code))
        );
    }
}

fn normalize() -> rdlt_engine::SchemaSettings {
    rdlt_engine::SchemaSettings::new().nested(rdlt_engine::Nested::normalize())
}

#[tokio::test(start_paused = true)]
async fn a_table_keeps_history_for_history_streams_only() {
    let rows = || vec![batch(vec![("id", ints(&[1])), ("v", text(&["a"]))])];
    let keyed = || BatchStream::new("events", rows()).primary_key(&["id"]);
    let run = |write: WriteMode, store: &'static str| async move {
        engine(commit_every(10))
            .run(
                pipeline(store, [stream("events").write(write)]),
                batches(store, vec![keyed()]).await,
                memory(store).await,
            )
            .await
    };
    for (first, then, store) in [
        (WriteMode::Merge, WriteMode::History, "merged_then_history"),
        (WriteMode::History, WriteMode::Merge, "history_then_merged"),
        (
            WriteMode::History,
            WriteMode::Append,
            "history_then_appended",
        ),
    ] {
        let outcome = run(first, store).await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{store}");
        let refused = run(then, store).await.error.expect("the second run fails");
        assert_eq!(
            (refused.kind(), refused.code()),
            (ErrorKind::Config, Some("table_history_mismatch")),
            "{store}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn an_update_leaving_columns_unchanged_is_refused_in_a_history_stream() {
    let spec = ChangedStream {
        partial: true,
        ..timed()
    };
    let outcome = engine(commit_every(16))
        .run(
            pipeline("history-partial", [history_of("orders", DeleteMode::Hard)]),
            changes(8, &spec).await,
            memory("history_partial").await,
        )
        .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(error.code(), Some("partial_updates_unsupported"));
}

#[tokio::test(start_paused = true)]
async fn versions_begin_when_their_batch_arrived_where_the_stream_names_no_change_time() {
    let spec = ChangedStream {
        changed_at: false,
        ..timed()
    };
    // A clock that moves on every reading: a run lasting as long as a following one's does.
    let outcome = crate::support::ticking_engine(commit_every(1_000))
        .run(
            pipeline("history-untimed", [history_of("orders", DeleteMode::Hard)]),
            changes(8, &spec).await,
            memory("history_untimed").await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let published = rdlt_connector_reference::published("history_untimed", "orders");
    let loaded = published
        .iter()
        .flat_map(|batch| micros(batch, "_rdlt_loaded_at"))
        .min()
        .flatten()
        .expect("rows were loaded");
    let spans: Vec<(i64, Option<i64>)> = published
        .iter()
        .flat_map(|batch| {
            let from = micros(batch, "_rdlt_valid_from");
            let to = micros(batch, "_rdlt_valid_to");
            from.into_iter().zip(to)
        })
        .map(|(from, to)| (from.expect("every version begins"), to))
        .collect();
    // Versions begin as their batches arrive, after the load started, so a version one batch
    // opens and a later one closes spans the time between.
    assert!(
        spans
            .iter()
            .all(|(from, to)| *from >= loaded && to.is_none_or(|to| to >= *from))
    );
    assert!(
        spans
            .iter()
            .any(|(from, to)| to.is_some_and(|to| to > *from)),
        "{spans:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_change_whose_change_time_alone_moved_opens_no_version() {
    let at = |micros: i64| -> ArrayRef {
        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"))
    };
    let runs = [
        batch(vec![
            ("id", ints(&[1])),
            ("v", text(&["a"])),
            ("at", at(10)),
        ]),
        batch(vec![
            ("id", ints(&[1])),
            ("v", text(&["a"])),
            ("at", at(20)),
        ]),
        batch(vec![
            ("id", ints(&[1])),
            ("v", text(&["b"])),
            ("at", at(30)),
        ]),
    ];
    for rows in runs {
        let events = BatchStream::new("events", vec![rows])
            .primary_key(&["id"])
            .change_time("at");
        let outcome = engine(commit_every(10))
            .run(
                pipeline(
                    "timed-history",
                    [stream("events").write(WriteMode::History)],
                ),
                batches("timed_history", vec![events]).await,
                memory("timed_history").await,
            )
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
    }
    let published = rdlt_connector_reference::published("timed_history", "events");
    let mut spans: Vec<(Option<i64>, Option<i64>)> = published
        .iter()
        .flat_map(|batch| {
            let from = micros(batch, "_rdlt_valid_from");
            let to = micros(batch, "_rdlt_valid_to");
            from.into_iter().zip(to)
        })
        .collect();
    spans.sort();
    assert_eq!(spans, [(Some(10), Some(30)), (Some(30), None)]);
}

#[tokio::test(start_paused = true)]
async fn a_change_time_other_than_the_one_a_history_table_began_with_is_refused() {
    let at = |micros: i64| -> ArrayRef {
        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"))
    };
    let rows = || {
        batch(vec![
            ("id", ints(&[1])),
            ("v", text(&["a"])),
            ("at", at(1_000)),
            ("created", at(10)),
        ])
    };
    let run = |events: BatchStream| async move {
        engine(commit_every(10))
            .run(
                pipeline("rebased", [stream("events").write(WriteMode::History)]),
                batches("rebased", vec![events]).await,
                memory("rebased").await,
            )
            .await
    };
    let keyed = || BatchStream::new("events", vec![rows()]).primary_key(&["id"]);
    let first = run(keyed().change_time("at")).await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    // Another column, or none, would open a version for an unchanged row and begin it at
    // another basis: the table's versions began at `at`.
    for events in [keyed().change_time("created"), keyed()] {
        let outcome = run(events).await;
        assert_eq!(outcome.report.status, RunStatus::Failed);
        let error = outcome.error.expect("the run failed");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some("table_change_time_mismatch"))
        );
    }
    let again = run(keyed().change_time("at")).await;
    assert_eq!(
        again.report.status,
        RunStatus::Succeeded,
        "{:?}",
        again.error
    );
    let published = rdlt_connector_reference::published("rebased", "events");
    assert_eq!(
        published.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn a_history_read_again_after_a_reset_holds_the_versions_its_changes_make() {
    let spec = timed();
    for scope in [ResetScope::Positions, ResetScope::Tables] {
        let store = format!("history_reset_{scope:?}").to_lowercase();
        let name = store.replace('_', "-");
        let run = || async {
            engine(commit_every(16))
                .run(
                    pipeline(&name, [history_of("orders", DeleteMode::Hard)]),
                    changes(11, &spec).await,
                    memory(&store).await,
                )
                .await
        };
        let first = run().await;
        assert_eq!(
            first.report.status,
            RunStatus::Succeeded,
            "{:?}",
            first.error
        );
        engine(commit_every(16))
            .reset(
                &name,
                &["orders"],
                scope,
                changes(11, &spec).await,
                memory(&store).await,
            )
            .await
            .expect("the reset commits");
        // Read again from its snapshot, the stream changes no version its first read made, or
        // makes them all again in the table a reset dropped.
        let again = run().await;
        assert_eq!(
            again.report.status,
            RunStatus::Succeeded,
            "{:?}",
            again.error
        );
        let published = versions(&rdlt_connector_reference::published(&store, "orders"));
        assert_eq!(published, history(11, &spec, false), "{scope:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn json_the_source_renders_again_differently_opens_no_version() {
    // The same object, its keys in another order and spaced otherwise, as JSON text.
    let texts = [
        r#"{"a": 1, "b": [2, 3]}"#,
        r#"{"b":[2,3],"a":1}"#,
        r#"{"a": 1, "b": [3, 2]}"#,
    ];
    for text in texts {
        let json = arrow_schema::Field::new("j", DataType::Utf8, true).with_metadata(
            [("ARROW:extension:name".to_owned(), "arrow.json".to_owned())]
                .into_iter()
                .collect(),
        );
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            json,
        ]);
        let columns: Vec<ArrayRef> = vec![ints(&[1]), text_array(text)];
        let rows = RecordBatch::try_new(Arc::new(schema), columns).expect("a valid batch");
        let events = BatchStream::new("events", vec![rows]).primary_key(&["id"]);
        let outcome = engine(commit_every(10))
            .run(
                pipeline("json-history", [stream("events").write(WriteMode::History)]),
                batches("json_history", vec![events]).await,
                memory("json_history").await,
            )
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
    }
    let current: Vec<bool> = rdlt_connector_reference::published("json_history", "events")
        .iter()
        .flat_map(|batch| {
            let current = batch
                .column_by_name("_rdlt_is_current")
                .expect("a history column");
            current.as_boolean().iter().flatten().collect::<Vec<_>>()
        })
        .collect();
    // Only the third changed the object: two versions, the second current.
    assert_eq!(current.len(), 2, "{current:?}");
    assert_eq!(current.iter().filter(|current| **current).count(), 1);
}

fn text_array(text: &str) -> ArrayRef {
    Arc::new(arrow_array::StringArray::from(vec![text]))
}

//! What tables may grow to across pushes and runs: their columns, nested fields counted, and the
//! child tables a normalized stream adds.

use rdlt_engine::{
    EngineConfig, ErrorKind, GrowthLimits, Nested, RunOutcome, RunStatus, SchemaSettings,
};
use serde_json::{Map, Value, json};

use crate::support::batches::{BatchStream, batches};
use crate::support::{commit_every, engine, memory, pipeline, stream};

/// The least memory an engine reading sixteen partitions may have, whose derived column limit is
/// small enough to reach.
fn least() -> rdlt_engine::EngineConfigBuilder {
    commit_every(1000).memory(EngineConfig::least_memory(16))
}

/// The most columns a table may hold under [`least`], nested fields counted.
fn column_limit() -> usize {
    let limit = least()
        .build()
        .expect("a valid config")
        .limits()
        .schema_columns;
    usize::try_from(limit).expect("a small limit")
}

/// A record of `count` integer fields named `prefix` and their number.
fn record(prefix: &str, count: usize) -> Value {
    let fields: Map<String, Value> = (0..count)
        .map(|field| (format!("{prefix}{field}"), json!(1)))
        .collect();
    Value::Object(fields)
}

/// Loads `documents` into `store` as stream `events`, under `config` and `nested`.
async fn load(
    store: &str,
    documents: &[Value],
    config: rdlt_engine::EngineConfigBuilder,
    nested: Nested,
) -> RunOutcome {
    let pushes: Vec<String> = documents.iter().map(Value::to_string).collect();
    let pushes: Vec<&str> = pushes.iter().map(String::as_str).collect();
    let source = batches(store, vec![BatchStream::json("events", &pushes)]).await;
    let settings = SchemaSettings::default().nested(nested);
    let plan = pipeline(store, [stream("events").schema(settings)]);
    engine(config).run(plan, source, memory(store).await).await
}

/// Checks that `outcome` failed, refused for its schema with `code`.
fn refused(outcome: &RunOutcome, code: &str) {
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.as_ref().expect("the run fails");
    assert_eq!(error.kind(), ErrorKind::Schema, "{error:?}");
    assert_eq!(error.code(), Some(code), "{error:?}");
    assert!(!error.is_retryable());
}

#[tokio::test(start_paused = true)]
async fn a_table_at_its_column_limit_loads_and_one_column_more_is_refused() {
    let store = "growth_columns";
    let full = record("k", column_limit());
    let outcome = load(store, &[full], least(), Nested::Native).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let outcome = load(store, &[json!({ "extra": 1 })], least(), Nested::Native).await;
    refused(&outcome, "table_columns_exceeded");
}

#[tokio::test(start_paused = true)]
async fn a_struct_column_whose_fields_pass_the_column_limit_is_refused() {
    let store = "growth_fields";
    let half = column_limit() / 2;
    let first = json!({ "id": 1, "attrs": record("a", half) });
    let outcome = load(store, &[first], least(), Nested::Native).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let second = json!({ "id": 2, "attrs": record("b", half) });
    let outcome = load(store, &[second], least(), Nested::Native).await;
    refused(&outcome, "table_columns_exceeded");
}

#[tokio::test(start_paused = true)]
async fn a_stream_adding_child_tables_past_the_default_limit_is_refused() {
    let store = "growth_children";
    let arrays: Map<String, Value> = (0..=1024)
        .map(|array| (format!("a{array}"), json!([1])))
        .collect();
    let mut document = arrays;
    document.insert("id".to_owned(), json!(1));
    let outcome = load(
        store,
        &[Value::Object(document)],
        commit_every(1000),
        Nested::normalize(),
    )
    .await;
    refused(&outcome, "child_tables_exceeded");
}

#[tokio::test(start_paused = true)]
async fn child_tables_recorded_by_earlier_runs_count_toward_the_limit() {
    let store = "growth_recorded_children";
    let limited = || commit_every(1000).growth(GrowthLimits::new(3, 128).expect("a valid limit"));
    let runs = [
        json!({ "id": 1, "a0": [1], "a1": [2] }),
        json!({ "id": 2, "b0": [3] }),
        json!({ "id": 3, "a0": [4], "b0": [5] }),
    ];
    for document in runs {
        let outcome = load(store, &[document], limited(), Nested::normalize()).await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
    }
    let outcome = load(
        store,
        &[json!({ "id": 4, "b1": [6] })],
        limited(),
        Nested::normalize(),
    )
    .await;
    refused(&outcome, "child_tables_exceeded");
}

#[tokio::test(start_paused = true)]
async fn wide_child_tables_passing_the_state_limit_are_refused_as_child_tables() {
    let store = "growth_wide_children";
    let config =
        least().commit(rdlt_engine::CommitPolicy::new(None, Some(1), None).expect("valid"));
    let limits = config.clone().build().expect("a valid config");
    // Fewer child tables than the limit counts, each of a hundred columns.
    let documents: Vec<Value> = (0..40)
        .map(|document| {
            let mut fields: Map<String, Value> = (0..10)
                .map(|array| (format!("a{document}_{array}"), json!([record("f", 100)])))
                .collect();
            fields.insert("id".to_owned(), json!(document));
            Value::Object(fields)
        })
        .collect();
    assert!(documents.len() * 10 < limits.child_table_limit());
    let outcome = load(store, &documents, config, Nested::normalize()).await;
    refused(&outcome, "child_tables_exceeded");
}

#[tokio::test]
async fn a_table_changed_by_every_push_loads_through_a_served_destination() {
    use crate::support::targets::Target;
    // A served connection carries at most 200 calls, and each open writer is one: a writer per
    // schema version kept open would wait for a call it never gets.
    let target = Target::SpawnedJsonl;
    let store = target.name("growth_versions");
    let pushes: Vec<String> = (0..250)
        .map(|push| json!({ "id": push, format!("k{push}"): 1 }).to_string())
        .collect();
    let pushes: Vec<&str> = pushes.iter().map(String::as_str).collect();
    let source = batches(&store, vec![BatchStream::json("events", &pushes)]).await;
    let outcome = engine(commit_every(1_000_000))
        .run(
            pipeline(&store, [stream("events")]),
            source,
            target.destination(&store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(target.ids(&store, "events"), (0..250).collect::<Vec<i64>>());
}

#[tokio::test]
async fn a_logged_commit_of_a_table_changed_by_every_push_replays_through_a_served_destination() {
    use std::sync::Arc;
    use std::time::Duration;

    use rdlt_engine::{LocalWal, RetryPolicy, WalStore};

    use crate::support::destinations::{Step, failing};
    use crate::support::logging_engine;
    use crate::support::targets::Target;
    // The first commit fails, so the next attempt stages each of the commit's schema versions
    // again from the log: a writer per version kept open would wait for a call it never gets,
    // until its deadline.
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let target = Target::SpawnedJsonl;
    let store = target.name("growth_replayed");
    let pushes: Vec<String> = (0..250)
        .map(|push| json!({ "id": push, format!("k{push}"): 1 }).to_string())
        .collect();
    let pushes: Vec<&str> = pushes.iter().map(String::as_str).collect();
    let source = batches(&store, vec![BatchStream::json("events", &pushes)]).await;
    let mut options = rdlt_host::Options::default();
    options.deadlines.apply_schema = Duration::from_secs(15);
    options.deadlines.write_ack = Duration::from_secs(15);
    let served = target
        .placed_by(&store, crate::support::local().options(options))
        .await;
    let retry = RetryPolicy::default()
        .max_attempts(2)
        .initial(Duration::from_millis(10));
    let outcome = logging_engine(commit_every(1_000_000).retry(retry), wal)
        .run(
            pipeline(&store, [stream("events")]).with_wal(true),
            source,
            failing(served, Step::CommitOnce),
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.attempts.len(), 2);
    assert_eq!(target.ids(&store, "events"), (0..250).collect::<Vec<i64>>());
}

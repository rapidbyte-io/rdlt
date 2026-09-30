//! Resetting streams between runs: from their beginning into the tables they have, or into new
//! tables, handed to any pipeline.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use rdlt_connector::{ConnectContext, ReadMode, Source, source_factory};
use rdlt_connector_reference::LogSource;
use rdlt_connector_reference::changes::expected;
use rdlt_engine::{
    ErrorKind, LocalWal, Nested, ResetReport, ResetScope, RunStatus, SchemaSettings, WalStore,
    WriteMode,
};
use serde_json::json;

use crate::changes::{changes, orders, rows};
use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::{Step, failing, limited};
use crate::support::script::{Script, ScriptStream, id};
use crate::support::targets::Target;
use crate::support::{
    commit_every, each, engine, generator, logging_engine, memory, pipeline, published_ids,
    published_json, retrying, stream,
};

/// A log of four messages in one partition, read by `group`.
async fn log(group: &str) -> Arc<dyn Source> {
    let config = json!({
        "seed": 3, "group": group,
        "streams": [{ "name": "events", "partitions": 1, "messages": 4 }],
    });
    let source = source_factory::<LogSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the log connects");
    Arc::from(source)
}

/// Loads the log's events into `store` at `target` as pipeline `name`, which must succeed.
async fn load(target: Target, name: &str, store: &str) {
    let plan = pipeline(name, [stream("events").read(ReadMode::Incremental)]);
    let outcome = engine(commit_every(16))
        .run(plan, log(name).await, target.destination(store).await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{target:?}: {:?}",
        outcome.error
    );
}

/// The offsets published to the events of `store` at `target`, sorted.
fn offsets(target: Target, store: &str) -> Vec<u64> {
    let mut offsets: Vec<u64> = target
        .json(store, "events")
        .iter()
        .map(|row| row["offset"].as_u64().expect("an offset"))
        .collect();
    offsets.sort_unstable();
    offsets
}

/// Resets the events of pipeline `name` in `store` at `target` as `scope` says.
async fn reset(target: Target, name: &str, store: &str, scope: ResetScope) -> ResetReport {
    engine(commit_every(16))
        .reset(
            name,
            &["events"],
            scope,
            log(name).await,
            target.destination(store).await,
        )
        .await
        .unwrap_or_else(|error| panic!("{target:?}: the reset commits: {error}"))
}

#[tokio::test]
async fn a_stream_reset_to_its_beginning_is_read_again_into_its_table() {
    each(Target::IN_PROCESS, |target| async move {
        load(target, "again", "reset_positions").await;
        assert_eq!(
            offsets(target, "reset_positions"),
            [0, 1, 2, 3],
            "{target:?}"
        );
        let report = reset(target, "again", "reset_positions", ResetScope::Positions).await;
        assert!(report.dropped.is_empty(), "{target:?}");
        // Appended, every message lands twice: once from each read.
        load(target, "again", "reset_positions").await;
        let twice = [0, 0, 1, 1, 2, 2, 3, 3];
        assert_eq!(offsets(target, "reset_positions"), twice, "{target:?}");
    })
    .await;
}

#[tokio::test]
async fn a_stream_reset_with_its_tables_is_loaded_into_a_new_table() {
    let targets = Target::IN_PROCESS
        .into_iter()
        .chain(Target::SPAWNED)
        .chain(Target::REMOTE);
    each(targets, |target| async move {
        load(target, "anew", "reset_tables").await;
        let report = reset(target, "anew", "reset_tables", ResetScope::Tables).await;
        assert_eq!(report.dropped.len(), 1, "{target:?}");
        assert!(
            target.json("reset_tables", "events").is_empty(),
            "{target:?}"
        );
        load(target, "anew", "reset_tables").await;
        assert_eq!(offsets(target, "reset_tables"), [0, 1, 2, 3], "{target:?}");
    })
    .await;
}

#[tokio::test]
async fn a_reset_retried_changes_nothing_more() {
    let target = Target::Memory;
    load(target, "retried", "reset_retried").await;
    for _ in 0..2 {
        reset(target, "retried", "reset_retried", ResetScope::Tables).await;
    }
    load(target, "retried", "reset_retried").await;
    assert_eq!(offsets(target, "reset_retried"), [0, 1, 2, 3]);
}

#[tokio::test]
async fn a_reset_of_a_stream_the_pipeline_recorded_nothing_of_is_refused_and_changes_nothing() {
    let target = Target::Memory;
    load(target, "unknown", "reset_unknown").await;
    let refused = engine(commit_every(16))
        .reset(
            "unknown",
            &["events", "missing"],
            ResetScope::Positions,
            log("unknown").await,
            target.destination("reset_unknown").await,
        )
        .await
        .expect_err("a stream the pipeline never loaded is refused");
    assert_eq!(refused.kind(), ErrorKind::Config);
    assert_eq!(refused.code(), Some("stream_not_found"));
    // Nothing was reset: the next run finds every message already loaded.
    load(target, "unknown", "reset_unknown").await;
    assert_eq!(offsets(target, "reset_unknown"), [0, 1, 2, 3]);
}

#[tokio::test]
async fn a_destination_that_drops_no_tables_refuses_to_reset_a_stream_s_tables() {
    let target = Target::Memory;
    load(target, "undroppable", "reset_undroppable").await;
    let destination = limited(memory("reset_undroppable").await, |capabilities| {
        capabilities.drop_tables = false;
    });
    let refused = engine(commit_every(16))
        .reset(
            "undroppable",
            &["events"],
            ResetScope::Tables,
            log("undroppable").await,
            destination,
        )
        .await
        .expect_err("the destination drops no tables");
    assert_eq!(refused.kind(), ErrorKind::Config);
    assert_eq!(refused.code(), Some("drop_unsupported"));
    assert_eq!(offsets(target, "reset_undroppable"), [0, 1, 2, 3]);
}

#[tokio::test]
async fn a_table_reset_with_its_stream_is_handed_to_any_pipeline() {
    each(Target::IN_PROCESS, |target| async move {
        let store = "reset_handed";
        load(target, "first", store).await;
        let plan = pipeline("second", [stream("events").read(ReadMode::Incremental)]);
        let refused = engine(commit_every(16))
            .run(
                plan.clone(),
                log("second").await,
                target.destination(store).await,
            )
            .await;
        assert_eq!(refused.report.status, RunStatus::Failed, "{target:?}");
        let error = refused.error.expect("the table is the first pipeline's");
        assert_eq!(error.code(), Some("table_owned"), "{target:?}");
        reset(target, "first", store, ResetScope::Tables).await;
        let outcome = engine(commit_every(16))
            .run(plan, log("second").await, target.destination(store).await)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{target:?}: {:?}",
            outcome.error
        );
        assert_eq!(offsets(target, store), [0, 1, 2, 3], "{target:?}");
    })
    .await;
}

#[tokio::test]
async fn a_merge_table_reset_with_its_stream_takes_a_change_stream() {
    let store = "reset_to_changes";
    let merged = pipeline("switched", [stream("orders").write(WriteMode::Merge)]);
    let outcome = engine(commit_every(16))
        .run(
            merged,
            generator(&[("orders", 40, 1, 8)]).await,
            memory(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    engine(commit_every(16))
        .reset(
            "switched",
            &["orders"],
            ResetScope::Tables,
            generator(&[("orders", 40, 1, 8)]).await,
            memory(store).await,
        )
        .await
        .expect("the reset commits");
    let cdc = pipeline(
        "switched",
        [stream("orders").read(ReadMode::Cdc).write(WriteMode::Merge)],
    );
    let outcome = engine(commit_every(16))
        .run(cdc, changes(13, &orders(&[])).await, memory(store).await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(rows(store, "orders"), expected(13, &orders(&[])));
}

#[tokio::test(start_paused = true)]
async fn rows_a_load_logged_before_its_stream_was_reset_never_land_after_it() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let wal: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    // p1 holds nothing at first and records no position, so its first seals start from nowhere.
    let mut events = ScriptStream::new("events", 2, 20, 5);
    events.replayable = false;
    events.final_checkpoint = false;
    events.rows[1].store(0, Ordering::SeqCst);
    let (script, source) = Script::new(vec![events]).connect("reset_logged").await;
    let plan = pipeline(
        "reset-logged",
        [stream("events").read(ReadMode::Incremental)],
    )
    .with_wal(true);
    let run = |destination| {
        logging_engine(retrying(1), Arc::clone(&wal)).run(
            plan.clone(),
            Arc::clone(&source),
            destination,
        )
    };
    let loaded = run(memory("reset_logged").await).await;
    assert_eq!(
        loaded.report.status,
        RunStatus::Succeeded,
        "{:?}",
        loaded.error
    );
    // The next load logs p1's rows, which the source then forgets, but never commits them.
    script.streams[0].rows[1].store(10, Ordering::SeqCst);
    let logged = run(failing(memory("reset_logged").await, Step::Commit)).await;
    assert_eq!(logged.report.status, RunStatus::Failed);
    let acknowledged = script
        .acks
        .lock()
        .iter()
        .filter(|(_, partition, _)| partition == "p1")
        .map(|(_, _, next)| *next)
        .max()
        .expect("the source acknowledged p1's logged rows");
    // The stream is reset through a source that no longer serves it, which cannot say whether
    // it reads again: the reset's epoch alone keeps what the failed load logged from landing.
    engine(commit_every(16))
        .reset(
            "reset-logged",
            &["events"],
            ResetScope::Positions,
            generator(&[("other", 1, 1, 1)]).await,
            memory("reset_logged").await,
        )
        .await
        .expect("the reset commits");
    let after = run(memory("reset_logged").await).await;
    // Read from their beginning, both partitions wait for rows their source forgot.
    assert_eq!(after.report.status, RunStatus::Failed);
    let expected: Vec<i64> = (0..20).map(|offset| id(0, offset)).collect();
    assert_eq!(published_ids("reset_logged", "events"), expected);
    assert!(acknowledged > 0);
}

#[tokio::test]
async fn a_stream_whose_source_cannot_read_again_is_not_reset() {
    let mut events = ScriptStream::new("events", 1, 10, 5);
    events.replayable = false;
    let (_, source) = Script::new(vec![events]).connect("reset_forgetful").await;
    let refused = engine(commit_every(16))
        .reset(
            "reset-forgetful",
            &["events"],
            ResetScope::Tables,
            source,
            memory("reset_forgetful").await,
        )
        .await
        .expect_err("a source that cannot read again cannot read from the beginning");
    assert_eq!(refused.kind(), ErrorKind::Config);
    assert_eq!(refused.code(), Some("reset_unreplayable"));
}

#[tokio::test]
async fn a_destination_that_panics_fails_the_reset_rather_than_its_caller() {
    let destination = failing(memory("reset_panicking").await, Step::PanicOnOpen);
    let failed = engine(commit_every(16))
        .reset(
            "panicking",
            &["events"],
            ResetScope::Positions,
            log("panicking").await,
            destination,
        )
        .await
        .expect_err("a panicking destination fails the reset");
    assert_eq!(failed.kind(), ErrorKind::Internal);
}

#[tokio::test(start_paused = true)]
async fn a_normalized_stream_reset_with_its_tables_drops_its_child_tables_too() {
    let store = "reset_children";
    let push = r#"{"id":1,"items":[{"sku":"x"},{"sku":"y"}]}"#;
    let source = batches(store, vec![BatchStream::json("events", &[push])]).await;
    let normalized = || {
        let settings = SchemaSettings::new().nested(Nested::normalize());
        pipeline("children", [stream("events").schema(settings)])
    };
    let load = || async {
        let outcome = engine(commit_every(16))
            .run(normalized(), Arc::clone(&source), memory(store).await)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
    };
    load().await;
    let skus = [json!({"sku": "x"}), json!({"sku": "y"})];
    assert_eq!(published_json(store, "events__items"), skus);
    let report = engine(commit_every(16))
        .reset(
            "children",
            &["events"],
            ResetScope::Tables,
            Arc::clone(&source),
            memory(store).await,
        )
        .await
        .expect("the reset commits");
    assert_eq!(report.dropped.len(), 2, "{report:?}");
    assert!(published_json(store, "events").is_empty());
    assert!(published_json(store, "events__items").is_empty());
    // Loaded again, the child table is created anew beside its root.
    load().await;
    assert_eq!(published_json(store, "events__items"), skus);
    assert_eq!(published_json(store, "events"), [json!({"id": 1})]);
}

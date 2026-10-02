//! Write-ahead logs: what a load wrote and was about to commit, kept durable so a source that
//! cannot read again what it acknowledged loses nothing when a load fails.

use std::sync::Arc;

use rdlt_connector::{PipelineId, ReadMode};
use rdlt_engine::{ErrorKind, LocalWal, RunOutcome, RunStatus, WalStore};

use crate::support::destinations::{Step, counting, failing};
use crate::support::logs::Counted;
use crate::support::script::{Script, ScriptStream, id};
use crate::support::{
    commit_every, engine, logging_engine, memory, pipeline, published_ids, retrying, stream,
};

/// The ids `partitions` partitions of `rows` rows each hold, in order.
fn ids(partitions: usize, rows: u64) -> Vec<i64> {
    let mut ids: Vec<i64> = (0..partitions)
        .flat_map(|partition| (0..rows).map(move |offset| id(partition, offset)))
        .collect();
    ids.sort_unstable();
    ids
}

/// Checks that `outcome` failed on its configuration, as `code`, before loading anything.
fn refused(outcome: RunOutcome, code: &str) {
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the run fails");
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.code(), Some(code));
    assert_eq!(outcome.report.rows, 0);
    assert_eq!(
        outcome.report.attempts.len(),
        1,
        "a configuration error is final"
    );
}

/// A stream of 10 rows whose source cannot read again what it acknowledged, as `name`.
async fn forgetful(name: &str) -> Arc<dyn rdlt_connector::Source> {
    let mut once = ScriptStream::new("events", 1, 10, 5);
    once.replayable = false;
    Script::new(vec![once]).connect(name).await.1
}

#[tokio::test(start_paused = true)]
async fn a_load_that_must_log_ahead_fails_where_the_engine_keeps_no_logs() {
    let (_, replayable) = Script::new(vec![ScriptStream::new("events", 1, 10, 5)])
        .connect("wal_asked")
        .await;
    let incremental = || [stream("events").read(ReadMode::Incremental)];
    let engine = engine(commit_every(5));
    let required = engine
        .run(
            pipeline("wal-required", incremental()),
            forgetful("wal_required").await,
            memory("wal_required").await,
        )
        .await;
    refused(required, "wal_required");
    let changes = engine
        .run(
            pipeline(
                "wal-required-changes",
                [stream("events").read(ReadMode::Cdc)],
            ),
            forgetful("wal_required_changes").await,
            memory("wal_required_changes").await,
        )
        .await;
    refused(changes, "wal_required");
    let asked = engine
        .run(
            pipeline("wal-asked", incremental()).with_wal(true),
            replayable,
            memory("wal_asked").await,
        )
        .await;
    refused(asked, "wal_store_missing");
}

#[tokio::test(start_paused = true)]
async fn a_source_that_cannot_read_again_is_never_read_in_full() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let engine = logging_engine(commit_every(5), store);
    // A full read starts again from the beginning, which such a source no longer holds.
    let full = engine
        .run(
            pipeline("wal-full", [stream("events")]),
            forgetful("wal_full").await,
            memory("wal_full").await,
        )
        .await;
    refused(full, "full_read_unreplayable");
}

#[tokio::test(start_paused = true)]
async fn a_logged_load_publishes_every_row_once_and_leaves_no_log() {
    let base = tempfile::tempdir().expect("a temporary directory");
    for replayable in [true, false] {
        let counted = Arc::new(Counted::new(LocalWal::new(base.path())));
        let store: Arc<dyn WalStore> = Arc::clone(&counted) as Arc<dyn WalStore>;
        let name = format!("wal_logged_{replayable}");
        let mut events = ScriptStream::new("events", 2, 30, 7);
        events.replayable = replayable;
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        let plan = pipeline(
            &name.replace('_', "-"),
            [stream("events").read(ReadMode::Incremental)],
        )
        .with_wal(true);
        let outcome = logging_engine(commit_every(10), Arc::clone(&store))
            .run(plan, source, memory(&name).await)
            .await;
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        assert_eq!(outcome.report.status, RunStatus::Succeeded);
        assert_eq!(published_ids(&name, "events"), ids(2, 30));
        let commits = usize::try_from(outcome.report.attempts[0].commits).expect("few commits");
        assert!(commits > 1, "{commits} commits");
        // Each commit's frame and the closing one are made durable, after every batch's frame.
        assert_eq!(counted.syncs(), commits + 1);
        assert!(counted.appends() > 2 * commits);
        let acked = script.acks.lock().clone();
        for partition in ["p0", "p1"] {
            let last = acked
                .iter()
                .filter(|(_, acked, _)| acked == partition)
                .map(|(_, _, next)| *next)
                .max();
            assert_eq!(last, Some(30), "{partition} acknowledged to its end");
        }
        let pipeline = PipelineId::parse(name.replace('_', "-")).expect("a valid pipeline");
        assert_eq!(store.loads(&pipeline).await.expect("loads list"), []);
    }
}

#[tokio::test(start_paused = true)]
async fn a_commit_the_destination_missed_lands_from_the_log_before_the_source_is_read_again() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let mut events = ScriptStream::new("events", 2, 30, 7);
    events.replayable = false;
    let (script, source) = Script::new(vec![events]).connect("wal_replayed").await;
    let destination = failing(memory("wal_replayed").await, Step::CommitOnce);
    let plan = pipeline(
        "wal-replayed",
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = logging_engine(retrying(3), Arc::clone(&store))
        .run(plan, source, destination)
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.attempts.len(), 2);
    // The first commit's rows were acknowledged and are gone from the source: only the log had
    // them, and they land once.
    assert_eq!(published_ids("wal_replayed", "events"), ids(2, 30));
    assert_eq!(
        script.early_reads.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    let pipeline = PipelineId::parse("wal-replayed").expect("a valid pipeline");
    assert_eq!(store.loads(&pipeline).await.expect("loads list"), []);
}

#[tokio::test(start_paused = true)]
async fn a_replay_whose_commit_fails_closes_the_session_it_opened() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let source = forgetful("wal_closed").await;
    let plan = pipeline("wal-closed", [stream("events").read(ReadMode::Incremental)]);
    let run = |destination| {
        logging_engine(retrying(1), Arc::clone(&store)).run(
            plan.clone(),
            Arc::clone(&source),
            destination,
        )
    };
    let logged = run(failing(memory("wal_closed").await, Step::Commit)).await;
    assert_eq!(logged.report.status, RunStatus::Failed);
    let (destination, sessions) = counting(failing(memory("wal_closed").await, Step::Commit));
    let replayed = run(destination).await;
    assert_eq!(replayed.report.status, RunStatus::Failed);
    let error = replayed.error.expect("the replay fails");
    assert_eq!(error.kind(), ErrorKind::Destination);
    let opened = sessions.opened.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(opened, 1, "the replay's session, and no attempt's");
    let closed = sessions.closed.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(closed, opened);
}

#[tokio::test(start_paused = true)]
async fn a_load_that_needs_no_log_keeps_none_though_the_engine_has_a_store() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let counted = Arc::new(Counted::new(LocalWal::new(base.path())));
    let store: Arc<dyn WalStore> = Arc::clone(&counted) as Arc<dyn WalStore>;
    let (_, source) = Script::new(vec![ScriptStream::new("events", 2, 30, 7)])
        .connect("wal_unneeded")
        .await;
    let plan = pipeline(
        "wal-unneeded",
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = logging_engine(commit_every(10), store)
        .run(plan, source, memory("wal_unneeded").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(published_ids("wal_unneeded", "events"), ids(2, 30));
    assert_eq!((counted.appends(), counted.syncs()), (0, 0));
}

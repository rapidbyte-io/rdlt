//! Write-ahead logs: what a load wrote and was about to commit, kept durable so a source that
//! cannot read again what it acknowledged loses nothing when a load fails.

use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::{PipelineId, ReadMode};
use rdlt_engine::{Chunk, ErrorKind, GrowthLimits, LocalWal, RunOutcome, RunStatus, WalStore};

use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::{Step, counting, failing};
use crate::support::logs::Counted;
use crate::support::memory_wal::Memory;
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
        // Each commit's chunk and the closing one are published, after every batch's frame.
        assert_eq!(counted.publishes(), commits + 1);
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
    assert_eq!((counted.appends(), counted.publishes()), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_logged_commit_of_a_table_nested_to_the_limit_replays() {
    for nesting in rdlt_testkit::nested::NESTINGS {
        let name = format!("wal_deepest_{nesting:?}").to_lowercase();
        let base = tempfile::tempdir().expect("a temporary directory");
        let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
        let document = crate::json::deepest(1, nesting);
        let source = batches(&name, vec![BatchStream::json("events", &[&document])]).await;
        let destination = failing(memory(&name).await, Step::CommitOnce);
        let plan = pipeline(&name, [stream("events")]).with_wal(true);
        let outcome = logging_engine(retrying(3), Arc::clone(&store))
            .run(plan, source, destination)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{nesting:?}: {:?}",
            outcome.error
        );
        assert_eq!(published_ids(&name, "events"), [1], "{nesting:?}");
        let pipeline = PipelineId::parse(name.replace('_', "-")).expect("a valid pipeline");
        assert_eq!(store.loads(&pipeline).await.expect("loads list"), []);
    }
}

#[tokio::test(start_paused = true)]
async fn a_source_that_never_checkpoints_is_refused_before_its_log_passes_its_limit() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    // A partition that checkpoints only at its end: every commit's barrier finds it unsealed.
    let mut events = ScriptStream::new("events", 1, 4_000, 20);
    events.replayable = false;
    events.checkpoint_every = u64::MAX;
    let (script, source) = Script::new(vec![events]).connect("wal_unsealed").await;
    let limit = 16 << 10;
    let growth = GrowthLimits::default()
        .with_log_bytes(limit)
        .expect("a valid limit");
    let plan = pipeline(
        "wal-unsealed",
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = logging_engine(commit_every(100).growth(growth), Arc::clone(&store))
        .run(plan, source, memory("wal_unsealed").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the run fails");
    assert_eq!(error.code(), Some("log_bytes_exceeded"), "{error:?}");
    assert!(!error.is_retryable());
    // Nothing of the read was committed, and the source heard of nothing.
    assert!(script.acks.lock().is_empty());
    assert_eq!(published_ids("wal_unsealed", "events"), Vec::<i64>::new());
}

#[tokio::test(start_paused = true)]
async fn a_store_s_largest_chunk_bounds_a_load_s_log_as_its_limit_does() {
    let small = std::num::NonZeroU64::new(16 << 10).expect("not zero");
    let large = std::num::NonZeroU64::new(1 << 30).expect("not zero");
    // The lower of the store's bound and the engine's applies, whichever it is.
    for (chunks, log) in [
        (small, GrowthLimits::default()),
        (large, GrowthLimits::default()),
    ] {
        let log = if chunks == large {
            log.with_log_bytes(small.get()).expect("a valid limit")
        } else {
            log
        };
        let name = format!("wal_chunked_{}", chunks.get());
        // A partition that checkpoints only at its end, as above.
        let store = Arc::new(Memory::chunked(chunks));
        let mut events = ScriptStream::new("events", 1, 4_000, 20);
        events.replayable = false;
        events.checkpoint_every = u64::MAX;
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        let plan = pipeline(
            &name.replace('_', "-"),
            [stream("events").read(ReadMode::Incremental)],
        );
        let engine = logging_engine(commit_every(100).growth(log), Arc::clone(&store) as _);
        let refused = engine.run(plan, source, memory(&name).await).await;
        // Refused by the engine at the bound, not by the store past it.
        let error = refused.error.expect("the run fails");
        assert_eq!(
            error.code(),
            Some("log_bytes_exceeded"),
            "{name}: {error:?}"
        );
        assert!(script.acks.lock().is_empty(), "{name}");
        assert_eq!(published_ids(&name, "events"), Vec::<i64>::new(), "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_full_disk_fails_its_attempts_retryably_and_keeps_none_of_what_they_staged() {
    // A disk with room for less than one commit's chunk.
    let store = Arc::new(Memory::of(2_048));
    let mut events = ScriptStream::new("events", 2, 30, 7);
    events.replayable = false;
    let (_, source) = Script::new(vec![events]).connect("wal_full_disk").await;
    let plan = pipeline(
        "wal-full-disk",
        [stream("events").read(ReadMode::Incremental)],
    );
    let full = logging_engine(retrying(3), Arc::clone(&store) as Arc<dyn WalStore>)
        .run(plan, source, memory("wal_full_disk").await)
        .await;
    assert_eq!(full.report.status, RunStatus::Failed);
    assert_eq!(full.report.attempted, 3);
    let error = full.error.expect("the disk is full");
    assert_eq!(error.code(), Some("wal_storage_full"), "{error:?}");
    assert!(error.is_retryable());
    // Each attempt gave back what it staged: a disk full is not filled further by retrying.
    assert_eq!(store.staged(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_disk_a_crashed_load_filled_is_freed_by_the_next_replay_before_it_writes() {
    let store = Arc::new(Memory::of(64 << 10));
    let pipeline_id = PipelineId::parse("wal-freed").expect("a valid pipeline");
    // A load staged until the disk was full, and crashed.
    let crashed = Chunk {
        load: rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 1),
        number: 0,
    };
    store
        .open_log(&pipeline_id, crashed.load)
        .await
        .expect("opens");
    let mut staged = store.stage(&pipeline_id, crashed).await.expect("stages");
    while staged.append(Bytes::from_static(&[0; 1024])).await.is_ok() {}
    drop(staged);
    let mut events = ScriptStream::new("events", 2, 30, 7);
    events.replayable = false;
    let (_, source) = Script::new(vec![events]).connect("wal_freed").await;
    let plan = pipeline("wal-freed", [stream("events").read(ReadMode::Incremental)]);
    let freed = logging_engine(commit_every(10), Arc::clone(&store) as Arc<dyn WalStore>)
        .run(plan, source, memory("wal_freed").await)
        .await;
    assert_eq!(
        freed.report.status,
        RunStatus::Succeeded,
        "{:?}",
        freed.error
    );
    assert_eq!(published_ids("wal_freed", "events"), ids(2, 30));
    assert_eq!(store.loads(&pipeline_id).await.expect("lists"), []);
    assert_eq!(store.staged(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_log_written_for_one_destination_is_never_replayed_into_another() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let plan = pipeline("wal-bound", [stream("events").read(ReadMode::Incremental)]);
    // One pipeline, its logs in one place, run into two destinations from sources of their own.
    let mut sources = Vec::new();
    for destination in ["wal_bound_one", "wal_bound_two"] {
        let mut events = ScriptStream::new("events", 1, 10, 5);
        events.replayable = false;
        let (script, source) = Script::new(vec![events]).connect(destination).await;
        let loaded = logging_engine(commit_every(10), Arc::clone(&store))
            .run(plan.clone(), Arc::clone(&source), memory(destination).await)
            .await;
        assert_eq!(
            loaded.report.status,
            RunStatus::Succeeded,
            "{:?}",
            loaded.error
        );
        sources.push((script, source));
    }
    // A load into the first fails to commit what its source was told of: its log holds it.
    let (script, source) = &sources[0];
    script.streams[0].grow(10);
    let failed = logging_engine(retrying(1), Arc::clone(&store))
        .run(
            plan.clone(),
            Arc::clone(source),
            failing(memory("wal_bound_one").await, Step::Commit),
        )
        .await;
    assert_eq!(failed.report.status, RunStatus::Failed);
    // The second destination's run refuses the log; the first's lands it.
    let refused = logging_engine(retrying(1), Arc::clone(&store))
        .run(
            plan.clone(),
            Arc::clone(&sources[1].1),
            memory("wal_bound_two").await,
        )
        .await;
    let error = refused.error.expect("another destination's log");
    assert_eq!(error.code(), Some("wal_foreign"), "{error}");
    let landed = logging_engine(retrying(1), Arc::clone(&store))
        .run(plan, Arc::clone(source), memory("wal_bound_one").await)
        .await;
    assert_eq!(
        landed.report.status,
        RunStatus::Succeeded,
        "{:?}",
        landed.error
    );
    assert_eq!(published_ids("wal_bound_one", "events"), ids(1, 20));
}

#[tokio::test(start_paused = true)]
async fn a_pipeline_s_destination_takes_logs_from_one_store_only() {
    let (one, two) = (
        tempfile::tempdir().expect("a temporary directory"),
        tempfile::tempdir().expect("a temporary directory"),
    );
    let plan = pipeline("wal-stores", [stream("events").read(ReadMode::Incremental)]);
    let mut events = ScriptStream::new("events", 1, 10, 5);
    events.replayable = false;
    let (script, source) = Script::new(vec![events]).connect("wal_stores").await;
    let run = |base: &std::path::Path| {
        let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base));
        let (plan, source) = (plan.clone(), Arc::clone(&source));
        async move {
            logging_engine(commit_every(10), store)
                .run(plan, source, memory("wal_stores").await)
                .await
        }
    };
    let first = run(one.path()).await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    // Another store for the same pipeline and destination is refused before anything is read.
    script.streams[0].grow(10);
    let reads = script.reads.load(std::sync::atomic::Ordering::SeqCst);
    let refused = run(two.path()).await;
    let error = refused.error.expect("another store");
    assert_eq!(error.code(), Some("wal_store_other"), "{error}");
    assert!(!error.is_retryable());
    assert_eq!(
        script.reads.load(std::sync::atomic::Ordering::SeqCst),
        reads
    );
    let again = run(one.path()).await;
    assert_eq!(
        again.report.status,
        RunStatus::Succeeded,
        "{:?}",
        again.error
    );
    assert_eq!(published_ids("wal_stores", "events"), ids(1, 20));
}

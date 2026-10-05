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
    commit_every, engine, logging_engine, memory, memory as memory_destination, pipeline,
    published_ids, retrying, stream,
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

/// Loads `rows` rows of one partition, a checkpoint every batch of twenty and a commit every
/// `every` rows, through a log of `log_bytes`, with `engine`; the run's outcome.
async fn checkpointing(
    name: &str,
    (rows, every): (u64, u64),
    log_bytes: u64,
    engine: impl FnOnce(
        rdlt_engine::EngineConfigBuilder,
        Arc<dyn WalStore>,
    ) -> crate::support::TestEngine,
) -> RunOutcome {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let mut events = ScriptStream::new("events", 1, rows, 20);
    events.replayable = false;
    let (_, source) = Script::new(vec![events]).connect(name).await;
    let growth = GrowthLimits::default()
        .with_log_bytes(log_bytes)
        .expect("a valid limit");
    let plan = pipeline(
        &name.replace('_', "-"),
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = engine(commit_every(every).growth(growth), store)
        .run(plan, source, memory(name).await)
        .await;
    if outcome.report.status == RunStatus::Succeeded {
        assert_eq!(published_ids(name, "events"), ids(1, rows), "{name}");
    }
    outcome
}

#[tokio::test(start_paused = true)]
async fn a_source_that_checkpoints_loads_through_a_full_log_whatever_it_sends() {
    for (rows, log_bytes) in [(400, 16 << 10), (4_000, 256 << 10), (40_000, 1 << 20)] {
        let name = format!("wal_backpressed_{rows}");
        let outcome = checkpointing(&name, (rows, 2_000), log_bytes, logging_engine).await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{rows}: {:?}",
            outcome.error
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_that_checkpoints_loads_through_a_full_log_on_the_real_clock() {
    const ROWS: u64 = 10_000;
    let engine = |config, store| crate::support::pooled_logging_engine(config, 2, store);
    let outcome = checkpointing("wal_backpressed_real", (ROWS, 20), 256 << 10, engine).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test(start_paused = true)]
async fn a_batch_waiting_for_room_in_the_log_brings_its_commit_at_once() {
    // No policy makes a commit due, and a barrier would wait an hour for a partition that does
    // not answer: only the batch waiting for room asks for the commits, and is not waited for.
    let never = rdlt_engine::CommitPolicy::new(None, Some(u64::MAX), None).expect("a policy");
    let engine = |config: rdlt_engine::EngineConfigBuilder, store| {
        let config = config
            .commit(never)
            .barrier_wait(std::time::Duration::from_secs(3_600));
        logging_engine(config, store)
    };
    let outcome = checkpointing("wal_rung", (4_000, 2_000), 16 << 10, engine).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(outcome.report.commits > 1, "{:?}", outcome.report);
}

#[tokio::test(start_paused = true)]
async fn what_a_store_stages_in_memory_is_charged_to_the_budget_and_bounded_by_it() {
    let memory = 64_u64 << 20;
    let log_share = memory / 16;
    // Half the share, what the default part takes of the default budget's: what a carry reads
    // back at once is held beside it from the other half.
    let most = log_share / 2;
    for (staging, refused) in [(most, false), (most + 1, true)] {
        let name = format!("wal_staging_{refused}");
        let store = Arc::new(Memory::staging(staging));
        let mut events = ScriptStream::new("events", 1, 40, 20);
        events.replayable = false;
        let (_, source) = Script::new(vec![events]).connect(&name).await;
        let plan = pipeline(
            &name.replace('_', "-"),
            [stream("events").read(ReadMode::Incremental)],
        );
        let config = commit_every(20).memory(memory);
        let outcome = logging_engine(config, Arc::clone(&store) as Arc<dyn WalStore>)
            .run(plan, source, memory_destination(&name).await)
            .await;
        if refused {
            let error = outcome.error.expect("refused");
            assert_eq!(
                error.code(),
                Some("wal_staging_exceeds_budget"),
                "{error:?}"
            );
            assert_eq!(error.kind(), ErrorKind::Config);
            assert_eq!(published_ids(&name, "events"), Vec::<i64>::new());
        } else {
            assert_eq!(
                outcome.report.status,
                RunStatus::Succeeded,
                "{:?}",
                outcome.error
            );
            assert!(
                outcome.report.peak_memory >= staging,
                "{:?}",
                outcome.report
            );
        }
    }
}

/// Loads `rows` rows of each of `partitions` partitions, a checkpoint every `every` batches of
/// twenty and a commit every `commit` rows, through a log of `log_bytes`, with `engine`; the
/// run's outcome.
async fn partitioned(
    name: &str,
    (partitions, every, commit): (usize, u64, u64),
    rows: u64,
    log_bytes: u64,
    engine: impl FnOnce(
        rdlt_engine::EngineConfigBuilder,
        Arc<dyn WalStore>,
    ) -> crate::support::TestEngine,
) -> RunOutcome {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let mut events = ScriptStream::new("events", partitions, rows, 20);
    events.replayable = false;
    events.checkpoint_every = every;
    let (_, source) = Script::new(vec![events]).connect(name).await;
    let growth = GrowthLimits::default()
        .with_log_bytes(log_bytes)
        .expect("a valid limit");
    let plan = pipeline(
        &name.replace('_', "-"),
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = engine(commit_every(commit).growth(growth), store)
        .run(plan, source, memory(name).await)
        .await;
    if outcome.report.status == RunStatus::Succeeded {
        assert_eq!(
            published_ids(name, "events"),
            ids(partitions, rows),
            "{name}"
        );
    }
    outcome
}

#[tokio::test(start_paused = true)]
async fn a_partition_whose_checkpoints_lie_nearly_a_log_apart_loads_through_it() {
    // Batches of twenty rows take some 600 bytes in the log: 28 between checkpoints fill most
    // of 16 KiB, and nothing but its own checkpoint commits them.
    for every in [8, 16, 24, 28] {
        let name = format!("wal_gap_{every}");
        let shape = (1, every, 1_000_000);
        let outcome = partitioned(&name, shape, 4_000, 16 << 10, logging_engine).await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{every}: {:?}",
            outcome.error
        );
    }
}

#[tokio::test(start_paused = true)]
async fn several_partitions_load_through_a_log_their_open_frames_fit() {
    for (partitions, every, log_bytes) in [
        (2, 1, 64 << 10),
        (4, 3, 64 << 10),
        (8, 3, 64 << 10),
        (4, 1, 16 << 10),
        (16, 1, 32 << 10),
    ] {
        let name = format!("wal_partitions_{partitions}_{every}_{log_bytes}");
        let shape = (partitions, every, 1_000);
        let outcome = partitioned(&name, shape, 800, log_bytes, logging_engine).await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{name}: {:?}",
            outcome.error
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_partitions_load_through_a_log_their_open_frames_fit_on_the_real_clock() {
    let engine = |config, store| crate::support::pooled_logging_engine(config, 2, store);
    for (partitions, every, log_bytes) in [(4, 3, 64 << 10), (8, 1, 64 << 10), (4, 1, 16 << 10)] {
        let name = format!("wal_partitions_real_{partitions}_{every}_{log_bytes}");
        let shape = (partitions, every, 20);
        let outcome = partitioned(&name, shape, 800, log_bytes, engine).await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{name}: {:?}",
            outcome.error
        );
    }
}

/// Loads a stream whose one partition checkpoints every `every` batches beside one whose
/// partition checkpoints every batch, both of `rows` rows in batches of `batch_rows` rows written
/// as they come, so each commit's chunk holds a frame of each, through a log of `log_bytes` in
/// `store`, with `engine` and the first commit landing as `destination` lets it; the run's outcome.
async fn slow_beside_fast_into(
    name: &str,
    (every, rows, batch_rows): (u64, u64, u64),
    (store, log_bytes): (Arc<dyn WalStore>, u64),
    destination: Arc<dyn rdlt_connector::Destination>,
    engine: impl FnOnce(
        rdlt_engine::EngineConfigBuilder,
        Arc<dyn WalStore>,
    ) -> crate::support::TestEngine,
) -> RunOutcome {
    let mut slow = ScriptStream::new("slow", 1, rows, batch_rows);
    slow.replayable = false;
    slow.checkpoint_every = every;
    let mut fast = ScriptStream::new("fast", 1, rows, batch_rows);
    fast.replayable = false;
    let (_, source) = Script::new(vec![slow, fast]).connect(name).await;
    let growth = GrowthLimits::default()
        .with_log_bytes(log_bytes)
        .expect("a valid limit");
    let plan = pipeline(
        &name.replace('_', "-"),
        [
            stream("slow").read(ReadMode::Incremental),
            stream("fast").read(ReadMode::Incremental),
        ],
    );
    // One batch a frame, written at once.
    let batch = rdlt_engine::BatchPolicy::new(
        64 << 20,
        batch_rows,
        std::time::Duration::from_millis(1),
        16,
    )
    .expect("a valid policy");
    let config = commit_every(1).growth(growth).batch(batch);
    let outcome = engine(config, store).run(plan, source, destination).await;
    if outcome.report.status == RunStatus::Succeeded {
        assert_eq!(published_ids(name, "slow"), ids(1, rows), "{name}");
        assert_eq!(published_ids(name, "fast"), ids(1, rows), "{name}");
    }
    outcome
}

/// As [`slow_beside_fast_into`], in batches of one row, through a local log of `log_bytes`.
async fn slow_beside_fast(
    name: &str,
    (every, rows): (u64, u64),
    log_bytes: u64,
    engine: impl FnOnce(
        rdlt_engine::EngineConfigBuilder,
        Arc<dyn WalStore>,
    ) -> crate::support::TestEngine,
) -> RunOutcome {
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let destination = memory(name).await;
    slow_beside_fast_into(
        name,
        (every, rows, 1),
        (store, log_bytes),
        destination,
        engine,
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn a_slow_first_commit_beside_a_partition_committing_every_batch_loads_through_its_log() {
    // A frame of one row takes some 1,700 bytes: the slow partition holds 60% of the log open
    // while the first commit takes two seconds to land, and every frame logged meanwhile waits
    // in the chunks it gathers.
    let (frames, every) = (100, 60);
    let name = "wal_slow_first";
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let hook: crate::support::hooked::Hook = Arc::new(|| {
        Box::pin(async { tokio::time::sleep(std::time::Duration::from_secs(2)).await })
    });
    let destination = crate::support::hooked::hooked(
        memory(name).await,
        crate::support::hooked::At::Landed,
        hook,
    );
    let shape = (every, 3 * every + 10, 1);
    let log = (store, frames * 1_700);
    let outcome = slow_beside_fast_into(name, shape, log, destination, logging_engine).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[cfg(feature = "object-store")]
#[tokio::test(start_paused = true)]
async fn frames_larger_than_the_log_s_memory_are_carried_a_piece_at_a_time() {
    // A budget whose share for logs, beside a store's part, holds less than one batch frame: a
    // carry copies each frame in pieces, so the slow partition's frames still leave the chunks
    // the fast one settles.
    use object_store::memory::InMemory;
    use rdlt_engine::{ObjectStoreOptions, ObjectStoreWal, SystemClock};
    let (rows, frame, every) = (160_000, 1_920_000, 24);
    let name = "wal_large_frames";
    let options = ObjectStoreOptions::default().with_part_bytes((768 << 10).try_into().unwrap());
    let objects = Arc::new(InMemory::new());
    let wal = ObjectStoreWal::open(objects as _, "logs", Arc::new(SystemClock), options)
        .await
        .expect("the probe passes");
    let store: Arc<dyn WalStore> = Arc::new(wal);
    let destination = memory(name).await;
    let engine = |config: rdlt_engine::EngineConfigBuilder, store| {
        logging_engine(config.memory(34 << 20).partitions(2), store)
    };
    let shape = (every, rows * (2 * every + 4), rows);
    let outcome =
        slow_beside_fast_into(name, shape, (store, 40 * frame), destination, engine).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test(start_paused = true)]
async fn a_slow_partition_beside_one_committing_every_batch_loads_through_a_log_of_small_frames() {
    // A frame of one row takes some 1,700 bytes, about what a commit's frames take: 8 of them
    // are under half of 32 KiB.
    for every in [3, 8] {
        let name = format!("wal_slow_{every}");
        let outcome = slow_beside_fast(&name, (every, 100), 32 << 10, logging_engine).await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{every}: {:?}",
            outcome.error
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_partition_beside_one_committing_every_batch_loads_on_the_real_clock() {
    let engine = |config, store| crate::support::pooled_logging_engine(config, 2, store);
    let outcome = slow_beside_fast("wal_slow_real", (8, 100), 32 << 10, engine).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

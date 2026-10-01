//! A batch cut into pieces stays one unit of a load: whatever happens between its pieces, every
//! row is published once and in order, and no checkpoint lands among them.

use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow_array::Int64Array;
use rdlt_connector::serve::Served;
use rdlt_connector::{
    ConnectorError, ConnectorErrorKind, ConnectorId, Destination, Partition, PipelineId, Push,
    ReadRequest, Role, Source as _, SourceEvent, StreamName, partition_channel, source_factory,
};
use rdlt_connector_reference::published;
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, LocalWal, PipelinePlan, RayonPool, RetryPolicy, RunControl,
    RunOutcome, RunStatus, StopMode, StreamPlan, SystemEnv, WalStore,
};
use rdlt_host::{Connect, Connection, ConnectorRef, Kills, Options, Provider as _, RemoteSource};
use rdlt_wire::Limits;
use rdlt_wire::limits::{MIN_BATCH_ROWS, MIN_BATCH_VALUES};

use crate::support::connectors::{FLAGGED, Hook, Ticks, Writes, Writing};
use crate::support::{served, served_within};

/// Rows the source sends as one batch, and the flags beside each row's id: 301 values a row.
const ROWS: u64 = 5_000;
const FLAGS: usize = 300;

/// The pieces a destination taking the fewest rows a frame gets of the batch.
const PIECES: usize = 5;

/// The source of one batch of [`ROWS`] rows, served, read within `options`.
async fn ticks(options: Options) -> RemoteSource {
    tagged("", options).await
}

/// As [`ticks`], its rows counted under `tag`.
async fn tagged(tag: &str, options: Options) -> RemoteSource {
    let io = served(Served::new().with_source(source_factory::<Ticks>()));
    let config = serde_json::json!({ "rows": ROWS, "flags": FLAGS, "tag": tag });
    let connection = Connection::connect(io, Role::Source, &config, options)
        .await
        .expect("the source handshakes");
    RemoteSource::new(connection)
}

/// The host's end of a socket serving the memory destination over `hook`, taking the fewest rows
/// a frame the protocol allows, so the host cuts every batch it writes.
fn hooked(hook: &'static Hook) -> tokio::net::UnixStream {
    let limits = Limits {
        batch_rows: MIN_BATCH_ROWS,
        ..Limits::default()
    };
    let hooked = Served::new().with_destination(Writes::factory(Writing::Hooked(hook)));
    served_within(hooked, limits)
}

/// The memory destination over `hook` and `store`, reached again whenever its connection is lost
/// or `kills` cuts it.
async fn destination(hook: &'static Hook, store: &str, kills: &Kills) -> Arc<dyn Destination> {
    let connect = Connect::new(move || {
        let stream = hooked(hook);
        Box::pin(async move { Ok(Box::new(stream) as Box<dyn rdlt_host::Stream>) })
    })
    .kills(kills);
    let reference = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("valid"));
    let config = serde_json::json!({ "store": store });
    let placed = connect.destination(&reference, &config).await;
    Arc::from(placed.expect("the destination is reached").connector)
}

/// An engine that tries a failed attempt again at once, keeping logs in `wal` where given.
fn engine(wal: Option<Arc<dyn WalStore>>) -> Engine {
    let retry = RetryPolicy::default()
        .max_attempts(3)
        .initial(Duration::from_millis(10))
        .max_delay(Duration::from_millis(50));
    let commit = CommitPolicy::new(None, Some(100), None).expect("a row threshold is valid");
    let config = EngineConfig::builder().commit(commit).retry(retry).lanes(2);
    let config = config.build().expect("the configuration is valid");
    let pool = RayonPool::new(NonZeroUsize::new(2).expect("not zero")).expect("a pool");
    let env = SystemEnv::new(pool);
    let env = match wal {
        Some(wal) => env.with_wal(wal),
        None => env,
    };
    Engine::new(config, Arc::new(env))
}

fn plan(pipeline: &str) -> PipelinePlan {
    let stream = StreamName::new("ticks").expect("a valid stream name");
    let pipeline = PipelineId::parse(pipeline).expect("a valid pipeline id");
    PipelinePlan::new(pipeline, [StreamPlan::new(stream)]).expect("a plan")
}

/// The ids published to `store`, in order.
fn ids(store: &str) -> Vec<i64> {
    let batches = published(store, "ticks");
    let ids = batches.iter().flat_map(|batch| {
        let ids = batch.column_by_name("id").expect("the ids");
        let ids = ids.as_any().downcast_ref::<Int64Array>().expect("integers");
        ids.values().to_vec()
    });
    ids.collect()
}

fn every_row() -> Vec<i64> {
    (0..i64::try_from(ROWS).expect("few rows")).collect()
}

fn succeeded(outcome: &RunOutcome, attempts: usize) {
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.attempts.len(), attempts);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_that_fails_between_a_batchs_pieces_loads_every_row_once() {
    static HOOK: Hook = Hook::at(2);
    HOOK.then(|| {
        Err(ConnectorError::new(
            ConnectorErrorKind::Transient,
            "the second piece was refused",
        ))
    });
    let destination = destination(&HOOK, "cuts_failed", &Kills::new()).await;
    let source = Arc::new(ticks(Options::default()).await);
    let outcome = engine(None)
        .run(plan("cuts-failed"), source, destination)
        .await;
    succeeded(&outcome, 2);
    // The first piece of the failed attempt was staged and never published.
    assert_eq!(ids("cuts_failed"), every_row());
    assert_eq!(HOOK.writes(), 2 + PIECES);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connector_lost_between_a_batchs_pieces_loads_every_row_once() {
    static HOOK: Hook = Hook::at(2);
    let kills = Kills::new();
    HOOK.then({
        let kills = kills.clone();
        move || {
            kills.kill();
            Ok(())
        }
    });
    let destination = destination(&HOOK, "cuts_killed", &kills).await;
    let source = Arc::new(ticks(Options::default()).await);
    let outcome = engine(None)
        .run(plan("cuts-killed"), source, destination)
        .await;
    succeeded(&outcome, 2);
    assert_eq!(kills.count(), 1);
    assert_eq!(ids("cuts_killed"), every_row());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_stopped_between_a_batchs_pieces_publishes_none_of_them_and_the_next_run_all() {
    static HOOK: Hook = Hook::at(2);
    static CONTROL: OnceLock<RunControl> = OnceLock::new();
    HOOK.then(|| {
        CONTROL.get().expect("the run began").stop(StopMode::Now);
        Ok(())
    });
    let kills = Kills::new();
    let destination = destination(&HOOK, "cuts_stopped", &kills).await;
    let source = Arc::new(ticks(Options::default()).await);
    let run = engine(None).run(plan("cuts-stopped"), source, Arc::clone(&destination));
    CONTROL.set(run.control()).ok();
    let outcome = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("the run stops");
    assert_eq!(outcome.report.status, RunStatus::Cancelled);
    assert!(HOOK.writes() < PIECES, "{} writes", HOOK.writes());
    assert_eq!(ids("cuts_stopped"), Vec::<i64>::new());
    let source = Arc::new(ticks(Options::default()).await);
    let outcome = engine(None)
        .run(plan("cuts-stopped"), source, destination)
        .await;
    succeeded(&outcome, 1);
    assert_eq!(ids("cuts_stopped"), every_row());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_logged_batch_is_cut_again_when_its_commit_is_replayed() {
    static HOOK: Hook = Hook::at(usize::MAX);
    HOOK.failing_commit.store(true, Ordering::SeqCst);
    let logs = tempfile::tempdir().expect("a temporary directory");
    let wal: Arc<dyn WalStore> = Arc::new(LocalWal::new(logs.path()));
    let destination = destination(&HOOK, "cuts_replayed", &Kills::new()).await;
    let source = Arc::new(tagged("cuts_replayed", Options::default()).await);
    let outcome = engine(Some(wal))
        .run(plan("cuts-replayed").with_wal(true), source, destination)
        .await;
    succeeded(&outcome, 2);
    assert_eq!(ids("cuts_replayed"), every_row());
    // The rows of the commit the destination missed came from the log, uncut there, and were cut
    // for the destination again: the source sent them once, and the destination staged them twice.
    assert_eq!(HOOK.writes(), 2 * PIECES);
    let sent = FLAGGED.lock().expect("the lock is not poisoned");
    assert_eq!(sent.get("cuts_replayed"), Some(&ROWS));
}

#[tokio::test]
async fn a_checkpoint_asked_for_while_a_batchs_pieces_wait_follows_its_last_piece() {
    // The host takes the fewest values a frame the protocol allows, and grants credit for one
    // frame at a time: the batch's second piece waits for credit while the barrier arrives.
    let options = Options {
        limits: Limits {
            batch_values: MIN_BATCH_VALUES,
            ..Limits::default()
        },
        read_window: 1,
        ..Options::default()
    };
    let source = ticks(options).await;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(1).expect("not zero"));
    feed.request_checkpoint(1);
    let request = ReadRequest::new(
        StreamName::new("ticks").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let reading = tokio::spawn(async move { source.read(request, sink).await });
    let mut events = Vec::new();
    while let Some(event) = feed.recv().await {
        events.push(match event {
            SourceEvent::Push(Push::Arrow(batch)) => Ok(batch.num_rows()),
            SourceEvent::Checkpoint { answers, .. } => Err(answers),
            other => panic!("an event other than a push or a checkpoint: {other:?}"),
        });
    }
    tokio::time::timeout(Duration::from_secs(30), reading)
        .await
        .expect("the read ends")
        .expect("the read does not panic")
        .expect("the read succeeds");
    // 301 values a row: a frame's values hold 3483 rows.
    assert_eq!(events, [Ok(3_483), Ok(1_517), Err(Some(1))]);
}

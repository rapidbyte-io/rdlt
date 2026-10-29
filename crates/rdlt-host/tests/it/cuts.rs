//! A batch cut into pieces stays one unit of a load: whatever happens between its pieces, every
//! row is published once and in order, and no checkpoint lands among them.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow_array::Int64Array;
use rdlt_connector::BoxFuture;
use rdlt_connector::serve::Served;
use rdlt_connector::{
    ConnectorError, ConnectorErrorKind, ConnectorId, Destination, Partition, PipelineId, Push,
    ReadRequest, Role, Source as _, SourceEvent, StreamName, partition_channel, source_factory,
};
use rdlt_connector_reference::published;
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, PipelinePlan, RetryPolicy, RunControl, RunOutcome,
    RunStatus, StopMode, StreamPlan,
};
use rdlt_host::{
    Connect, Connection, ConnectorRef, Kills, Opener, Opening, Options, Provider as _,
    RemoteSource, Stream,
};
use rdlt_wire::Limits;
use rdlt_wire::limits::{MIN_BATCH_ROWS, MIN_BATCH_VALUES};

use crate::support::connectors::{FLAGGED, Hook, Ticks, Writes, Writing};
use crate::support::{opener, opener_within, served};

/// Rows the source sends as one batch, and the flags beside each row's id: 301 values a row.
const ROWS: u64 = 9_000;
const FLAGS: usize = 300;

/// The pieces a destination taking the fewest rows a frame gets of the batch.
const PIECES: usize = 9;

/// The source of one batch of [`ROWS`] rows, served, read within `options`.
async fn ticks(options: &Options) -> RemoteSource {
    tagged("", options).await
}

/// As [`ticks`], its rows counted under `tag`.
async fn tagged(tag: &str, options: &Options) -> RemoteSource {
    let io = served(Served::new().with_source(source_factory::<Ticks>()));
    let config = serde_json::json!({ "rows": ROWS, "flags": FLAGS, "tag": tag });
    let connection = Connection::connect(io, Role::Source, &config, *options)
        .await
        .expect("the source handshakes");
    RemoteSource::new(connection)
}

/// The host's end of a socket serving the memory destination over `hook`, taking the fewest rows
/// a frame the protocol allows, so the host cuts every batch it writes.
fn hooked(hook: &'static Hook) -> Box<dyn Opener> {
    let limits = Limits {
        batch_rows: MIN_BATCH_ROWS,
        ..Limits::default()
    };
    let hooked = Served::new().with_destination(Writes::factory(Writing::Hooked(hook)));
    opener_within(hooked, limits)
}

/// The memory destination over `hook` and `store`, reached again whenever its connection is lost
/// or `kills` cuts it.
async fn destination(hook: &'static Hook, store: &str, kills: &Kills) -> Arc<dyn Destination> {
    let connect = Connect::new(move || {
        let opener = hooked(hook);
        Box::pin(async move { Ok(opener) })
    })
    .kills(kills);
    let reference = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("valid"));
    let config = serde_json::json!({ "store": store });
    let placed = connect.destination(&reference, &config).await;
    Arc::from(placed.expect("the destination is reached").connector)
}

/// An engine that tries a failed attempt again at once.
fn engine() -> Engine {
    let retry = RetryPolicy::default()
        .max_attempts(3)
        .initial(Duration::from_millis(10))
        .max_delay(Duration::from_millis(50));
    let commit = CommitPolicy::new(None, Some(100), None).expect("a row threshold is valid");
    let config = EngineConfig::builder().commit(commit).retry(retry).lanes(2);
    let config = config.build().expect("the configuration is valid");
    Engine::new(config, Arc::new(crate::support::system_env()))
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
    let source = Arc::new(ticks(&Options::default()).await);
    let outcome = engine().run(plan("cuts-failed"), source, destination).await;
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
    let source = Arc::new(ticks(&Options::default()).await);
    let outcome = engine().run(plan("cuts-killed"), source, destination).await;
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
    let source = Arc::new(ticks(&Options::default()).await);
    let run = engine().run(plan("cuts-stopped"), source, Arc::clone(&destination));
    CONTROL.set(run.control()).ok();
    let outcome = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("the run stops");
    assert_eq!(outcome.report.status, RunStatus::Cancelled);
    assert!(HOOK.writes() < PIECES, "{} writes", HOOK.writes());
    assert_eq!(ids("cuts_stopped"), Vec::<i64>::new());
    let source = Arc::new(ticks(&Options::default()).await);
    let outcome = engine()
        .run(plan("cuts-stopped"), source, destination)
        .await;
    succeeded(&outcome, 1);
    assert_eq!(ids("cuts_stopped"), every_row());
}

/// An opener whose streams all fail once the host has read `left` bytes of them together.
///
/// Once they have, `kills` cuts every stream of the connection, as a connector's end going does.
struct Cutting {
    inner: Box<dyn Opener>,
    left: Arc<AtomicUsize>,
    kills: Kills,
}

impl Opener for Cutting {
    fn open(&self, opening: Opening) -> BoxFuture<'_, std::io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let inner = self.inner.open(opening).await?;
            let left = Arc::clone(&self.left);
            let kills = self.kills.clone();
            Ok(Box::new(Severed { inner, left, kills }) as Box<dyn Stream>)
        })
    }
}

/// A stream that fails once the host has read what `left` counts down, and then has `kills`
/// cut its connection's every other stream.
struct Severed {
    inner: Box<dyn Stream>,
    left: Arc<AtomicUsize>,
    kills: Kills,
}

impl tokio::io::AsyncRead for Severed {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.left.load(Ordering::SeqCst) == 0 {
            return std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()));
        }
        let before = buffer.filled().len();
        let read = std::pin::Pin::new(&mut self.inner).poll_read(context, buffer);
        let taken = buffer.filled().len() - before;
        let left = &self.left;
        let counted = left.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            Some(left.saturating_sub(taken))
        });
        if counted.is_ok_and(|was| was > 0 && was <= taken) {
            self.kills.kill();
        }
        read
    }
}

impl tokio::io::AsyncWrite for Severed {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(context, bytes)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_lost_between_a_batchs_pieces_commits_none_of_them_and_the_next_attempt_all() {
    // The source's rows reach the host as three pieces, of about 312, 312 and 185 kilobytes,
    // after a schema of 13. Its first connection fails once the host has read 700 kilobytes of
    // it: two pieces, and part of the third.
    static SEVERED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    static HOOK: Hook = Hook::at(usize::MAX);
    let options = Options {
        limits: Limits {
            batch_values: MIN_BATCH_VALUES,
            ..Limits::default()
        },
        ..Options::default()
    };
    let kills = Kills::new();
    let cutting = kills.clone();
    let connect = Connect::new(move || {
        let inner = opener(Served::new().with_source(source_factory::<Ticks>()));
        let left = if SEVERED.swap(true, Ordering::SeqCst) {
            usize::MAX
        } else {
            700_000
        };
        let left = Arc::new(AtomicUsize::new(left));
        let kills = cutting.clone();
        let opener = Box::new(Cutting { inner, left, kills }) as Box<dyn Opener>;
        Box::pin(async move { Ok(opener) })
    })
    .options(options)
    .kills(&kills);
    let reference = ConnectorRef::new(ConnectorId::parse("test.ticks").expect("valid"));
    let config = serde_json::json!({ "rows": ROWS, "flags": FLAGS, "tag": "cuts_lost" });
    let placed = connect.source(&reference, &config).await;
    let source: Arc<dyn rdlt_connector::Source> =
        Arc::from(placed.expect("the source is reached").connector);
    let destination = destination(&HOOK, "cuts_lost", &Kills::new()).await;
    let outcome = engine().run(plan("cuts-lost"), source, destination).await;
    succeeded(&outcome, 2);
    // The source sent the rows twice, and each is published once: nothing of the reading that
    // was cut off was committed. The pieces it gathered are written when its read ends, racing
    // the end of its attempt, so the destination's writes are not counted.
    let sent = FLAGGED.lock().expect("the lock is not poisoned");
    assert_eq!(sent.get("cuts_lost"), Some(&(2 * ROWS)));
    assert_eq!(ids("cuts_lost"), every_row());
}

#[tokio::test]
async fn a_checkpoint_asked_for_between_a_batchs_pieces_follows_its_last_piece() {
    // The host takes the fewest values a frame the protocol allows, so the batch goes as three
    // pieces, and the engine takes an event at a time: the host asks for a checkpoint with the
    // first piece in hand, and the source answers at once; its answer follows the last piece.
    let options = Options {
        limits: Limits {
            batch_values: MIN_BATCH_VALUES,
            ..Limits::default()
        },
        ..Options::default()
    };
    let io = served(Served::new().with_source(source_factory::<Ticks>()));
    let config = serde_json::json!({ "rows": ROWS, "flags": FLAGS, "pace_ms": 1 });
    let connection = Connection::connect(io, Role::Source, &config, options);
    let source = RemoteSource::new(connection.await.expect("the source handshakes"));
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(1).expect("not zero"));
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
        if events.len() == 1 {
            feed.request_checkpoint(1);
        }
    }
    tokio::time::timeout(Duration::from_secs(30), reading)
        .await
        .expect("the read ends")
        .expect("the read does not panic")
        .expect("the read succeeds");
    // 301 values a row: a frame's values hold 3483 rows.
    assert_eq!(events, [Ok(3_483), Ok(3_483), Ok(2_034), Err(Some(1))]);
}

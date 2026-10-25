//! A run within what its connectors carry at once: it reads fewer partitions at once than its
//! source carries calls, and holds fewer writers open than its destination does, keeping a call
//! of each for the other call the engine makes into it at a time.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, CommitMeta, ConnectContext, Cursor, Destination,
    DestinationSession, DestinationWriter, OpenContext, OpenedSession, PartitionId, PartitionPlan,
    PartitionSink, ReadMode, ReadRequest, Receipt, Result, SegmentId, Source, StreamName,
    StreamState, TableChange, TableRef, WriteStats, acknowledging_source_factory,
};
use rdlt_connector_reference::LogSource;
use rdlt_engine::{EngineConfig, ErrorKind, GrowthLimits, RunOutcome, RunStatus, Until, codes};
use serde_json::json;

use crate::support::{engine, generator, memory, pipeline, published_rows, stream};

fn calls(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).expect("a count of calls is more than zero")
}

/// A log source of one stream, `events`, of `partitions` partitions and no messages, carrying
/// `calls` at once.
async fn log(group: &str, partitions: u32, calls: NonZeroUsize) -> Arc<dyn Source> {
    let streams = json!([{ "name": "events", "partitions": partitions, "messages": 0 }]);
    let config = json!({ "seed": 5, "group": group, "streams": streams });
    let inner = acknowledging_source_factory::<LogSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the log connects");
    carrying(Arc::from(inner), calls, Arc::default())
}

/// `inner`, carrying `calls` at once, its reads counted in `reading`.
fn carrying(inner: Arc<dyn Source>, calls: NonZeroUsize, reading: Arc<Open>) -> Arc<dyn Source> {
    Arc::new(CarryingSource {
        inner,
        calls,
        reading,
    })
}

/// A source that carries `calls` at once, counting the reads in flight.
struct CarryingSource {
    inner: Arc<dyn Source>,
    calls: NonZeroUsize,
    reading: Arc<Open>,
}

impl Source for CarryingSource {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        self.inner.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        self.inner.plan(stream, state)
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        let reading = Arc::clone(&self.reading);
        let read = self.inner.read(request, sink);
        Box::pin(async move {
            reading.opened();
            let read = read.await;
            reading.closed();
            read
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        self.inner.committed(stream, cursors)
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        Some(self.calls)
    }
}

/// A destination that carries `calls` at once, counting the writers its sessions hold open.
struct CarryingDestination {
    inner: Arc<dyn Destination>,
    calls: NonZeroUsize,
    open: Arc<Open>,
}

/// What a carrying connector holds open, its reads or writers, the most it held at once, and the
/// sessions a destination opened.
#[derive(Debug, Default)]
struct Open {
    now: AtomicUsize,
    most: AtomicUsize,
    sessions: AtomicUsize,
}

impl Open {
    fn opened(&self) {
        let now = self.now.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
    }

    fn closed(&self) {
        self.now.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Destination for CarryingDestination {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            self.open.sessions.fetch_add(1, Ordering::SeqCst);
            let opened = self.inner.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(CountingSession {
                    inner: opened.session,
                    open: Arc::clone(&self.open),
                }),
                ..opened
            })
        })
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        Some(self.calls)
    }
}

struct CountingSession {
    inner: Box<dyn DestinationSession>,
    open: Arc<Open>,
}

impl DestinationSession for CountingSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let inner = self.inner.writer(table).await?;
            self.open.opened();
            let open = Arc::clone(&self.open);
            Ok(Box::new(CountedWriter { inner, open }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        self.inner.commit(meta)
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}

/// A writer counted open until it is dropped.
struct CountedWriter {
    inner: Box<dyn DestinationWriter>,
    open: Arc<Open>,
}

impl Drop for CountedWriter {
    fn drop(&mut self) {
        self.open.closed();
    }
}

impl DestinationWriter for CountedWriter {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        self.inner.write(segment, batch)
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        self.inner.flush()
    }
}

/// A configuration that follows its source with `partitions` read at once.
fn following(partitions: usize) -> rdlt_engine::EngineConfigBuilder {
    EngineConfig::builder()
        .lanes(2)
        .barrier_wait(Duration::from_millis(100))
        .replan(Duration::from_millis(200))
        .partitions(partitions)
}

#[tokio::test(start_paused = true)]
async fn a_following_run_of_more_unbounded_partitions_than_its_source_carries_is_refused_at_once() {
    // Four calls: the engine's other call, a slot for every other read, and two followed reads.
    let source = log("carried_beyond", 3, calls(4)).await;
    let plan = pipeline(
        "carried_beyond",
        [stream("events").read(ReadMode::Incremental)],
    )
    .with_until(Until::For(Duration::from_secs(60)));
    let started = tokio::time::Instant::now();
    let outcome = engine(following(8))
        .run(plan, source, memory("carried_beyond").await)
        .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Config, Some(codes::SOURCE_CALLS_EXCEEDED))
    );
    assert!(!error.is_retryable());
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn a_following_run_reads_as_many_unbounded_partitions_as_its_source_leaves_room_for() {
    let source = log("carried_within", 2, calls(4)).await;
    let plan = pipeline(
        "carried_within",
        [stream("events").read(ReadMode::Incremental)],
    )
    .with_until(Until::For(Duration::from_secs(1)));
    let outcome = engine(following(8))
        .run(plan, source, memory("carried_within").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test(start_paused = true)]
async fn a_run_holds_no_more_writers_open_than_its_destination_carries_beside_its_other_call() {
    let names: Vec<String> = (0..10).map(|table| format!("t{table}")).collect();
    let streams: Vec<(&str, u64, u64, u64)> =
        names.iter().map(|name| (name.as_str(), 1, 1, 1)).collect();
    let source = generator(&streams).await;
    let open = Arc::new(Open::default());
    let destination = Arc::new(CarryingDestination {
        inner: memory("carried_writers").await,
        calls: calls(4),
        open: Arc::clone(&open),
    });
    let growth = GrowthLimits::new(1024, 300).expect("valid limits");
    let plan = pipeline("carried_writers", names.iter().map(|name| stream(name)));
    let outcome = engine(EngineConfig::builder().growth(growth))
        .run(plan, source, destination)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    for name in &names {
        assert_eq!(published_rows("carried_writers", name), 1, "table {name}");
    }
    assert_eq!(open.most.load(Ordering::SeqCst), 3);
}

/// Three tables of a row each, read from a source carrying `source` calls at once into a
/// destination carrying `destination`, both counting what they hold open.
async fn tables(
    store: &str,
    source: usize,
    destination: usize,
) -> (RunOutcome, Arc<Open>, Arc<Open>) {
    let names: Vec<String> = (0..3).map(|table| format!("t{table}")).collect();
    let streams: Vec<(&str, u64, u64, u64)> =
        names.iter().map(|name| (name.as_str(), 1, 1, 1)).collect();
    let (reading, writing) = (Arc::new(Open::default()), Arc::new(Open::default()));
    let source = carrying(
        generator(&streams).await,
        calls(source),
        Arc::clone(&reading),
    );
    let destination = Arc::new(CarryingDestination {
        inner: memory(store).await,
        calls: calls(destination),
        open: Arc::clone(&writing),
    });
    let plan = pipeline(store, names.iter().map(|name| stream(name)));
    let outcome = engine(EngineConfig::builder())
        .run(plan, source, destination)
        .await;
    (outcome, reading, writing)
}

#[tokio::test(start_paused = true)]
async fn a_run_whose_connector_carries_one_call_is_refused_before_it_opens_its_destination() {
    // One call leaves no read or writer beside the other call the engine makes into either side.
    for (source, destination) in [(1, 2), (2, 1)] {
        let store = format!("carried_one_{source}_{destination}");
        let (outcome, _, writing) = tables(&store, source, destination).await;
        let error = outcome.error.expect("the run fails");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some(codes::CALLS_TOO_FEW)),
            "{source} and {destination} calls: {error}"
        );
        assert!(!error.is_retryable());
        assert_eq!(outcome.report.attempted, 1);
        assert_eq!(writing.sessions.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn connectors_carrying_two_calls_take_one_read_and_one_writer_at_a_time() {
    let (outcome, reading, writing) = tables("carried_two", 2, 2).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    for table in ["t0", "t1", "t2"] {
        assert_eq!(published_rows("carried_two", table), 1, "table {table}");
    }
    assert_eq!(reading.most.load(Ordering::SeqCst), 1);
    assert_eq!(writing.most.load(Ordering::SeqCst), 1);
}

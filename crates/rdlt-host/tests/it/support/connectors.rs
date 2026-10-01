//! Connectors that behave as a test needs: one that checkpoints only when asked, one whose check
//! fails, and a memory destination whose commit is slow.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::prelude::*;
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorSpec, Destination, DestinationFactory, DestinationSession,
    DestinationWriter, OpenContext, OpenedSession, destination_factory,
};
use rdlt_connector_reference::MemoryDestination;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How often [`Counted`]'s check has been called.
pub(crate) static CHECKS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Configuration of [`Counted`]: none.
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct CountedConfig {}

/// A source of no streams that counts its checks in [`CHECKS`].
#[derive(Debug)]
pub(crate) struct Counted;

#[source(id = "test.counted")]
impl SourceConnector for Counted {
    type Config = CountedConfig;

    async fn connect(_config: CountedConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self)
    }

    async fn check(&self) -> Result<()> {
        CHECKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new()
    }
}

/// Configuration of [`Ticks`].
#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct TicksConfig {
    /// Rows to read; none means reading until stopped.
    pub(crate) rows: Option<u64>,
    /// Milliseconds between rows.
    #[serde(default)]
    pub(crate) pace_ms: u64,
    /// Whether rows go as Arrow batches, the second half with a column the first lacks.
    #[serde(default)]
    pub(crate) arrow: bool,
    /// Whether the read starts with a warning, a metric, how far behind it is and that its
    /// partitions changed.
    #[serde(default)]
    pub(crate) chatty: bool,
    /// Rows after which the read fails with a data error.
    #[serde(default)]
    pub(crate) fail_after: Option<u64>,
    /// Whether the read, its rows sent, waits for more that never come.
    #[serde(default)]
    pub(crate) idle: bool,
    /// Columns of flags: where not zero, the rows go as one batch of them, see [`flagged`].
    #[serde(default)]
    pub(crate) flags: usize,
}

/// A source of numbered rows in one stream, `ticks`, that checkpoints only when the engine asks.
#[derive(Debug)]
pub(crate) struct Ticks {
    config: TicksConfig,
}

/// Each read of [`Ticks`]: whether its partition never ends, and whether it was asked to follow.
pub(crate) static READS: std::sync::Mutex<Vec<(bool, bool)>> = std::sync::Mutex::new(Vec::new());

/// How many idle reads of [`Ticks`] are running: only those, so other tests' reads in the same
/// process never count.
pub(crate) static IDLE_READING: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Counts an idle read of [`Ticks`] as running until it is dropped.
struct Running;

impl Running {
    fn start() -> Self {
        IDLE_READING.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        IDLE_READING.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// The next ticks each committed report said were committed, in order.
pub(crate) static COMMITTED: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::new(Vec::new());

#[source(id = "test.ticks")]
impl SourceConnector for Ticks {
    type Config = TicksConfig;

    async fn connect(config: TicksConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self { config })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(TickStream)
    }
}

struct TickStream;

/// The next tick to read.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Tick {
    pub(crate) next: u64,
}

/// Rows `0..rows` as one batch: their ids, then `flags` columns of flags, a bit a value.
pub(crate) fn flagged(rows: u64, flags: usize) -> arrow_array::RecordBatch {
    use arrow_array::{ArrayRef, BooleanArray, Int64Array};
    let rows = i64::try_from(rows).expect("ticks fit an i64");
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let flag: ArrayRef = Arc::new(BooleanArray::from_iter(
        (0..rows).map(|row| Some(row % 3 == 0)),
    ));
    let columns = (0..flags).map(|at| (format!("f{at}"), Arc::clone(&flag)));
    arrow_array::RecordBatch::try_from_iter(std::iter::once(("id".to_owned(), ids)).chain(columns))
        .expect("a valid batch")
}

/// Row `id` as a batch; with `extra`, a second column too.
fn tick_batch(id: u64, extra: bool) -> arrow_array::RecordBatch {
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    let id = i64::try_from(id).expect("ticks fit an i64");
    let mut columns: Vec<(&str, ArrayRef)> = vec![("id", Arc::new(Int64Array::from(vec![id])))];
    if extra {
        columns.push((
            "note",
            Arc::new(StringArray::from(vec![format!("tick {id}")])),
        ));
    }
    arrow_array::RecordBatch::try_from_iter(columns).expect("a valid batch")
}

impl ReadStream<Ticks> for TickStream {
    type Cursor = Tick;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("ticks").expect("a valid name"))
            .with_checkpointing(Checkpointing::OnDemand)
    }

    async fn read(
        &self,
        source: &Ticks,
        partition: &Partition,
        cursor: Tick,
        out: &mut Emitter<Tick>,
    ) -> Result<()> {
        let _running = source.config.idle.then(Running::start);
        READS
            .lock()
            .expect("the lock is not poisoned")
            .push((partition.is_unbounded(), out.follows()));
        let config = &source.config;
        if config.chatty {
            out.log(rdlt_connector::LogLevel::Warn, "ticking").await?;
            out.metric("ticks.started", 1.5).await?;
            out.behind(7).await?;
            out.replan().await?;
        }
        if let Some(rows) = config.rows.filter(|_| config.flags > 0) {
            out.batch(flagged(rows, config.flags)).await?;
            return out.checkpoint(&Tick { next: rows }).await;
        }
        let pace = Duration::from_millis(config.pace_ms);
        let mut next = cursor.next;
        while config.rows.is_none_or(|rows| next < rows) {
            if config.fail_after.is_some_and(|after| next >= after) {
                return Err(ConnectorError::data("the ticks broke"));
            }
            if config.arrow {
                let extra = config.rows.is_some_and(|rows| next >= rows / 2);
                out.batch(tick_batch(next, extra)).await?;
            } else {
                out.rows(&[serde_json::json!({ "id": next })]).await?;
            }
            next += 1;
            if out.checkpoint_due() {
                out.checkpoint(&Tick { next }).await?;
            }
            // Paces the read, and yields, so a read without end still lets the engine stop it.
            tokio::time::sleep(pace).await;
        }
        if config.idle {
            std::future::pending::<()>().await;
        }
        out.checkpoint(&Tick { next }).await
    }

    async fn committed(&self, _source: &Ticks, cursors: &[(PartitionId, Tick)]) -> Result<()> {
        let mut committed = COMMITTED.lock().expect("the lock is not poisoned");
        committed.extend(cursors.iter().map(|(_, tick)| tick.next));
        Ok(())
    }
}

/// A source whose check fails as `Auth`, with code `test.denied`.
#[derive(Debug)]
pub(crate) struct Denied;

#[source(id = "test.denied")]
impl SourceConnector for Denied {
    type Config = serde_json::Value;

    async fn connect(_config: serde_json::Value, _context: &ConnectContext) -> Result<Self> {
        Ok(Self)
    }

    async fn check(&self) -> Result<()> {
        Err(
            ConnectorError::new(ConnectorErrorKind::Auth, "the token was refused")
                .with_code("test.denied"),
        )
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new()
    }
}

/// The memory destination, whose commits each take `delay` longer.
pub(crate) struct SlowCommits {
    inner: Box<dyn DestinationFactory>,
    delay: Duration,
}

impl SlowCommits {
    pub(crate) fn factory(delay: Duration) -> Box<dyn DestinationFactory> {
        Box::new(Self {
            inner: destination_factory::<MemoryDestination>(),
            delay,
        })
    }
}

impl DestinationFactory for SlowCommits {
    fn spec(&self) -> &ConnectorSpec {
        self.inner.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let inner = self.inner.connect(config, context).await?;
            Ok(Box::new(Slow {
                inner: Arc::from(inner),
                delay: self.delay,
            }) as Box<dyn Destination>)
        })
    }
}

struct Slow {
    inner: Arc<dyn Destination>,
    delay: Duration,
}

impl Destination for Slow {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.inner.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(SlowSession {
                    inner: opened.session,
                    delay: self.delay,
                }),
                epoch: opened.epoch,
                state: opened.state,
            })
        })
    }
}

struct SlowSession {
    inner: Box<dyn DestinationSession>,
    delay: Duration,
}

impl DestinationSession for SlowSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        self.inner.writer(table)
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.inner.commit(meta).await
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}

/// How a [`Writes`] destination's writers go wrong.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Writing {
    /// Each write fails with a data error.
    Fails,
    /// No write ever finishes.
    Stalls,
    /// Each write panics.
    Panics,
    /// Each write is kept where the test that asked for it looks.
    Keeps(&'static Kept),
}

/// The writes a [`Writes`] destination that keeps them was given, in order, each with its segment.
pub(crate) type Kept = std::sync::Mutex<Vec<(u64, arrow_array::RecordBatch)>>;

/// The memory destination, whose writers go wrong as `writing` says.
pub(crate) struct Writes {
    inner: Box<dyn DestinationFactory>,
    writing: Writing,
}

impl Writes {
    pub(crate) fn factory(writing: Writing) -> Box<dyn DestinationFactory> {
        Box::new(Self {
            inner: destination_factory::<MemoryDestination>(),
            writing,
        })
    }
}

impl DestinationFactory for Writes {
    fn spec(&self) -> &ConnectorSpec {
        self.inner.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        Box::pin(async move {
            let inner = self.inner.connect(config, context).await?;
            Ok(Box::new(Wrong {
                inner: Arc::from(inner),
                writing: self.writing,
            }) as Box<dyn Destination>)
        })
    }
}

struct Wrong {
    inner: Arc<dyn Destination>,
    writing: Writing,
}

impl Destination for Wrong {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.inner.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(WrongSession {
                    inner: opened.session,
                    writing: self.writing,
                }),
                epoch: opened.epoch,
                state: opened.state,
            })
        })
    }
}

struct WrongSession {
    inner: Box<dyn DestinationSession>,
    writing: Writing,
}

impl DestinationSession for WrongSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        _table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        let writing = self.writing;
        Box::pin(async move { Ok(Box::new(WrongWriter(writing)) as Box<dyn DestinationWriter>) })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        self.inner.commit(meta)
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}

struct WrongWriter(Writing);

impl DestinationWriter for WrongWriter {
    fn write(
        &mut self,
        segment: rdlt_connector::SegmentId,
        batch: arrow_array::RecordBatch,
    ) -> BoxFuture<'_, Result<()>> {
        let writing = self.0;
        Box::pin(async move {
            match writing {
                Writing::Fails => Err(ConnectorError::data("the write was refused")),
                Writing::Stalls => std::future::pending().await,
                Writing::Panics => panic!("the writer panicked"),
                Writing::Keeps(kept) => {
                    let mut kept = kept.lock().expect("the lock is not poisoned");
                    kept.push((segment.0, batch));
                    Ok(())
                }
            }
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async { Ok(WriteStats::default()) })
    }
}

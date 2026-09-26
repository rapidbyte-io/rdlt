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
    /// Whether the read starts with a warning and a metric.
    #[serde(default)]
    pub(crate) chatty: bool,
    /// Rows after which the read fails with a data error.
    #[serde(default)]
    pub(crate) fail_after: Option<u64>,
}

/// A source of numbered rows in one stream, `ticks`, that checkpoints only when the engine asks.
#[derive(Debug)]
pub(crate) struct Ticks {
    config: TicksConfig,
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
        _partition: &Partition,
        cursor: Tick,
        out: &mut Emitter<Tick>,
    ) -> Result<()> {
        let config = &source.config;
        if config.chatty {
            out.log(rdlt_connector::LogLevel::Warn, "ticking").await?;
            out.metric("ticks.started", 1.5).await?;
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
}

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
        _segment: rdlt_connector::SegmentId,
        _batch: arrow_array::RecordBatch,
    ) -> BoxFuture<'_, Result<()>> {
        let writing = self.0;
        Box::pin(async move {
            match writing {
                Writing::Fails => Err(ConnectorError::data("the write was refused")),
                Writing::Stalls => std::future::pending().await,
            }
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async { Ok(WriteStats::default()) })
    }
}

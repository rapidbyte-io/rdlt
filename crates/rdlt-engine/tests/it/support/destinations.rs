//! Destinations for tests: one that discards what it stages, one that hides a capability, and one
//! that fails at a chosen step.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectContext, ConnectorError, ConnectorErrorKind,
    Destination, DestinationConnector, DestinationSession, DestinationWriter, OpenContext, Opened,
    OpenedSession, Receipt, Result, SegmentId, Session, TableChange, TableRef, TableWriter,
    WriteStats, destination_factory,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize, JsonSchema)]
struct NullConfig {}

/// Counts what it stages, slowly, and publishes nothing, so tests can measure the engine alone.
struct Null;

/// A destination that discards every batch.
pub(crate) async fn null() -> Arc<dyn Destination> {
    let destination = destination_factory::<Null>()
        .connect(json!({}), ConnectContext::new())
        .await
        .expect("the null destination connects");
    Arc::from(destination)
}

impl DestinationConnector for Null {
    const ID: &'static str = "io.test.null";
    const VERSION: &'static str = "0.0.0";
    type Config = NullConfig;
    type Session = NullSession;

    fn capabilities(&self) -> Capabilities {
        Capabilities::minimal()
    }

    async fn connect(_config: NullConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self)
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, _context: &OpenContext) -> Result<Opened<NullSession>> {
        Ok(Opened {
            session: NullSession::default(),
            epoch: rdlt_connector::Epoch(1),
            state: Vec::new(),
        })
    }
}

#[derive(Default)]
struct NullSession {
    staged: Arc<parking_lot::Mutex<BTreeMap<SegmentId, u64>>>,
}

impl Session for NullSession {
    type Writer = NullWriter;

    async fn apply_schema(&mut self, _change: &TableChange) -> Result<()> {
        Ok(())
    }

    async fn writer(&mut self, _table: &TableRef) -> Result<NullWriter> {
        Ok(NullWriter(Arc::clone(&self.staged)))
    }

    async fn discard_staged(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let mut staged = self.staged.lock();
        let rows = meta
            .segments
            .iter()
            .filter_map(|segment| staged.remove(&segment))
            .sum();
        Ok(Receipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
            committed_at: UNIX_EPOCH,
            rows,
            bytes: 0,
        })
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

struct NullWriter(Arc<parking_lot::Mutex<BTreeMap<SegmentId, u64>>>);

impl TableWriter for NullWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        // A slow writer, so batches would pile up in memory if nothing held the source back.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        *self.0.lock().entry(segment).or_default() += batch.num_rows() as u64;
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        Ok(WriteStats::default())
    }
}

/// `inner` with the capabilities `limit` leaves.
pub(crate) fn limited(
    inner: Arc<dyn Destination>,
    limit: impl FnOnce(&mut Capabilities),
) -> Arc<dyn Destination> {
    let mut capabilities = inner.capabilities().clone();
    limit(&mut capabilities);
    Arc::new(Limited {
        inner,
        capabilities,
    })
}

struct Limited {
    inner: Arc<dyn Destination>,
    capabilities: Capabilities,
}

impl Destination for Limited {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        self.inner.open(context)
    }
}

/// Where [`failing`] makes a destination fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    Open,
    CreateTable,
    Writer,
    Commit,
    /// The first commit lands, but its response is lost.
    LoseResponse,
    /// The first commit lands but its response is lost, and the open after it fails.
    LoseResponseThenOpen,
}

/// `inner`, failing with a transient error at `step`.
pub(crate) fn failing(inner: Arc<dyn Destination>, step: Step) -> Arc<dyn Destination> {
    Arc::new(Failing {
        inner,
        step,
        lost: Arc::new(AtomicBool::new(false)),
        refused: AtomicBool::new(false),
    })
}

struct Failing {
    inner: Arc<dyn Destination>,
    step: Step,
    /// Whether a response was already lost, for [`Step::LoseResponse`].
    lost: Arc<AtomicBool>,
    /// Whether an open after the lost response was already refused, for
    /// [`Step::LoseResponseThenOpen`].
    refused: AtomicBool,
}

fn injected() -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Transient, "injected")
}

impl Destination for Failing {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let refuse = self.step == Step::LoseResponseThenOpen
                && self.lost.load(Ordering::SeqCst)
                && !self.refused.swap(true, Ordering::SeqCst);
            if self.step == Step::Open || refuse {
                return Err(injected());
            }
            let opened = self.inner.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(FailingSession {
                    inner: opened.session,
                    step: self.step,
                    lost: Arc::clone(&self.lost),
                }),
                ..opened
            })
        })
    }
}

struct FailingSession {
    inner: Box<dyn DestinationSession>,
    step: Step,
    lost: Arc<AtomicBool>,
}

impl DestinationSession for FailingSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        if self.step == Step::CreateTable {
            return Box::pin(async { Err(injected()) });
        }
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        if self.step == Step::Writer {
            return Box::pin(async { Err(injected()) });
        }
        self.inner.writer(table)
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        if self.step == Step::Commit {
            return Box::pin(async { Err(injected()) });
        }
        Box::pin(async move {
            let receipt = self.inner.commit(meta).await?;
            let loses = matches!(self.step, Step::LoseResponse | Step::LoseResponseThenOpen);
            if loses && !self.lost.swap(true, Ordering::SeqCst) {
                return Err(injected());
            }
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}

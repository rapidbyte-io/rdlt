//! Destinations for tests: one that discards what it stages, one that hides a capability, one
//! that fails at a chosen step, one whose commits wait until a test lets them go, one whose
//! writes never return, one that counts the sessions it opens and closes and the writers they
//! create, and one that answers an open with state it rewrote.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
struct NullConfig {
    /// Whether writers keep what they stage until they flush, as SQL and file writers do.
    #[serde(default)]
    buffers: bool,
    /// Whether no write ever returns.
    #[serde(default)]
    stalls: bool,
}

/// Counts what it stages, slowly, and publishes nothing, so tests can measure the engine alone.
struct Null {
    buffers: bool,
    stalls: bool,
    /// The epoch the last open set: each open sets a newer one, as every destination does.
    epoch: std::sync::atomic::AtomicU64,
}

/// A destination that discards every batch.
pub(crate) async fn null() -> Arc<dyn Destination> {
    null_with(json!({})).await
}

/// A destination whose writers keep every batch until they flush, then discard it.
pub(crate) async fn buffering() -> Arc<dyn Destination> {
    null_with(json!({ "buffers": true })).await
}

/// A destination none of whose writes ever returns.
pub(crate) async fn stalling() -> Arc<dyn Destination> {
    null_with(json!({ "stalls": true })).await
}

async fn null_with(config: serde_json::Value) -> Arc<dyn Destination> {
    let destination = destination_factory::<Null>()
        .connect(config, ConnectContext::new())
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

    async fn connect(config: NullConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            buffers: config.buffers,
            stalls: config.stalls,
            epoch: std::sync::atomic::AtomicU64::new(0),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, _context: &OpenContext) -> Result<Opened<NullSession>> {
        Ok(Opened {
            session: NullSession {
                staged: Arc::default(),
                buffers: self.buffers,
                stalls: self.stalls,
            },
            epoch: rdlt_connector::Epoch(
                self.epoch.fetch_add(1, Ordering::SeqCst).saturating_add(1),
            ),
            state: Vec::new(),
        })
    }
}

struct NullSession {
    staged: Arc<parking_lot::Mutex<BTreeMap<SegmentId, u64>>>,
    buffers: bool,
    stalls: bool,
}

impl Session for NullSession {
    type Writer = NullWriter;

    async fn apply_schema(&mut self, _change: &TableChange) -> Result<()> {
        Ok(())
    }

    async fn writer(&mut self, _table: &TableRef) -> Result<NullWriter> {
        Ok(NullWriter {
            staged: Arc::clone(&self.staged),
            buffered: self.buffers.then(Vec::new),
            stalls: self.stalls,
        })
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

struct NullWriter {
    staged: Arc<parking_lot::Mutex<BTreeMap<SegmentId, u64>>>,
    /// What the writer keeps until it flushes, when it buffers.
    buffered: Option<Vec<RecordBatch>>,
    stalls: bool,
}

impl TableWriter for NullWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        if self.stalls {
            std::future::pending::<()>().await;
        }
        // A slow writer, so batches would pile up in memory if nothing held the source back.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        *self.staged.lock().entry(segment).or_default() += batch.num_rows() as u64;
        if let Some(buffered) = &mut self.buffered {
            buffered.push(batch);
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        if let Some(buffered) = &mut self.buffered {
            buffered.clear();
        }
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
    /// Opening panics, as a connector with a bug may.
    PanicOnOpen,
    /// The first commit fails before it lands; the others land.
    CommitOnce,
    /// Every commit lands, and is answered with the receipt of the commit after it.
    SkewReceipts,
    /// Every commit lands, and its receipt counts as many rows as a total holds.
    InflateReceipts,
    /// Every write and flush waits for ever.
    StallWrites,
    /// Every open answers with a state record that is not one.
    GarbleState,
    /// Every commit fails, and no close of a session ever returns.
    StallClose,
}

/// `inner`, failing with a transient error at `step`, or panicking there.
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
            assert!(self.step != Step::PanicOnOpen, "injected panic");
            if self.step == Step::Open || refuse {
                return Err(injected());
            }
            let mut opened = self.inner.open(context).await?;
            if self.step == Step::GarbleState {
                opened.state.push(rdlt_connector::StateRecord {
                    key: "garbled".to_owned(),
                    value: bytes::Bytes::from_static(b"garbled"),
                });
            }
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
        if self.step == Step::StallWrites {
            return Box::pin(async { Ok(Box::new(Stalled) as Box<dyn DestinationWriter>) });
        }
        self.inner.writer(table)
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        let once = self.step == Step::CommitOnce && !self.lost.swap(true, Ordering::SeqCst);
        if matches!(self.step, Step::Commit | Step::StallClose) || once {
            return Box::pin(async { Err(injected()) });
        }
        Box::pin(async move {
            let receipt = self.inner.commit(meta).await?;
            let loses = matches!(self.step, Step::LoseResponse | Step::LoseResponseThenOpen);
            if loses && !self.lost.swap(true, Ordering::SeqCst) {
                return Err(injected());
            }
            Ok(match self.step {
                Step::SkewReceipts => Receipt {
                    commit_seq: receipt.commit_seq.next(),
                    ..receipt
                },
                Step::InflateReceipts => Receipt {
                    rows: u64::MAX,
                    ..receipt
                },
                _ => receipt,
            })
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        if self.step == Step::StallClose {
            return Box::pin(std::future::pending());
        }
        self.inner.close()
    }
}

/// What holds a [`gated`] destination's commits: how many have started, and the permits they
/// wait for.
#[derive(Debug)]
pub(crate) struct Gate {
    /// Commits that reached the destination.
    pub(crate) started: AtomicUsize,
    release: tokio::sync::Semaphore,
}

impl Gate {
    /// A gate no commit passes yet.
    pub(crate) fn closed() -> Arc<Self> {
        Arc::new(Self {
            started: AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
        })
    }

    /// Lets every commit through, those waiting and those to come.
    pub(crate) fn open(&self) {
        self.release
            .add_permits(tokio::sync::Semaphore::MAX_PERMITS / 2);
    }
}

/// `inner`, each of whose commits waits at `gate` before it reaches `inner`.
pub(crate) fn gated(inner: Arc<dyn Destination>, gate: Arc<Gate>) -> Arc<dyn Destination> {
    Arc::new(Gated { inner, gate })
}

struct Gated {
    inner: Arc<dyn Destination>,
    gate: Arc<Gate>,
}

impl Destination for Gated {
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
                session: Box::new(GatedSession {
                    inner: opened.session,
                    gate: Arc::clone(&self.gate),
                }),
                ..opened
            })
        })
    }
}

struct GatedSession {
    inner: Box<dyn DestinationSession>,
    gate: Arc<Gate>,
}

impl DestinationSession for GatedSession {
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
            self.gate.started.fetch_add(1, Ordering::SeqCst);
            let permit = self.gate.release.acquire().await;
            drop(permit);
            self.inner.commit(meta).await
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}

/// A writer whose writes and flushes never return.
struct Stalled;

impl DestinationWriter for Stalled {
    fn write(&mut self, _segment: SegmentId, _batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(std::future::pending())
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(std::future::pending())
    }
}

/// How many sessions a [`counting`] destination opened and how many it closed, and how many
/// writers its sessions created.
#[derive(Debug, Default)]
pub(crate) struct Sessions {
    pub(crate) opened: AtomicUsize,
    pub(crate) closed: AtomicUsize,
    pub(crate) writers: AtomicUsize,
}

/// `inner`, counting in the [`Sessions`] returned the sessions it opens and closes.
pub(crate) fn counting(inner: Arc<dyn Destination>) -> (Arc<dyn Destination>, Arc<Sessions>) {
    let sessions = Arc::new(Sessions::default());
    let destination = Arc::new(Counting {
        inner,
        sessions: Arc::clone(&sessions),
    });
    (destination, sessions)
}

struct Counting {
    inner: Arc<dyn Destination>,
    sessions: Arc<Sessions>,
}

impl Destination for Counting {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.inner.open(context).await?;
            self.sessions.opened.fetch_add(1, Ordering::SeqCst);
            Ok(OpenedSession {
                session: Box::new(CountedSession {
                    inner: opened.session,
                    sessions: Arc::clone(&self.sessions),
                }),
                ..opened
            })
        })
    }
}

struct CountedSession {
    inner: Box<dyn DestinationSession>,
    sessions: Arc<Sessions>,
}

impl DestinationSession for CountedSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let writer = self.inner.writer(table).await?;
            self.sessions.writers.fetch_add(1, Ordering::SeqCst);
            Ok(writer)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        self.inner.commit(meta)
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.sessions.closed.fetch_add(1, Ordering::SeqCst);
        self.inner.close()
    }
}

/// How a [`restated`] destination rewrites the state records an open answers with.
pub(crate) type Restate = fn(&mut Vec<rdlt_connector::StateRecord>);

/// `inner`, answering every open with the state records `restate` rewrote.
pub(crate) fn restated(inner: Arc<dyn Destination>, restate: Restate) -> Arc<dyn Destination> {
    Arc::new(Restated { inner, restate })
}

struct Restated {
    inner: Arc<dyn Destination>,
    restate: Restate,
}

impl Destination for Restated {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let mut opened = self.inner.open(context).await?;
            (self.restate)(&mut opened.state);
            Ok(opened)
        })
    }
}

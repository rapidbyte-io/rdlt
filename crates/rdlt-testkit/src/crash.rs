//! Crash-injection tools: named fault points that kill a run at a
//! precise stage. A "crash" in tests = the run aborts; durable state (the shared
//! memory destination and the on-disk WAL) survives into the next run — exactly the
//! process-death model, minus the process.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, DestCapabilities, DestError, Destination,
    LoadSession, OpenCtx, PipelineId, RecordBatch, StateDoc, TableName, TableSchema, WriteMode,
};

/// Where to inject the fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// Fail the Nth `write` call (1-based) before the batch reaches the inner
    /// destination. Models a crash where the WAL has recorded the batch but the
    /// destination never received it; recovery must replay it from the WAL.
    BeforeWrite(u64),
    /// Fail the Nth `commit` call before the inner destination commits. The
    /// earlier `write` calls have all reached the inner destination, so this
    /// models a crash where the batches are staged but not yet published;
    /// recovery must re-drive the commit.
    BeforeCommit(u64),
    /// Let the Nth `commit` succeed inside, then fail — the receipt is lost.
    /// Models a crash after the destination published but before the caller
    /// learned; recovery must hit idempotence to avoid double-publishing.
    AfterCommit(u64),
}

/// Wraps any destination and fails exactly once at the configured fault point.
/// Clone shares the trigger, so "restart" = build a new engine over the same wrapper
/// (already-fired faults don't fire again).
#[derive(Debug, Clone)]
pub struct FlakyDestination<D> {
    inner: D,
    fault: FaultPoint,
    writes: Arc<AtomicU64>,
    commits: Arc<AtomicU64>,
    fired: Arc<AtomicU64>,
}

impl<D> FlakyDestination<D> {
    pub fn new(inner: D, fault: FaultPoint) -> Self {
        Self {
            inner,
            fault,
            writes: Arc::new(AtomicU64::new(0)),
            commits: Arc::new(AtomicU64::new(0)),
            fired: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many times the fault actually fired (each fault fires at most once).
    pub fn fired(&self) -> u64 {
        self.fired.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl<D: Destination + Clone> Destination for FlakyDestination<D> {
    fn spec(&self) -> ConnectorSpec {
        self.inner.spec()
    }

    fn capabilities(&self) -> DestCapabilities {
        self.inner.capabilities()
    }

    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError> {
        let session = self.inner.open(ctx).await?;
        Ok(Box::new(FlakySession {
            inner: session,
            fault: self.fault,
            writes: Arc::clone(&self.writes),
            commits: Arc::clone(&self.commits),
            fired: Arc::clone(&self.fired),
        }))
    }
}

struct FlakySession {
    inner: Box<dyn LoadSession>,
    fault: FaultPoint,
    writes: Arc<AtomicU64>,
    commits: Arc<AtomicU64>,
    fired: Arc<AtomicU64>,
}

impl FlakySession {
    fn crash(&self, what: &str) -> DestError {
        self.fired.fetch_add(1, Ordering::SeqCst);
        DestError::fatal(format!("injected crash: {what}"))
    }
}

#[async_trait]
impl LoadSession for FlakySession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestError> {
        self.inner.ensure_table(schema, mode).await
    }

    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if let FaultPoint::BeforeWrite(at) = self.fault
            && n == at
            && self.fired.load(Ordering::SeqCst) == 0
        {
            return Err(self.crash("before write"));
        }
        self.inner.write(table, batch).await
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError> {
        let n = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        let unfired = self.fired.load(Ordering::SeqCst) == 0;
        match self.fault {
            FaultPoint::BeforeCommit(at) if n == at && unfired => Err(self.crash("before commit")),
            FaultPoint::AfterCommit(at) if n == at && unfired => {
                // The inner commit SUCCEEDS; the caller never learns.
                self.inner.commit(meta).await?;
                Err(self.crash("after commit (receipt lost)"))
            }
            _ => self.inner.commit(meta).await,
        }
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        self.inner.read_state(pipeline).await
    }
}

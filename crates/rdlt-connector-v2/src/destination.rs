//! The destination half of the SPI: [`Destination`], its per-run
//! [`LoadSession`], and the context a session opens under.

use async_trait::async_trait;

use crate::RecordBatch;
use crate::capabilities::DestinationCapabilities;
use crate::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use crate::error::DestinationError;
use crate::spec::ConnectorSpec;

/// A data destination: capability declaration + session factory.
#[async_trait]
pub trait Destination: Send + Sync + 'static {
    /// Name, version, config JSON-schema.
    fn spec(&self) -> ConnectorSpec;

    /// A cheap connectivity probe: can this destination reach what it
    /// writes to, with the credentials it holds?
    ///
    /// Distinct from [`Destination::open`] on purpose — an operator wants
    /// "are the credentials right" answered in seconds, without staging
    /// anything. Failures classify exactly as a session would classify
    /// them.
    ///
    /// The default body returns `Ok(())` WITHOUT probing anything, so a
    /// destination that has not implemented it reports success trivially —
    /// a host needing a real answer must know which connectors implement
    /// the probe. Implementations replace it wholesale.
    async fn check(&self) -> Result<(), DestinationError> {
        Ok(())
    }

    /// Truthful capability declaration — the host plans from this and
    /// does not re-verify at runtime.
    fn capabilities(&self) -> DestinationCapabilities;

    /// Open a load session. MUST make uncommitted staged data from any
    /// previous (crashed) session invisible and reclaimable.
    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError>;
}

/// What a load session opens under.
///
/// Deliberately an exhaustive pub-field struct (a DX choice, semver
/// gated); [`OpenContext::new`] is the compatibility hedge, and every
/// consumer in the tree constructs through it.
#[derive(Debug, Clone)]
pub struct OpenContext {
    /// The pipeline whose state this session reads and republishes — the
    /// key `StateDoc` persists under.
    pub pipeline: PipelineId,
    /// THIS run's identity. Half of the `(load_id, commit_seq)` pair that
    /// makes a re-commit idempotent, and what distinguishes this
    /// session's staging from a crashed predecessor's.
    pub load_id: LoadId,
}

impl OpenContext {
    /// Build a context. Present so a later field addition is not a
    /// breaking change for callers constructing through here rather than
    /// with a struct literal.
    pub fn new(pipeline: PipelineId, load_id: LoadId) -> Self {
        Self { pipeline, load_id }
    }
}

/// One destination load session. Writes are staged and invisible until
/// `commit`; publication is atomic with pipeline state and idempotent per
/// `(load_id, commit_seq)`.
#[async_trait]
pub trait LoadSession: Send {
    /// Create or migrate the physical table for this schema version, and
    /// record the table's write disposition (`mode` is the root stream's
    /// mode for child tables — merge needs it at commit time).
    /// Idempotent; always precedes the first write at each schema
    /// version.
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError>;

    /// Write one batch. The host guarantees `batch` conforms exactly to
    /// the last ensured schema for `table` and that per-table batches
    /// arrive in order. Delivery is at-least-once — safe because staged
    /// writes stay invisible until commit and a crashed session's staging
    /// is reclaimed on the next open.
    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError>;

    /// Atomically publish all writes since the last commit AND persist
    /// `meta.state`. Re-committing the same `(load_id, commit_seq)`
    /// returns the prior receipt without re-publishing.
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError>;

    /// Recover the state persisted by the latest successful commit, or
    /// `None` for a fresh pipeline.
    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError>;
}

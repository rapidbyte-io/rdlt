//! The destination half of the SPI: [`Destination`], its per-run
//! [`LoadSession`], and the context a session opens under.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
///
/// constructed via `new` — wire-era fields arrive without breaking
/// out-of-crate construction (038)
#[derive(Clone)]
#[non_exhaustive]
pub struct OpenContext {
    /// The pipeline whose state this session reads and republishes — the
    /// key `StateDoc` persists under.
    pub pipeline: PipelineId,
    /// THIS run's identity. Half of the `(load_id, commit_seq)` pair that
    /// makes a re-commit idempotent, and what distinguishes this
    /// session's staging from a crashed predecessor's.
    pub load_id: LoadId,
    /// Where a file-writing destination reports each output part it
    /// closes. ADVISORY, like every telemetry signal: absent means
    /// nobody is listening and the destination must behave identically.
    /// SQL destinations never call it.
    pub part_events: Option<PartEventFn>,
}

impl OpenContext {
    /// Build a context. Present so a later field addition is not a
    /// breaking change for callers constructing through here rather than
    /// with a struct literal.
    pub fn new(pipeline: PipelineId, load_id: LoadId) -> Self {
        Self {
            pipeline,
            load_id,
            part_events: None,
        }
    }

    /// Attach a part-event listener.
    #[must_use]
    pub fn with_part_events(mut self, listener: PartEventFn) -> Self {
        self.part_events = Some(listener);
        self
    }

    /// Report a closed part to the listener, if any. Connectors call
    /// this at the moment a part's bytes are final; it must never fail
    /// and never block.
    pub fn notify_part_closed(&self, event: PartClosed) {
        if let Some(listener) = &self.part_events {
            listener(event);
        }
    }
}

impl std::fmt::Debug for OpenContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The listener is a function with no useful rendering; whether
        // one is attached is the fact worth printing.
        f.debug_struct("OpenContext")
            .field("pipeline", &self.pipeline)
            .field("load_id", &self.load_id)
            .field("part_events", &self.part_events.is_some())
            .finish()
    }
}

/// A closed-part report: the file-writing destinations' telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PartClosed {
    /// The table the part belongs to.
    pub table: TableName,
    /// The part's encoded, on-the-wire size.
    pub encoded_bytes: u64,
    /// What closed it.
    pub reason: PartCloseReason,
}

impl PartClosed {
    /// Build a report. The hedge for later field additions, like
    /// [`OpenContext::new`].
    pub fn new(table: TableName, encoded_bytes: u64, reason: PartCloseReason) -> Self {
        Self {
            table,
            encoded_bytes,
            reason,
        }
    }
}

/// Why a part closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PartCloseReason {
    /// It reached its size target.
    Target,
    /// It was open longer than the configured time bound.
    Time,
    /// The memory ceiling across open parts closed it early.
    Budget,
    /// A commit closed it — no part spans a commit.
    Commit,
    /// A schema change closed it — a file holds one schema.
    Schema,
}

/// The listener shape: a plain callback, so the SPI carries no channel
/// or runtime dependency for what is a fire-and-forget signal. `Arc`d
/// because sessions and contexts are moved around freely.
pub type PartEventFn = std::sync::Arc<dyn Fn(PartClosed) + Send + Sync>;

/// One destination load session. Writes are staged and invisible until
/// `commit`; publication is atomic with pipeline state and idempotent per
/// `(load_id, commit_seq)`.
///
/// D-038-2: Send-only deliberately: a remote client owns each session in
/// one task; no speculative Sync (038).
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

    /// Called exactly once whenever the session ends. After the run's
    /// last successful commit its error PROPAGATES (a cleanup failure
    /// is a real destination error); on failure/cancellation paths the
    /// engine invokes it best-effort and ignores its error — the
    /// session must tolerate being closed after arbitrary partial
    /// work (037 US2 T7 fix round 2, I1). Default: nothing to do.
    async fn close(&mut self) -> Result<(), DestinationError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Closeless;

    #[async_trait]
    impl LoadSession for Closeless {
        async fn ensure_table(
            &mut self,
            _schema: &TableSchema,
            _mode: &WriteMode,
        ) -> Result<(), DestinationError> {
            Ok(())
        }
        async fn write(
            &mut self,
            _table: &TableName,
            _batch: RecordBatch,
        ) -> Result<(), DestinationError> {
            Ok(())
        }
        async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
            Ok(CommitReceipt {
                load_id: meta.load_id,
                commit_seq: meta.commit_seq,
            })
        }
        async fn read_state(
            &mut self,
            _pipeline: &PipelineId,
        ) -> Result<Option<StateDoc>, DestinationError> {
            Ok(None)
        }
    }

    /// The default `close` is a trivial success BY CONTRACT (037 US2
    /// T7 fix round 1): a session with nothing to release on close need
    /// not implement it at all.
    #[tokio::test]
    async fn the_default_close_reports_success_without_doing_anything() {
        assert!(Closeless.close().await.is_ok());
    }

    #[test]
    fn part_events_serialize_with_the_core_twin_spelling() {
        let event = PartClosed::new(TableName::new("t"), 512, PartCloseReason::Budget);
        let wire = serde_json::to_value(&event).expect("serializes");
        assert_eq!(
            wire["reason"], "budget",
            "snake_case, matching rdlt_core::PartClose"
        );
        let back: PartClosed = serde_json::from_value(wire).expect("round-trips");
        assert_eq!(back, event);
    }
}

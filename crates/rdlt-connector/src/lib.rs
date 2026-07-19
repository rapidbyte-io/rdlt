//! # rdlt-connector — the in-process connector SPI
//!
//! `Source`/`Destination`/`LoadSession` traits and their adjuncts. Behavioral contract:
//! `specs/001-rdlt-ingestion-engine/contracts/connector-spi.md` — every clause there is
//! enforced by the public conformance suites in `rdlt-testkit`.
//!
//! Vocabulary types come from [`rdlt_core`] (re-exported as [`core`]); this crate adds
//! the traits. Traits are object-safe and all exchange types serde-serializable, so a
//! future process/WASM host can adapt this SPI over a wire without engine changes.
//!
//! SEMVER-SACRED: gated by `cargo semver-checks` in CI.

pub mod capabilities;
pub mod error;
pub mod spec;
pub mod stream;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};

pub use arrow_array as arrow;
pub use arrow_array::RecordBatch;
pub use capabilities::DestCapabilities;
pub use error::{BoxError, DestError, SourceError};
/// The rdlt vocabulary. Connectors MUST take these types from here (single-version
/// identity across the workspace).
pub use rdlt_core as core;
pub use rdlt_core::{
    CommitMeta, CommitReceipt, Cursor, LoadId, PipelineId, StateDoc, TableName, TableSchema,
    WriteMode,
};
pub use spec::ConnectorSpec;
pub use stream::StreamSpec;

/// A data source: declares streams, then pushes records for one stream per
/// [`Source::read`] call.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Name, version, config JSON-schema.
    fn spec(&self) -> ConnectorSpec;

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError>;

    /// Read ONE stream, pushing through `req.out`. The engine schedules streams
    /// concurrently. Given `req.since`, the source MUST resume — never re-emit rows
    /// already covered by that cursor (clause S1). Awaiting the push IS the flow
    /// control (clause S5); treat [`ChannelClosed`] as cancellation (clause S4).
    async fn read(&self, req: ReadRequest) -> Result<(), SourceError>;
}

/// Everything a source needs to read one stream.
///
/// Deliberately an exhaustive pub-field struct (DX choice, semver-gated); [`ReadRequest::new`]
/// exists as the compatibility hedge.
#[derive(Debug)]
pub struct ReadRequest {
    pub stream: StreamSpec,
    /// Last committed cursor; only ever a cursor the destination previously committed
    /// (clause E6).
    pub since: Option<Cursor>,
    /// Push handle.
    pub out: RecordsOut,
}

impl ReadRequest {
    pub fn new(stream: StreamSpec, since: Option<Cursor>, out: RecordsOut) -> Self {
        Self { stream, since, out }
    }
}

/// A data destination: capability declaration + session factory.
#[async_trait]
pub trait Destination: Send + Sync + 'static {
    fn spec(&self) -> ConnectorSpec;

    /// Truthful capability declaration — the engine plans from this and will not
    /// re-verify at runtime (clause D7).
    fn capabilities(&self) -> DestCapabilities;

    /// Open a load session. MUST make uncommitted staged data from any previous
    /// (crashed) session invisible and reclaimable (clause D4).
    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError>;
}

/// Session context. Exhaustive pub-field struct; [`OpenCtx::new`] as hedge.
#[derive(Debug, Clone)]
pub struct OpenCtx {
    pub pipeline: PipelineId,
    pub load_id: LoadId,
}

impl OpenCtx {
    pub fn new(pipeline: PipelineId, load_id: LoadId) -> Self {
        Self { pipeline, load_id }
    }
}

/// One destination load session. Writes are staged and invisible until `commit`
/// (clause D1); publication is atomic with state (D2) and idempotent per
/// `(load_id, commit_seq)` (D3).
#[async_trait]
pub trait LoadSession: Send {
    /// Create or migrate the physical table for this schema version, and record the
    /// table's write disposition (`mode` is the root stream's mode for child tables —
    /// merge needs it at commit time). Idempotent; always precedes the first write at
    /// each schema version (clause E1/D5).
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestError>;

    /// Write one batch. The engine guarantees `batch` conforms exactly to the last
    /// ensured schema for `table` (clause E4) and that per-table batches arrive in
    /// order (E1). Delivery is at-least-once (E3) — safe because of D1/D4.
    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError>;

    /// Atomically publish all writes since the last commit AND persist `meta.state`.
    /// Re-committing the same `(load_id, commit_seq)` returns the prior receipt
    /// without re-publishing (clause D3).
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError>;

    /// Recover the state persisted by the latest successful commit, or `None` for a
    /// fresh pipeline (clause D6).
    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError>;
}

/// The stream was closed by the host (cancellation or failure downstream). Sources
/// should return promptly without escalating (clause S4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("records channel closed by host")]
pub struct ChannelClosed;

/// What a source pushed. Obtained by hosts from [`RecordsIn::recv`]; carries its
/// byte-budget permit, released when the message is dropped.
#[derive(Debug)]
pub struct SourcePush {
    pub payload: PushPayload,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[derive(Debug)]
pub enum PushPayload {
    /// Raw JSON bytes: one document, an array of documents, or NDJSON. The engine's
    /// shredder parses these directly into Arrow builders. `rows()` also lands here
    /// (canonically serialized) so hosts have exactly one JSON ingest path.
    RawJson(Bytes),
    /// A source-native Arrow batch; bypasses the shredder (schema check only).
    Arrow(RecordBatch),
    /// "All rows pushed so far are complete up to this cursor."
    Checkpoint(Cursor),
}

/// Push handle held by sources. Awaiting a push is the flow control: the byte budget
/// (bounded in BYTES, not messages) is what a slow destination propagates back to the
/// source.
#[derive(Debug)]
pub struct RecordsOut {
    tx: mpsc::Sender<SourcePush>,
    budget: Arc<Semaphore>,
    budget_max: usize,
}

impl RecordsOut {
    /// Perf path: raw JSON bytes (one document, an array, or NDJSON). Sources that
    /// already hold bytes (HTTP bodies, files) should always use this.
    pub async fn raw_json(&mut self, bytes: Bytes) -> Result<(), ChannelClosed> {
        let size = bytes.len();
        self.send(PushPayload::RawJson(bytes), size).await
    }

    /// Convenience path for programmatically constructed rows; serialized here to
    /// NDJSON so hosts see a single JSON ingest path.
    pub async fn rows(
        &mut self,
        rows: impl IntoIterator<Item = serde_json::Value>,
    ) -> Result<(), ChannelClosed> {
        let mut buf = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut buf, &row).map_err(|_| ChannelClosed)?;
            buf.push(b'\n');
        }
        if buf.is_empty() {
            return Ok(());
        }
        let size = buf.len();
        self.send(PushPayload::RawJson(buf.into()), size).await
    }

    pub async fn arrow(&mut self, batch: RecordBatch) -> Result<(), ChannelClosed> {
        let size = batch.get_array_memory_size();
        self.send(PushPayload::Arrow(batch), size).await
    }

    /// "All rows pushed so far are complete up to `cursor`" (clause S2).
    pub async fn checkpoint(&mut self, cursor: Cursor) -> Result<(), ChannelClosed> {
        self.send(PushPayload::Checkpoint(cursor), 0).await
    }

    async fn send(&mut self, payload: PushPayload, size: usize) -> Result<(), ChannelClosed> {
        // A single oversized message may not exceed the whole budget or it would
        // deadlock; it degrades to "budget fully drained" instead.
        let permits = size.min(self.budget_max).try_into().unwrap_or(u32::MAX);
        let permit = if permits > 0 {
            Some(
                Arc::clone(&self.budget)
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| ChannelClosed)?,
            )
        } else {
            None
        };
        self.tx
            .send(SourcePush {
                payload,
                _permit: permit,
            })
            .await
            .map_err(|_| ChannelClosed)
    }
}

/// Receiving half held by hosts (the engine, or a conformance harness).
#[derive(Debug)]
pub struct RecordsIn {
    rx: mpsc::Receiver<SourcePush>,
    budget: Arc<Semaphore>,
}

impl RecordsIn {
    /// `None` when the source finished (dropped its `RecordsOut`).
    pub async fn recv(&mut self) -> Option<SourcePush> {
        self.rx.recv().await
    }

    /// Closing tells the source to stop at its next push (clause S4).
    pub fn close(&mut self) {
        self.rx.close();
        // Wake any source blocked on the byte budget so it observes the closure.
        self.budget.close();
    }
}

/// Create the push channel between a host and one `Source::read` call.
/// `byte_budget` bounds in-flight bytes (backpressure); message count is secondary.
pub fn records_channel(byte_budget: usize) -> (RecordsOut, RecordsIn) {
    let budget = Arc::new(Semaphore::new(byte_budget));
    let (tx, rx) = mpsc::channel(64);
    (
        RecordsOut {
            tx,
            budget: Arc::clone(&budget),
            budget_max: byte_budget,
        },
        RecordsIn { rx, budget },
    )
}

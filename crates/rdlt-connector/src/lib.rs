//! # rdlt-connector — the in-process connector SPI
//!
//! `Source`/`Destination`/`LoadSession` traits and their adjuncts. The behavioral
//! contract these traits obey — sources resume from a committed cursor without re-emitting
//! rows, destinations stage writes invisibly and publish them atomically with pipeline
//! state, delivery is at-least-once and idempotent per commit — is enforced by the public
//! conformance suites in `rdlt-testkit`.
//!
//! Vocabulary types come from [`rdlt_core`] (re-exported as [`core`]); this crate adds
//! the traits. Traits are object-safe and all exchange types serde-serializable, so a
//! future process/WASM host can adapt this SPI over a wire without engine changes.
//!
//! SEMVER-SACRED: gated by `cargo semver-checks` in CI.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

pub mod capabilities;
pub mod channel;
pub mod error;
pub mod output;
pub mod secret;
pub mod spec;
pub mod stream;

use async_trait::async_trait;

pub use arrow_array as arrow;
pub use arrow_array::RecordBatch;
pub use capabilities::DestinationCapabilities;
pub use channel::{ChannelClosed, PushPayload, RecordsIn, RecordsOut, SourcePush, records_channel};
pub use error::{BoxError, DestinationError, SourceError};
/// How a destination writes parquet — plain data, no parquet dependency in
/// the SPI. Connectors re-export it from their own config paths and translate
/// it into `WriterProperties` at their own boundary.
pub use output::{ParquetCompression, ParquetOptions};
/// The rdlt vocabulary. Connectors MUST take these types from here (single-version
/// identity across the workspace).
pub use rdlt_core as core;
pub use rdlt_core::{
    CommitMeta, CommitReceipt, Cursor, LoadId, PipelineId, StateDoc, TableName, TableSchema,
    WriteMode,
};
/// The shared credential newtype: serde-transparent, renders as `***`,
/// [`Secret::reveal`] the sole (grep-able) accessor. Connectors re-export it
/// from their own config paths.
pub use secret::Secret;
pub use spec::ConnectorSpec;
pub use stream::StreamSpec;

/// A data source: declares streams, then pushes records for one stream per
/// [`Source::read`] call.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Name, version, config JSON-schema.
    fn spec(&self) -> ConnectorSpec;

    /// Discover the streams this source can read.
    ///
    /// Called once per run, before any [`Source::read`]. The engine plans table
    /// ownership from the result, so two streams must not claim the same
    /// destination table.
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError>;

    /// Read ONE stream, pushing through `req.out`. The engine schedules streams
    /// concurrently. Given `req.since`, the source MUST resume — never re-emit rows
    /// already covered by that cursor. Awaiting the push IS the flow control; treat
    /// [`ChannelClosed`] as cancellation and return promptly.
    async fn read(&self, req: ReadRequest) -> Result<(), SourceError>;
}

/// Everything a source needs to read one stream.
///
/// Deliberately an exhaustive pub-field struct (DX choice, semver-gated); [`ReadRequest::new`]
/// exists as the compatibility hedge.
#[derive(Debug)]
pub struct ReadRequest {
    /// The one stream this call must read.
    pub stream: StreamSpec,
    /// Last committed cursor; only ever a cursor the destination previously committed.
    pub since: Option<Cursor>,
    /// Push handle.
    pub out: RecordsOut,
}

impl ReadRequest {
    /// Build a request. Present so adding a field later is not a breaking change
    /// for callers who construct through here rather than with a struct literal.
    pub fn new(stream: StreamSpec, since: Option<Cursor>, out: RecordsOut) -> Self {
        Self { stream, since, out }
    }
}

/// A data destination: capability declaration + session factory.
#[async_trait]
pub trait Destination: Send + Sync + 'static {
    /// Name, version, config JSON-schema.
    fn spec(&self) -> ConnectorSpec;

    /// Truthful capability declaration — the engine plans from this and will not
    /// re-verify at runtime.
    fn capabilities(&self) -> DestinationCapabilities;

    /// Open a load session. MUST make uncommitted staged data from any previous
    /// (crashed) session invisible and reclaimable.
    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestinationError>;
}

/// Session context. Exhaustive pub-field struct; [`OpenCtx::new`] as hedge.
#[derive(Debug, Clone)]
pub struct OpenCtx {
    /// Identifies the pipeline whose state this session reads and republishes —
    /// the key under which `StateDoc` is persisted.
    pub pipeline: PipelineId,
    /// Identifies THIS run. Half of the `(load_id, commit_seq)` identity that
    /// makes a re-commit idempotent, and what distinguishes this session's
    /// staging from a crashed predecessor's.
    pub load_id: LoadId,
}

impl OpenCtx {
    /// Build a context. Present so adding a field later is not a breaking change
    /// for callers who construct through here rather than with a struct literal.
    pub fn new(pipeline: PipelineId, load_id: LoadId) -> Self {
        Self { pipeline, load_id }
    }
}

/// One destination load session. Writes are staged and invisible until `commit`;
/// publication is atomic with pipeline state and idempotent per `(load_id, commit_seq)`.
#[async_trait]
pub trait LoadSession: Send {
    /// Create or migrate the physical table for this schema version, and record the
    /// table's write disposition (`mode` is the root stream's mode for child tables —
    /// merge needs it at commit time). Idempotent; always precedes the first write at
    /// each schema version.
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError>;

    /// Write one batch. The engine guarantees `batch` conforms exactly to the last
    /// ensured schema for `table` and that per-table batches arrive in order. Delivery
    /// is at-least-once — safe because staged writes stay invisible until commit and a
    /// crashed session's staging is reclaimed on the next open.
    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError>;

    /// Atomically publish all writes since the last commit AND persist `meta.state`.
    /// Re-committing the same `(load_id, commit_seq)` returns the prior receipt
    /// without re-publishing.
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError>;

    /// Recover the state persisted by the latest successful commit, or `None` for a
    /// fresh pipeline.
    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError>;
}

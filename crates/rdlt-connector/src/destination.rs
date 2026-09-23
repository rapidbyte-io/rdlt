//! Destination connectors: the traits authors implement and the engine-facing form the SDK builds.

mod adapter;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::sync::Arc;

use arrow_array::RecordBatch;
use serde::de::DeserializeOwned;

use crate::capabilities::Capabilities;
use crate::commit::{CommitMeta, Receipt};
use crate::error::Result;
use crate::id::{Epoch, LoadId, PipelineId, SchemaVersion, SegmentId, TablePath};
use crate::schema::TableSchema;
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec};
use crate::state::StateRecord;
use crate::types::{Field, LogicalType};

pub use adapter::destination_factory;

/// Who is opening a destination session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenContext {
    /// The pipeline whose state the session reads and writes.
    pub pipeline: PipelineId,
    /// The load the session serves.
    pub load_id: LoadId,
}

/// A session a destination opened: the new epoch and the committed state records.
#[derive(Debug)]
pub struct Opened<S> {
    /// The session.
    pub session: S,
    /// The epoch this open set; commits with an older epoch must fail with a fenced error.
    pub epoch: Epoch,
    /// Every committed state record of the pipeline.
    pub state: Vec<StateRecord>,
}

/// A destination table, as the engine names it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableRef {
    /// The logical table.
    pub path: TablePath,
    /// The destination identifier the engine assigned.
    pub name: Arc<str>,
    /// The schema version writes follow.
    pub version: SchemaVersion,
}

/// A schema change to apply before writing under a new schema version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableChange {
    /// Create the table.
    Create {
        /// The table.
        table: TableRef,
        /// Its schema.
        schema: TableSchema,
    },
    /// Add a nullable column.
    AddColumn {
        /// The table.
        table: TableRef,
        /// The new column.
        field: Field,
    },
    /// Widen a column's type.
    Widen {
        /// The table.
        table: TableRef,
        /// The column.
        column: Arc<str>,
        /// The current type.
        from: LogicalType,
        /// The new type.
        to: LogicalType,
    },
}

/// What a writer staged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Rows staged.
    pub rows: u64,
    /// Bytes staged, as the destination measures them.
    pub bytes: u64,
}

/// A destination connector, as its author writes it.
///
/// The `#[destination(id = "...")]` attribute fills in `ID` and `VERSION`.
pub trait DestinationConnector: Sized + Send + Sync + 'static {
    /// The connector's id.
    const ID: &'static str;
    /// The connector's version.
    const VERSION: &'static str;
    /// The configuration the connector accepts; its JSON Schema is published.
    type Config: DeserializeOwned + schemars::JsonSchema + Send;
    /// A session serving one load.
    type Session: Session;

    /// What the destination can store and how it commits.
    fn capabilities(&self) -> Capabilities;

    /// Builds the connector's clients from its configuration.
    fn connect(
        config: Self::Config,
        context: &ConnectContext,
    ) -> impl Future<Output = Result<Self>> + Send;

    /// Verifies connectivity and permissions; must succeed exactly when opening can.
    fn check(&self) -> impl Future<Output = Result<()>> + Send;

    /// Atomically increments the pipeline's epoch and returns it with the committed state.
    fn open(
        &self,
        context: &OpenContext,
    ) -> impl Future<Output = Result<Opened<Self::Session>>> + Send;
}

/// One load's session with a destination.
pub trait Session: Send + 'static {
    /// A writer staging batches into one table.
    type Writer: TableWriter;

    /// Applies a schema change before any write under the new version.
    fn apply_schema(&mut self, change: &TableChange) -> impl Future<Output = Result<()>> + Send;

    /// A writer for `table`.
    fn writer(&mut self, table: &TableRef) -> impl Future<Output = Result<Self::Writer>> + Send;

    /// Removes every staged, unpublished segment; the SDK calls it at open.
    fn discard_staged(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Publishes the staged segments in `meta` with its state changes, atomically.
    ///
    /// Re-committing the same `(load_id, commit_seq)` returns the stored receipt without
    /// publishing. A commit whose epoch is older than the pipeline's fails with a fenced error.
    fn commit(&mut self, meta: &CommitMeta) -> impl Future<Output = Result<Receipt>> + Send;

    /// Ends the session.
    fn close(self) -> impl Future<Output = Result<()>> + Send;
}

/// Stages batches into one table; staged data is invisible until a commit publishes it.
pub trait TableWriter: Send + 'static {
    /// Stages `batch` as part of `segment`.
    fn write(
        &mut self,
        segment: SegmentId,
        batch: RecordBatch,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Makes every write so far durable in staging.
    fn flush(&mut self) -> impl Future<Output = Result<WriteStats>> + Send;
}

/// A connected destination, as the engine drives it; built by [`destination_factory`].
pub trait Destination: Send + Sync {
    /// What the destination can store and how it commits.
    fn capabilities(&self) -> &Capabilities;

    /// Verifies connectivity and permissions.
    fn check(&self) -> BoxFuture<'_, Result<()>>;

    /// Opens a session: fences older sessions, discards unpublished staging and returns state.
    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>>;
}

/// An open session, as the engine drives it.
pub struct OpenedSession {
    /// The session.
    pub session: Box<dyn DestinationSession>,
    /// The epoch this open set.
    pub epoch: Epoch,
    /// Every committed state record of the pipeline, with distinct keys.
    pub state: Vec<StateRecord>,
}

impl std::fmt::Debug for OpenedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedSession")
            .field("epoch", &self.epoch)
            .field("state", &self.state.len())
            .finish_non_exhaustive()
    }
}

/// The engine-facing form of [`Session`].
pub trait DestinationSession: Send {
    /// See [`Session::apply_schema`].
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>>;

    /// See [`Session::writer`].
    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>>;

    /// See [`Session::commit`].
    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>>;

    /// See [`Session::close`].
    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>>;
}

/// The engine-facing form of [`TableWriter`].
pub trait DestinationWriter: Send {
    /// See [`TableWriter::write`].
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>>;

    /// See [`TableWriter::flush`].
    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>>;
}

/// Creates connected destinations from JSON configuration.
pub trait DestinationFactory: Send + Sync {
    /// The connector's identity and configuration schema.
    fn spec(&self) -> &ConnectorSpec;

    /// Validates `config` and connects.
    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>>;
}

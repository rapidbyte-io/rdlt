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
use crate::id::{Epoch, GenerationId, LoadId, PipelineId, SchemaVersion, SegmentId, TablePath};
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
    /// The replace generation writes fill, hidden from readers until a commit finishes it; `None`
    /// writes the table itself.
    pub generation: Option<GenerationId>,
    /// For a merge table, how published rows are matched; `None` appends every row.
    pub merge: Option<MergeKey>,
}

/// How a merge table matches rows.
///
/// A published row replaces the published row with the same key. Among the rows one commit
/// publishes for one key, the row with the greatest `seq` wins.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MergeKey {
    /// The key columns' identifiers.
    pub columns: Vec<Arc<str>>,
    /// The identifier of the column that orders rows within a commit: 16 bytes of `Binary`,
    /// compared bytewise.
    pub seq: Arc<str>,
}

/// A schema change to apply before writing under a new schema version.
///
/// Changes name columns by their identifiers and give the types the destination stores, the
/// engine's metadata columns included. Applying a change the table already reflects must succeed
/// and change nothing: when an attempt fails between applying a change and committing, the next
/// attempt applies it again.
///
/// A column already holding the declared type — the type lattice joins the two to the column's
/// own type — reflects the change. A [`TableChange::Widen`] of a column that does not hold `to`
/// makes it the join of its type and `to`: an attempt that failed before committing may have
/// widened it along another branch of the lattice. A `Create` or `AddColumn` declaring a column
/// at a type the table's column does not hold, or a widen to a join the destination cannot
/// store, fails with a `Data` error coded `schema_conflict` and changes nothing; the engine
/// answers a conflicting new column by choosing another identifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableChange {
    /// Create the table; on a table that exists, add the columns it lacks as nullable.
    Create {
        /// The table.
        table: TableRef,
        /// Its columns.
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
        /// The column's identifier.
        column: Arc<str>,
        /// The current type.
        from: LogicalType,
        /// The new type.
        to: LogicalType,
    },
}

impl TableChange {
    /// The table the change applies to.
    pub fn table(&self) -> &TableRef {
        match self {
            Self::Create { table, .. }
            | Self::AddColumn { table, .. }
            | Self::Widen { table, .. } => table,
        }
    }
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

    /// Applies a schema change before any write under the new version; staging follows the
    /// table.
    ///
    /// A change the table already reflects succeeds and changes nothing; a change that conflicts
    /// with the table's columns fails as [`TableChange`] describes. Writers created before a
    /// change receive batches with the new columns after it.
    fn apply_schema(&mut self, change: &TableChange) -> impl Future<Output = Result<()>> + Send;

    /// A writer for `table`.
    fn writer(&mut self, table: &TableRef) -> impl Future<Output = Result<Self::Writer>> + Send;

    /// Removes every staged, unpublished segment; the SDK calls it at open.
    fn discard_staged(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Publishes the staged segments in `meta` with its state changes, atomically.
    ///
    /// Re-committing the same `(load_id, commit_seq)` returns the stored receipt without
    /// publishing. A commit whose epoch is older than the pipeline's fails with a fenced error.
    /// A segment this session never staged publishes nothing; the commit may instead fail.
    fn commit(&mut self, meta: &CommitMeta) -> impl Future<Output = Result<Receipt>> + Send;

    /// Ends the session.
    fn close(self) -> impl Future<Output = Result<()>> + Send;
}

/// Stages batches into one table; staged data is invisible until a commit publishes it.
///
/// A writer can outlive its session's epoch: once a newer session opens, anything this writer
/// stages must never be published. Refuse the write with a fenced error, or keep it apart from the
/// newer session's staging.
pub trait TableWriter: Send + 'static {
    /// Stages `batch` as part of `segment`.
    ///
    /// The batch's columns are some of the table's, each at the column's type or a type the
    /// column holds: a batch written after a widen can still carry the narrower type, since a
    /// partition resolved before the widen writes it.
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

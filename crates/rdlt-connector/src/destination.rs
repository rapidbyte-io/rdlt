//! Destination connectors: the traits authors implement and the engine-facing form the SDK builds.

mod adapter;
#[cfg(feature = "certify")]
mod read_back;
#[cfg(feature = "certify")]
mod readable;
#[cfg(test)]
mod tests;

use std::future::Future;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

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
#[cfg(feature = "certify")]
pub use read_back::{PublishedReader, PublishedRows, ReadBack, Reading};
#[cfg(feature = "certify")]
pub use readable::readable_destination_factory;

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
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
/// publishes for one key, the row with the greatest `seq` wins. A child table of a merge table
/// follows its root instead (see [`RootKey`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeKey {
    /// The key columns' identifiers.
    pub columns: Vec<Arc<str>>,
    /// The identifier of the column that orders rows within a commit: 16 bytes of `Binary`,
    /// compared bytewise.
    pub seq: Arc<str>,
    /// For a child table of a merge table, the root table whose merges replace its rows.
    pub root: Option<RootKey>,
    /// For a change stream's table, the columns that say what each row does; `None` for a table
    /// whose rows are all upserts ordered within their commit.
    pub changes: Option<ChangeColumns>,
    /// For a history table, the columns recording each version's life; `None` for a table that
    /// keeps one row per key.
    pub history: Option<HistoryColumns>,
}

/// How a history table keeps every version of each key (SCD2).
///
/// Written rows carry, besides their data, `valid_from` (when the version begins), `row_hash` (16
/// bytes of `Binary`, equal for rows whose data columns are equal, null on a delete), a null
/// `valid_to` and a true `is_current`. The key's rows apply in `seq` order, each past the key's
/// newest version's `seq`, its tombstone and the table's bound where the table is a change
/// stream's (as [`ChangeColumns`] says), and only within their commit otherwise:
///
/// - an upsert whose hash equals the key's current version's, where that version is not deleted,
///   changes nothing;
/// - any other upsert closes the key's current version, setting its `valid_to` to the upsert's
///   `valid_from` and its `is_current` to false, and publishes the upsert as the key's current
///   version;
/// - a hard delete closes the key's current version and records its tombstone; a soft delete
///   closes a current version that is not deleted and publishes, as current, a version keeping
///   its data and hash with the delete's `seq`, `valid_from` and deletion time;
/// - a truncate does to each key's current version sequenced before it what a delete does, and
///   raises the bound as a merge's truncate does.
///
/// A version's `seq` stays the row's that published it; closing changes only `valid_to` and
/// `is_current`. A change stream's source sends each key's changes in `seq` order, sending some
/// again at most, so a change equal to the current version leaves the key's guard where it was:
/// a change sent again from before it is either behind the guard or equal to the version still
/// current. `unchanged` flags have no place in a history table: its hashes need whole rows.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryColumns {
    /// When each version begins, a timestamp.
    pub valid_from: Arc<str>,
    /// When a later change closed the version, a timestamp; null while it is current.
    pub valid_to: Arc<str>,
    /// Whether the version is its key's current one, a boolean.
    pub is_current: Arc<str>,
    /// The hash of the version's data columns.
    pub row_hash: Arc<str>,
}

/// How the rows of a change stream's merge table apply (spec §9.3, §9.4).
///
/// Each written row carries a [`ChangeOp`](crate::ChangeOp) code in `op`, and its source position
/// in the key's `seq`, which orders rows across commits too: a row applies only when its `seq` is
/// greater than the published row's with its key, so a replayed change changes nothing. Rows
/// apply in `seq` order:
///
/// - an insert or update replaces the row with its key, keeping the published value of each
///   column `unchanged` flags (a column with no published value is null);
/// - a delete removes the row with its key, or marks it deleted (see [`Deletion`]);
/// - a truncate removes, or marks deleted, every row whose `seq` is smaller than its own; it
///   carries no key.
///
/// A source gives each row a position of its own. Where a truncate and a key's row share a
/// `seq` all the same, the truncate applies first: the row is not before it, and applies.
///
/// `op` and `unchanged` are in written batches only: they are never stored, and no schema change
/// names them.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeColumns {
    /// The column holding each row's op code, an `Int8`.
    pub op: Arc<str>,
    /// The column flagging an update's unchanged columns, where rows may flag some: a nullable
    /// `Binary` bitmap over the written batch's field ordinals, bit `i` (bit `i % 8` of byte
    /// `i / 8`) set when field `i` keeps its published value.
    pub unchanged: Option<Arc<str>>,
    /// How deletes and truncates remove rows.
    pub deletion: Deletion,
}

/// How a change stream's deletes and truncates remove rows.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Deletion {
    /// The rows are removed.
    Hard,
    /// The rows stay with their published values, and the column `at` records when they were
    /// deleted.
    ///
    /// A deleted row takes the deleting row's value in `at`, and its `seq`. A history table
    /// reads a version as deleted by that value, so its deletes and truncates each carry one:
    /// a destination refuses one that does not, as a data error coded `deletion_untimed`.
    Soft {
        /// The column recording when the row was deleted.
        at: Arc<str>,
    },
}

/// How a child table of a normalized merge stream follows its root table (spec §8.7): a merge
/// replaces all child rows of each root it publishes.
///
/// The child table's key ([`MergeKey::columns`]) is its rows' root id, and its `seq` column holds
/// the sequence of the root row each child row came from. A commit that publishes rows of the
/// root table removes every published child row whose root id is among those rows' `id`s, then
/// publishes the staged child rows whose root id and sequence are a published root row's `id` and
/// `seq`: the children of each root's winning row. A root whose winning row has no children is
/// left with none, and so is a child table the commit stages nothing for: the commit lists every
/// child table in [`CommitMeta::child_tables`].
///
/// [`CommitMeta::child_tables`]: crate::CommitMeta::child_tables
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootKey {
    /// The root table's identifier.
    pub table: Arc<str>,
    /// The root table's column holding each row's id.
    pub id: Arc<str>,
    /// The root table's sequence column.
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
/// answers a refused widen by leaving the column as it is and routing what it cannot hold to a
/// variant column, and a conflicting new column by choosing another identifier.
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
    ///
    /// A table belongs to the pipeline that first created it: a change from another pipeline's
    /// session fails with a `Config` error coded `table_owned` and changes nothing, so no
    /// pipeline's replace or merge reaches rows another pipeline loaded (clause `D-OWNED`).
    fn apply_schema(&mut self, change: &TableChange) -> impl Future<Output = Result<()>> + Send;

    /// A writer for `table`; a table another pipeline owns fails as
    /// [`apply_schema`](Self::apply_schema) does.
    fn writer(&mut self, table: &TableRef) -> impl Future<Output = Result<Self::Writer>> + Send;

    /// Removes the unpublished segments that sessions of the pipeline older than this one staged;
    /// the SDK calls it at open.
    ///
    /// A newer session's staging must stay: this call can run after a newer session opened and
    /// staged, when it waited behind that session.
    fn discard_staged(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Publishes the staged segments in `meta` with its state changes, atomically.
    ///
    /// Re-committing the same `(load_id, commit_seq)` returns the stored receipt without
    /// publishing. A commit whose epoch is older than the pipeline's fails with a fenced error.
    /// A segment this session never staged publishes nothing; the commit may instead fail. A
    /// segment may hold rows for several tables, as a stream and its child tables share their
    /// partition's segments, and its commit publishes each table's rows (clause `D-TABLES`).
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
    /// partition resolved before the widen writes it. A column may be dictionary-encoded, as the
    /// engine sends columns holding one value per batch; its values are what the column stores
    /// (clause `D-ENCODING`).
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
    /// Every committed state record of the pipeline, with distinct keys: the engine refuses state
    /// holding a key twice.
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

    /// Whether the destination can read back what it published, for certification.
    fn reads_back(&self) -> bool {
        false
    }

    /// Validates `config` and connects, with a reader of what the destination published.
    ///
    /// # Errors
    ///
    /// An unsupported error when the destination cannot read back what it published.
    #[cfg(feature = "certify")]
    fn connect_reading(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, Result<Reading>> {
        drop((config, context));
        Box::pin(async {
            Err(crate::error::ConnectorError::new(
                crate::error::ConnectorErrorKind::Unsupported,
                "this destination cannot read back what it published",
            )
            .with_code(PUBLISHED_CODE))
        })
    }
}

/// The code of the error a destination that cannot read back what it published refuses to.
#[cfg(any(feature = "serve", feature = "certify"))]
pub(crate) const PUBLISHED_CODE: &str = "published";

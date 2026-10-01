//! The rdlt connector contract: the vocabulary, the traits connectors implement, and the SDK.
//!
//! A source implements [`SourceConnector`] and one [`ReadStream`] per stream; a destination
//! implements [`DestinationConnector`], [`Session`] and [`TableWriter`]. The SDK turns them into
//! the engine-facing [`Source`] and [`Destination`] and owns the choreography that makes
//! exactly-once delivery work: cursor versioning, stop handling, barrier answers and discarding
//! stale staging at open.
//!
//! ```
//! use rdlt_connector::prelude::*;
//!
//! #[derive(serde::Deserialize, schemars::JsonSchema)]
//! struct Config {
//!     rows: u64,
//! }
//!
//! struct Numbers {
//!     rows: u64,
//! }
//!
//! #[source(id = "io.example.numbers")]
//! impl SourceConnector for Numbers {
//!     type Config = Config;
//!
//!     async fn connect(config: Config, _context: &ConnectContext) -> Result<Self> {
//!         Ok(Self { rows: config.rows })
//!     }
//!
//!     async fn check(&self) -> Result<()> {
//!         Ok(())
//!     }
//!
//!     fn streams(&self) -> Streams<Self> {
//!         Streams::new().with(Counting)
//!     }
//! }
//!
//! struct Counting;
//!
//! impl ReadStream<Numbers> for Counting {
//!     type Cursor = u64;
//!
//!     fn spec(&self) -> StreamSpec {
//!         StreamSpec::new(StreamName::new("numbers").expect("valid name"))
//!     }
//!
//!     async fn read(&self, source: &Numbers, _: &Partition, next: u64, out: &mut Emitter<u64>) -> Result<()> {
//!         for n in next..source.rows {
//!             out.rows(&[serde_json::json!({ "n": n })]).await?;
//!             out.checkpoint(&(n + 1)).await?;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! assert_eq!(source_factory::<Numbers>().spec().id.as_str(), "io.example.numbers");
//! ```

#![forbid(unsafe_code)]

#[cfg(all(feature = "serve", panic = "abort"))]
compile_error!(
    "a served connector fails a call whose task panics by unwinding: build with panic = \"unwind\""
);

mod capabilities;
mod catalog;
mod change;
mod commit;
mod config;
pub mod cost;
mod cursor;
mod destination;
mod emitter;
mod error;
mod factory;
mod id;
pub mod limits;
mod meta;
mod schema;
mod secret;
#[cfg(feature = "serve")]
pub mod serve;
mod sink;
mod source;
mod spec;
#[cfg(feature = "sqlgen")]
pub mod sqlgen;
mod state;
#[cfg(feature = "testing")]
pub mod testing;
pub mod text;
mod types;
#[cfg(feature = "wire")]
pub mod wire;

pub use capabilities::{
    Capabilities, CommitKind, DeleteModes, IdentifierCase, IdentifierChars, IdentifierRules,
    NestedSupport, SchemaChanges, WriteModes,
};
pub use catalog::{Catalog, Checkpointing, DuplicateStream, Partitioning, ReadMode, StreamSpec};
pub use change::{ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN, validate_change_batch};
pub use commit::{
    ChildTable, CommitMeta, DroppedTable, Receipt, SegmentRange, SegmentSet, UnorderedRanges,
};
pub use cursor::Cursor;
pub use destination::{
    ChangeColumns, Deletion, Destination, DestinationConnector, DestinationFactory,
    DestinationSession, DestinationWriter, HistoryColumns, MergeKey, OpenContext, Opened,
    OpenedSession, RootKey, Session, TableChange, TableRef, TableWriter, WriteStats,
    destination_factory,
};
#[cfg(feature = "certify")]
pub use destination::{
    PublishedReader, PublishedRows, ReadBack, Reading, readable_destination_factory,
};
pub use emitter::Emitter;
pub use error::{
    CURSOR_UNISSUED, ConnectorError, ConnectorErrorKind, LimitExceeded, RETENTION_LOST, Result,
    ResultExt,
};
pub use factory::{RoleFactory, Serve};
pub use id::{
    CommitSeq, ConnectorId, Epoch, GenerationId, IdError, LoadId, PartitionId, PipelineId,
    SchemaVersion, SegmentId, StreamName, TablePath,
};
pub use meta::{
    DELETED_AT_COLUMN, ID_COLUMN, IDX_COLUMN, IS_CURRENT_COLUMN, LOAD_ID_COLUMN, LOADED_AT_COLUMN,
    META_PREFIX, PARENT_ID_COLUMN, ROOT_ID_COLUMN, ROW_HASH_COLUMN, VALID_FROM_COLUMN,
    VALID_TO_COLUMN,
};
#[cfg(feature = "macros")]
pub use rdlt_connector_macros::{destination, source};
pub use schema::{ColumnKey, ColumnPath, EmptyColumnPath, SchemaError, TableSchema};
pub use secret::Secret;
#[cfg(feature = "serve")]
pub use serve::serve;
pub use sink::{
    Admission, LogLevel, PartitionFeed, PartitionSink, Permit, Push, Requested, SourceEvent,
    admitted_partition_channel, decoded_bytes, decoded_rows, partition_channel,
};
pub use source::{
    ACKNOWLEDGED_CODE, POSITION_UNSENT, Partition, PartitionPlan, ReadRequest, ReadStream, Sent,
    Source, SourceConnector, SourceFactory, Streams, source_factory,
};
#[cfg(feature = "certify")]
pub use source::{AcknowledgedReader, Acknowledging, acknowledging_source_factory};
pub use spec::{BoxFuture, ConnectContext, ConnectorSpec, Role};
pub use state::{
    NameConflict, NameMap, PartitionState, PipelineState, Sequences, StateChange, StateEntry,
    StateError, StateKey, StateRecord, StreamState, TableState,
};
pub use types::{
    DecimalType, Field, Fields, LOGICAL_TYPE_KEY, LogicalType, MAX_DECIMAL_PRECISION, TimeUnit,
    TypeError, TypeKind, UnsupportedType,
};

/// Everything a connector author needs, in one import.
pub mod prelude {
    pub use crate::{
        Capabilities, Catalog, Checkpointing, CommitMeta, ConnectContext, ConnectorError,
        ConnectorErrorKind, Cursor, DestinationConnector, Emitter, LogicalType, OpenContext,
        Opened, Partition, PartitionId, ReadMode, ReadStream, Receipt, Result, ResultExt, Secret,
        Session, SourceConnector, StreamName, StreamSpec, StreamState, Streams, TableChange,
        TableRef, TableSchema, TableWriter, WriteStats, destination_factory, source_factory,
    };
    #[cfg(feature = "certify")]
    pub use crate::{PublishedRows, ReadBack, readable_destination_factory};
    #[cfg(feature = "macros")]
    pub use crate::{destination, source};
}

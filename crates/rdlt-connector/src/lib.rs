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

mod capabilities;
mod catalog;
mod change;
mod commit;
mod config;
mod cursor;
mod destination;
mod emitter;
mod error;
mod id;
pub mod limits;
mod meta;
mod schema;
mod secret;
mod sink;
mod source;
mod spec;
#[cfg(feature = "sqlgen")]
pub mod sqlgen;
mod state;
#[cfg(feature = "testing")]
pub mod testing;
mod types;

pub use capabilities::{
    Capabilities, CommitKind, DeleteModes, IdentifierCase, IdentifierChars, IdentifierRules,
    NestedSupport, SchemaChanges, WriteModes,
};
pub use catalog::{Catalog, Checkpointing, DuplicateStream, Partitioning, ReadMode, StreamSpec};
pub use change::{ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN, validate_change_batch};
pub use commit::{CommitMeta, Receipt, SegmentRange, SegmentSet, UnorderedRanges};
pub use cursor::Cursor;
pub use destination::{
    Destination, DestinationConnector, DestinationFactory, DestinationSession, DestinationWriter,
    MergeKey, OpenContext, Opened, OpenedSession, Session, TableChange, TableRef, TableWriter,
    WriteStats, destination_factory,
};
pub use emitter::Emitter;
pub use error::{ConnectorError, ConnectorErrorKind, LimitExceeded, Result, ResultExt};
pub use id::{
    CommitSeq, ConnectorId, Epoch, GenerationId, IdError, LoadId, PartitionId, PipelineId,
    SchemaVersion, SegmentId, StreamName, TablePath,
};
pub use meta::{LOAD_ID_COLUMN, LOADED_AT_COLUMN, META_PREFIX};
#[cfg(feature = "macros")]
pub use rdlt_connector_macros::{destination, source};
pub use schema::{ColumnKey, ColumnPath, EmptyColumnPath, SchemaError, TableSchema};
pub use secret::Secret;
pub use sink::{
    Admission, LogLevel, PartitionFeed, PartitionSink, Permit, Push, SourceEvent,
    admitted_partition_channel, partition_channel,
};
pub use source::{
    Partition, ReadRequest, ReadStream, Source, SourceConnector, SourceFactory, Streams,
    source_factory,
};
pub use spec::{BoxFuture, ConnectContext, ConnectorSpec, Role};
pub use state::{
    NameConflict, NameMap, PartitionState, PipelineState, StateChange, StateEntry, StateError,
    StateKey, StateRecord, StreamState, TableState,
};
pub use types::{
    DecimalType, Field, Fields, LogicalType, MAX_DECIMAL_PRECISION, TimeUnit, TypeError, TypeKind,
    UnsupportedType,
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
    #[cfg(feature = "macros")]
    pub use crate::{destination, source};
}

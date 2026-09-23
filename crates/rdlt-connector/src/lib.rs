//! The rdlt connector contract: the vocabulary, the traits connectors implement, and the SDK.

mod catalog;
mod change;
mod commit;
mod cursor;
mod error;
mod id;
pub mod limits;
mod schema;
mod secret;
mod state;
mod types;

pub use catalog::{Catalog, Checkpointing, DuplicateStream, Partitioning, ReadMode, StreamSpec};
pub use change::{ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN, validate_change_batch};
pub use commit::{CommitMeta, Receipt, SegmentRange, SegmentSet, UnorderedRanges};
pub use cursor::Cursor;
pub use error::{ConnectorError, ConnectorErrorKind, LimitExceeded, Result, ResultExt};
pub use id::{
    CommitSeq, ConnectorId, Epoch, GenerationId, IdError, LoadId, PartitionId, PipelineId,
    SchemaVersion, SegmentId, StreamName, TablePath,
};
#[cfg(feature = "macros")]
pub use rdlt_connector_macros::{destination, source};
pub use schema::{ColumnPath, EmptyColumnPath, SchemaError, TableSchema};
pub use secret::Secret;
pub use state::{
    NameConflict, NameMap, PartitionState, PipelineState, StateChange, StateEntry, StateError,
    StateKey, StateRecord, StreamState, TableState,
};
pub use types::{
    DecimalType, Field, Fields, LogicalType, MAX_DECIMAL_PRECISION, TimeUnit, TypeError, TypeKind,
    UnsupportedType,
};

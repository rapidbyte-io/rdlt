//! The rdlt connector contract: the vocabulary, the traits connectors implement, and the SDK.

mod id;

pub use id::{
    CommitSeq, ConnectorId, Epoch, GenerationId, IdError, LoadId, PartitionId, PipelineId,
    SchemaVersion, SegmentId, StreamName, TablePath,
};
#[cfg(feature = "macros")]
pub use rdlt_connector_macros::{destination, source};

//! Load data from sources into destinations exactly once: a pipeline described in code or in a
//! document, placed by a host its operator configures, and run with one set of checks, errors
//! and events.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod codes;
mod document;
mod error;
mod host;
pub mod limits;
mod lower;
mod pipeline;

pub use document::{
    Access, Column, Commit, Connector, ConnectorReference, Defaults, DeleteMode, Document, Hint,
    MaskStep, MaskWith, Nested, OnTruncate, OnUnsupported, ReadMode, Resources, RetentionLoss,
    Retry, SchemaPolicy, SchemaRules, Side, Stream, TransformStep, Until, WriteMode,
};
pub use error::Error;
pub use host::Host;
#[cfg(feature = "remote")]
pub use host::Tls;
pub use pipeline::{Checked, Pipeline, PipelineBuilder, PlacedConnector, Placement};
pub use rdlt_connector::{
    BoxFuture, Catalog, ConnectorId, ConnectorSpec, DestinationConnector, DestinationFactory,
    Epoch, PipelineId, Role, Secret, SourceConnector, SourceFactory, StreamName, StreamSpec,
    TablePath,
};
pub use rdlt_engine::{
    Bound, Code, ErrorKind, ErrorReport, Literal, Mask, Op, Origin, ResetReport, ResetScope,
    Transform, TransformError,
};
#[cfg(feature = "local")]
pub use rdlt_host::{Interrupts, StopsSpawned};
pub use rdlt_host::{SecretFault, SecretKind, SecretReference, SecretResolver};
pub use serde_json::{Value, json};

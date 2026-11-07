//! Load data from sources into destinations exactly once: a pipeline described in code or in a
//! document, placed by a host its operator configures, and run with one set of checks, errors
//! and events.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod codes;
mod document;
mod error;
pub mod limits;

pub use document::{
    Access, Column, Commit, Connector, ConnectorReference, Defaults, DeleteMode, Document, Hint,
    MaskStep, MaskWith, Nested, OnTruncate, OnUnsupported, ReadMode, Resources, RetentionLoss,
    Retry, SchemaPolicy, SchemaRules, Side, Stream, TransformStep, Until, WriteMode,
};
pub use error::Error;
pub use rdlt_engine::{Code, ErrorKind, ErrorReport, Literal, Origin};
pub use serde_json::{Value, json};

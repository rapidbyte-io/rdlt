//! Load data from sources into destinations exactly once: a pipeline described in code or in a
//! document, placed by a host its operator configures, and run with one set of checks, errors
//! and events.
//!
//! ```no_run
//! use rdlt::{Connector, Host, Pipeline, ReadMode, Side, Stream};
//! use rdlt_connector_reference::{LogSource, SqliteDestination};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let database = std::env::args()
//!         .nth(1)
//!         .unwrap_or_else(|| "demo.db".to_owned());
//!     // Compiled in and trusted: both connectors run in this process.
//!     let host = Host::new()
//!         .source::<LogSource>()
//!         .destination::<SqliteDestination>();
//!     // Two partitions of 500 seeded messages each, read up to their heads.
//!     let source = Side::new(Connector::new("io.rapidbyte.log")).config(rdlt::json!({
//!         "seed": 7,
//!         "streams": [{ "name": "events", "partitions": 2, "messages": 500, "bounded": true }]
//!     }));
//!     let destination = Side::new(Connector::new("io.rapidbyte.sqlite"))
//!         .config(rdlt::json!({ "path": database }));
//!     // One stream, read incrementally: each run starts where the last committed one ended.
//!     let pipeline = Pipeline::builder("events-demo", source, destination)
//!         .stream(Stream::new("events").read(ReadMode::Incremental))
//!         .build(&host)
//!         .await?;
//!     let outcome = pipeline.start().await;
//!     if let Some(error) = outcome.error {
//!         return Err(error.into());
//!     }
//!     println!("{:?}: committed {} rows", outcome.report.status, outcome.report.rows);
//!     Ok(())
//! }
//! ```
//!
//! The facade runs on tokio: build and await pipelines inside a tokio runtime. A code transform
//! the host registers speaks Arrow, which [`arrow`] re-exports.
//!
//! # Features
//!
//! - `local`, on by default: connectors spawned as local processes from the host's connector
//!   directories, in the sandbox unless the host trusts their binaries.
//! - `remote`: connectors reached over mutual TLS, as `Host::remote` and `Tls` set up.
//! - `sql`: SQL transforms, planned by `rdlt-sql`.
//! - `otel`: [`telemetry::otlp`], which exports traces and metrics to a collector.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod codes;
mod document;
mod error;
mod events;
mod host;
pub mod limits;
mod lower;
mod metrics;
mod pipeline;
mod run;
#[cfg(feature = "otel")]
pub mod telemetry;

pub use arrow;
pub use document::{
    Access, Column, Commit, Connector, ConnectorReference, Defaults, DeleteMode, Document, Hint,
    MaskStep, MaskWith, Nested, OnTruncate, OnUnsupported, ReadMode, Resources, RetentionLoss,
    Retry, SchemaPolicy, SchemaRules, Side, Stream, TransformStep, Until, WriteMode,
};
pub use error::Error;
pub use events::{Events, Lagged};
#[cfg(feature = "remote")]
pub use host::Tls;
pub use host::{Host, HostFile, RemoteFiles};
pub use pipeline::{
    Checked, Pipeline, PipelineBuilder, PlacedConnector, Placement, StreamSelector,
};
pub use rdlt_connector::{
    BoxFuture, Catalog, ConnectorId, ConnectorSpec, DestinationConnector, DestinationFactory,
    Epoch, PipelineId, Role, Secret, SourceConnector, SourceFactory, StreamName, StreamSpec,
    TablePath,
};
pub use rdlt_engine::{
    Acks, AttemptReport, Bound, Call, Code, ColumnChange, CommitPhases, Commits, Cores, Counters,
    Discard, DiscardReason, Discards, ErrorKind, ErrorReport, Event, EventKind, FilteredRows,
    Flushes, Forgotten, Listed, Literal, Mask, MemoryHeld, Op, OpenWaits, Origin, Partitions,
    Phase, PoolCounters, Quiet, Report, ResetReport, ResetScope, RunControl, RunStatus, Scope,
    ShredCounts, Status, Step, StopMode, StreamReport, StreamStatus, Transform, TransformError,
    Trigger, Waited, Waits, WriterCall, WriterCounters, WriterStatus,
};
#[cfg(feature = "local")]
pub use rdlt_host::{Interrupts, StopsSpawned};
pub use rdlt_host::{SecretFault, SecretKind, SecretReference, SecretResolver};
pub use run::{Outcome, Run};
pub use serde_json::{Value, json};

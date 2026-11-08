//! Load data from sources into destinations exactly once: a pipeline described in code or in a
//! document, placed by a host its operator configures, and run with one set of checks, errors
//! and events.
//!
//! ```no_run
//! use rdlt::{Access, Connector, Host, Pipeline, Side, Stream};
//!
//! # #[cfg(feature = "local")]
//! # async fn example() -> Result<(), rdlt::Error> {
//! let host = Host::new().connectors("/opt/rdlt/bin").writable("/data");
//! let source = Side::new(Connector::new("io.rapidbyte.generator"))
//!     .config(rdlt::json!({ "seed": 7, "streams": [{ "name": "events", "rows": 1000 }] }));
//! let destination = Side::new(Connector::new("io.rapidbyte.sqlite"))
//!     .config(rdlt::json!({ "path": "/data/out.db" }))
//!     .access(Access::new().write("/data"));
//! let pipeline = Pipeline::builder("events", source, destination)
//!     .stream(Stream::new("events"))
//!     .build(&host)
//!     .await?;
//! let run = pipeline.start();
//! let mut events = run.events();
//! // Read while the run runs, so no event falls out of the run's ring.
//! let telling = tokio::spawn(async move {
//!     while let Some(event) = events.next().await {
//!         println!("{event:?}");
//!     }
//! });
//! let outcome = run.await;
//! telling.await.ok();
//! match outcome.error {
//!     Some(error) => Err(error),
//!     None => Ok(()),
//! }
//! # }
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

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod codes;
mod document;
mod error;
mod events;
mod host;
pub mod limits;
mod lower;
mod pipeline;
mod run;

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
    Acks, AttemptReport, Bound, Call, Code, ColumnChange, CommitPhases, Commits, Counters, Discard,
    DiscardReason, Discards, ErrorKind, ErrorReport, Event, EventKind, FilteredRows, Flushes,
    Forgotten, Listed, Literal, Mask, MemoryHeld, Op, OpenWaits, Origin, Partitions, Phase,
    PoolCounters, Quiet, Report, ResetReport, ResetScope, RunControl, RunStatus, Scope,
    ShredCounts, Status, Step, StopMode, StreamReport, StreamStatus, Transform, TransformError,
    Trigger, Waited, Waits, WriterCall, WriterCounters, WriterStatus,
};
#[cfg(feature = "local")]
pub use rdlt_host::{Interrupts, StopsSpawned};
pub use rdlt_host::{SecretFault, SecretKind, SecretReference, SecretResolver};
pub use run::{Outcome, Run};
pub use serde_json::{Value, json};

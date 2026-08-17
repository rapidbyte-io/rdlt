//! # rdlt — library-first ELT engine
//!
//! Extract → shred (normalize) → load, with schema inference/evolution, incremental
//! cursors, and crash-safe resumable runs.
//!
//! ```no_run
//! use rdlt::prelude::*;
//! use rdlt::report;
//! use rdlt_testkit::{MemoryDestination, MemorySource};
//!
//! # async fn demo() -> Result<(), Error> {
//! let pipeline = Pipeline::builder("demo")
//!     .source(MemorySource::default())
//!     .destination(MemoryDestination::new())
//!     .write_mode(WriteMode::Append)
//!     .build()?; // config errors die here, pre-I/O
//! let report: report::Run = pipeline.run().await?;
//! # let _ = report; Ok(())
//! # }
//! ```
//!
//! Connectors run OUT OF PROCESS: every arm of a pipeline document — the
//! rich spellings (`postgres:`, `file:`, `duckdb:`, …) and the explicit
//! `connector:` form alike — resolves to a connector binary spawned and
//! supervised through [`runtime`]. In-process implementations of the SPI
//! traits (like the doctest's memory pair above) still plug straight into
//! the [`Pipeline`] builder.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

mod builder;
pub mod pipeline_spec;

pub use builder::{Pipeline, PipelineBuilder};
/// The connector-authoring layer, re-exported for embedders that build or
/// parse connector configs directly (`sdk::config::Document` is the trait
/// behind every connector's `from_yaml`/`from_json`/`from_value`).
pub use rdlt_connector_sdk as sdk;
pub use rdlt_core::commit::{BatchPolicy, CommitPolicy, WriteMode};
pub use rdlt_core::cursor::Cursor;
pub use rdlt_core::error::Error;
pub use rdlt_core::event::{PartCloseReason, PipelineEvent};
pub use rdlt_core::id::{StreamName, TableName};
pub use rdlt_core::metrics::{self, Metrics};
pub use rdlt_core::report::{self, ResumedFrom};
pub use rdlt_engine::policy::{PolicyAction, SchemaPolicy};
/// The out-of-process connector runtime, re-exported for embedders that
/// supply their own [`runtime::provider::Provider`] to
/// [`pipeline_spec::build_pipeline_with`] — or configure the default
/// [`runtime::local::Local`] the no-provider form uses.
pub use rdlt_runtime as runtime;

/// Glob-import target for pipeline authors.
///
/// Rule: the prelude is the crate-root vocabulary re-export set (every TYPE re-exported
/// at `rdlt::`) plus the [`Pipeline`] entry point. Import `rdlt::prelude::*` and you can
/// name everything a pipeline definition touches without reaching for the submodules;
/// the module-nouned outcome types stay behind their nouns — [`report::Run`] and
/// [`metrics::Snapshot`] — because bare `Run`/`Table`/`Snapshot` in a glob would
/// collide with an author's own names. [`Error`] is the deliberate exception: a
/// crate's own `Error` at the root and in the prelude is the usual convention, and
/// the only name it can shadow is the `std::error::Error` trait, which is not a
/// type-name clash.
pub mod prelude {
    pub use crate::{
        BatchPolicy, CommitPolicy, Cursor, Error, Metrics, PartCloseReason, Pipeline,
        PipelineEvent, PolicyAction, ResumedFrom, SchemaPolicy, StreamName, TableName, WriteMode,
    };
}

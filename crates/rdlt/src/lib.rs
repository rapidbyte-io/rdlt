//! # rdlt — library-first ELT engine
//!
//! Extract → shred (normalize) → load, with schema inference/evolution, incremental
//! cursors, and crash-safe resumable runs.
//!
//! ```no_run
//! use rdlt::prelude::*;
//! use rdlt_testkit::{MemoryDestination, MemorySource};
//!
//! # async fn demo() -> Result<(), RdltError> {
//! let pipeline = Pipeline::builder("demo")
//!     .source(MemorySource::default())
//!     .destination(MemoryDestination::new())
//!     .write_mode(WriteMode::Append)
//!     .build()?; // config errors die here, pre-I/O
//! let report: RunReport = pipeline.run().await?;
//! # let _ = report; Ok(())
//! # }
//! ```
//!
//! Connector features: `rest` (declarative REST source), `duckdb`, `postgres`
//! (with `postgres-source` / `postgres-dest` narrowings), `file`, `parquet`, and
//! `iceberg`. The bundled connectors live under [`connector`], one module per SYSTEM —
//! e.g. `rdlt::connector::postgres` carries `source`, `dest`, and `tls` together.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

mod builder;
pub mod pipeline_spec;

pub use builder::{Pipeline, PipelineBuilder};
pub use rdlt_core::{
    CommitPolicy, Cursor, PipelineEvent, PolicyAction, RdltError, ResumedFrom, RunReport,
    SchemaPolicy, TableReport, WriteMode,
};

/// The bundled connectors, one module per system — the same pattern as the
/// crates themselves (`rdlt-connector-<system>`). A system module exposes
/// everything it has: `connector::postgres::{source, destination, tls}`,
/// `connector::rest::RestSource`, `connector::duckdb::DuckDb`, etc.
pub mod connector {
    #[cfg(feature = "duckdb")]
    pub use rdlt_connector_duckdb as duckdb;

    #[cfg(feature = "file")]
    pub use rdlt_connector_file as file;

    #[cfg(feature = "iceberg")]
    pub use rdlt_connector_iceberg as iceberg;

    /// Frozen alias: the parquet destination lives in the file family;
    /// `rdlt::connector::parquet::ParquetDir` keeps resolving for consumers
    /// of the frozen `parquet` feature spelling.
    #[cfg(feature = "parquet")]
    pub use rdlt_connector_file as parquet;

    #[cfg(any(
        feature = "postgres",
        feature = "postgres-source",
        feature = "postgres-dest"
    ))]
    pub use rdlt_connector_postgres as postgres;

    #[cfg(feature = "rest")]
    pub use rdlt_connector_rest as rest;

    #[cfg(feature = "snowflake")]
    pub use rdlt_connector_snowflake as snowflake;
}

/// Glob-import target for pipeline authors.
///
/// Rule: the prelude is the crate-root vocabulary re-export set (every type re-exported
/// at `rdlt::`) plus the [`Pipeline`] entry point. Import `rdlt::prelude::*` and you can
/// name everything a pipeline definition and its report handling touch without reaching
/// for the submodules. Anything reachable at the crate root is reachable here.
pub mod prelude {
    pub use crate::{
        CommitPolicy, Cursor, Pipeline, PipelineEvent, PolicyAction, RdltError, ResumedFrom,
        RunReport, SchemaPolicy, TableReport, WriteMode,
    };
}

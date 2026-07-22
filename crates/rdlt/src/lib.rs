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
//! let mut pipeline = Pipeline::builder("demo")
//!     .source(MemorySource::default())
//!     .destination(MemoryDestination::new())
//!     .write_mode(WriteMode::Append)
//!     .build()?; // config errors die here, pre-I/O
//! let report: RunReport = pipeline.run().await?;
//! # let _ = report; Ok(())
//! # }
//! ```
//!
//! Connector features: `rest` (declarative REST source), `duckdb`, `postgres`,
//! `postgres-source`, `file`, `parquet`. The bundled connectors live under
//! [`connector`], one module per SYSTEM — e.g. `rdlt::connector::postgres`
//! carries `source`, `dest`, and `tls` together.

mod builder;

pub use builder::{Pipeline, PipelineBuilder};
pub use rdlt_core::{
    CommitPolicy, Cursor, PipelineEvent, PolicyAction, RdltError, ResumedFrom, RunReport,
    SchemaPolicy, TableReport, WriteMode,
};

/// The bundled connectors, one module per system — the same pattern as the
/// crates themselves (`rdlt-connector-<system>`). A system module exposes
/// everything it has: `connector::postgres::{source, dest, tls}`,
/// `connector::rest::RestSource`, `connector::duckdb::DuckDb`, ….
pub mod connector {
    #[cfg(feature = "duckdb")]
    pub use rdlt_connector_duckdb as duckdb;

    #[cfg(feature = "file")]
    pub use rdlt_connector_file as file;

    #[cfg(any(feature = "postgres", feature = "postgres-source"))]
    pub use rdlt_connector_postgres as postgres;

    #[cfg(feature = "rest")]
    pub use rdlt_connector_rest as rest;
}

pub mod prelude {
    pub use crate::{
        CommitPolicy, Pipeline, PipelineEvent, PolicyAction, RdltError, RunReport, SchemaPolicy,
        WriteMode,
    };
}

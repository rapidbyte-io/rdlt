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
//! Connector features: `rest` (declarative REST source), `duckdb`, `postgres`.

mod builder;

pub use builder::{Pipeline, PipelineBuilder};
pub use rdlt_core::{
    CommitPolicy, Cursor, PipelineEvent, PolicyAction, RdltError, ResumedFrom, RunReport,
    SchemaPolicy, TableReport, WriteMode,
};

#[cfg(feature = "rest")]
pub use rdlt_source_rest as rest;

#[cfg(feature = "duckdb")]
pub use rdlt_dest_duckdb as duckdb;

#[cfg(feature = "postgres")]
pub use rdlt_dest_postgres as postgres;

#[cfg(feature = "file")]
pub use rdlt_source_file as file;

#[cfg(feature = "parquet")]
pub use rdlt_dest_parquet as parquet;

#[cfg(feature = "postgres-source")]
pub use rdlt_source_postgres as postgres_source;

pub mod prelude {
    pub use crate::{
        CommitPolicy, Pipeline, PipelineEvent, PolicyAction, RdltError, RunReport, SchemaPolicy,
        WriteMode,
    };
}

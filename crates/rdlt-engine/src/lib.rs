//! # rdlt-engine
//!
//! The rdlt ingestion engine: shredding, schema registry, write-ahead log, and load
//! orchestration over byte-bounded channels.
//!
//! ```rust,no_run
//! # use rdlt_connector::source::StreamSpec;
//! # use rdlt_engine::{Engine, EngineConfig};
//! # use rdlt_testkit::{MemoryDestination, MemorySource};
//! # use serde_json::json;
//! # async fn run() -> Result<(), rdlt_engine::RdltError> {
//! let source = MemorySource::single_stream(
//!     StreamSpec::new("events"),
//!     vec![json!({"id": 1, "name": "first"})],
//! );
//! let destination = MemoryDestination::new();
//!
//! let config = EngineConfig::new("quickstart");
//! let engine = Engine::new(config, source, destination.clone());
//!
//! // Subscribe before running; multiple subscribers each see every event.
//! let mut events = engine.events();
//!
//! let report = engine.run().await?;
//! assert_eq!(report.total_rows(), 1);
//! while let Some(event) = events.recv().await {
//!     println!("{event:?}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Everything below `lib.rs` is `pub(crate)`: unit-tested privately, free to change
//! without semver cost. The public surface is [`Engine`] and [`EngineConfig`] plus the
//! vocabulary re-exported from `rdlt_core` — the one exception is the doc-hidden
//! [`fuzzing`] module, the bench/fuzz seam, which carries no semver guarantee.

mod config;
mod coverage;
mod engine;
mod load;
mod runtime;
mod schema;
mod shred;
mod wal;

pub use config::{
    DEFAULT_BYTE_BUDGET, DEFAULT_MAX_BATCH_CELLS, DEFAULT_MAX_STREAMS_PER_SOURCE, EngineConfig,
};
pub use engine::{Engine, EventStream};
pub use rdlt_core::{Metrics, MetricsSnapshot, PartClose, PipelineEvent, RdltError, RunReport};

#[doc(hidden)]
pub mod fuzzing;

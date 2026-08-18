//! # rdlt-engine
//!
//! The rdlt ingestion engine: shredding, schema registry, write-ahead log, and load
//! orchestration over byte-bounded channels.
//!
//! ```rust,no_run
//! # use rdlt_connector::source::StreamSpec;
//! # use rdlt_engine::{Engine, EngineConfig};
//! # use rdlt_testkit::memory;
//! # use serde_json::json;
//! # async fn run() -> Result<(), rdlt_core::error::Error> {
//! let source = memory::Source::single_stream(
//!     StreamSpec::new("events"),
//!     vec![json!({"id": 1, "name": "first"})],
//! );
//! let destination = memory::Destination::new();
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
//! The public surface is [`Engine`] and [`EngineConfig`], the vocabulary
//! from `rdlt_core` (canonical paths, not re-exported here), and the three
//! engine-owned semantics modules an embedder configures or a host names:
//! [`policy`] (what happens when data would change a schema), [`naming`]
//! (destination identifiers) and [`identity`] (deterministic row ids).
//! Everything else below `lib.rs` is `pub(crate)`: unit-tested privately, free
//! to change without semver cost — the one exception is the doc-hidden
//! [`fuzzing`] module, the bench/fuzz seam, which carries no semver guarantee.

mod classify;
mod config;
mod engine;
pub mod identity;
mod lineage;
mod load;
pub mod naming;
pub mod policy;
mod runtime;
mod schema;
mod shred;
mod wal;

pub use config::{
    DEFAULT_BYTE_BUDGET, DEFAULT_MAX_BATCH_CELLS, DEFAULT_MAX_STREAMS_PER_SOURCE, EngineConfig,
};
pub use engine::{Engine, EventStream};

#[doc(hidden)]
pub mod fuzzing;

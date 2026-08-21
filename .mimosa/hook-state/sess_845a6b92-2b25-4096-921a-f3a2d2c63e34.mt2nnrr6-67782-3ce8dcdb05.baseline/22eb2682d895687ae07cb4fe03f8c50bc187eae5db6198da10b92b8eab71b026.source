//! # rdlt-engine
//!
//! The rdlt ingestion engine: it turns a source's pushes into typed,
//! lineage-stamped batches under a schema policy, journals intent in a
//! write-ahead log, and drives one load session to exactly-once commits —
//! resumable, cancel-safe, byte-bounded.
//!
//! ```rust,no_run
//! # use rdlt_connector::source::StreamSpec;
//! # use rdlt_engine::config::Config;
//! # use rdlt_engine::engine::Engine;
//! # use rdlt_testkit::memory;
//! # use serde_json::json;
//! # async fn run() -> Result<(), rdlt_core::error::Error> {
//! let source = memory::Source::single_stream(
//!     StreamSpec::new("events"),
//!     vec![json!({"id": 1, "name": "first"})],
//! );
//! let destination = memory::Destination::new();
//!
//! let engine = Engine::new(Config::new("quickstart"), source, destination.clone());
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
//! The public surface is the engine handle ([`engine::Engine`], its
//! [`engine::EventStream`] and the [`check::Summary`] its no-run check
//! reports) and its [`config::Config`], the vocabulary from
//! `rdlt_core` (canonical paths, not re-exported here), and the three
//! engine-owned semantics an embedder configures or a host names:
//! [`policy`] (what happens when data would change a schema), [`naming`]
//! (destination identifiers) and [`identity`] (deterministic row ids).
//! Everything else is `pub(crate)`: unit-tested privately, free to change
//! without semver cost — the one exception is the doc-hidden [`fuzzing`]
//! module, the bench/fuzz seam, which carries no semver guarantee.

mod blocking;
pub mod check;
mod classify;
pub mod config;
pub mod engine;
#[doc(hidden)]
pub mod fuzzing;
pub mod identity;
mod lineage;
mod load;
pub mod naming;
pub mod policy;
mod run;
mod schema;
mod shred;
#[cfg(test)]
mod testing;
mod wal;

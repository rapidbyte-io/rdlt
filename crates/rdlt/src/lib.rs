//! # rdlt — the crate an embedder depends on
//!
//! A pipeline is ONE YAML document: pipeline-wide settings, a source arm
//! and a destination arm, each arm naming an out-of-process connector by
//! id (`connector: {id, config, …}`). Construct it from the file, run it.
//! Construction spawns and handshakes both connectors — the connector's
//! own gate validates its config — and every configuration refusal dies
//! there, before a row moves:
//!
//! ```no_run
//! use rdlt::error::Error;
//! use rdlt::pipeline::Pipeline;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let pipeline = Pipeline::from_file("pipeline.yaml").await?;
//! match pipeline.run().await {
//!     Ok(report) => println!("{} rows", report.total_rows()),
//!     Err(Error::Cancelled) => println!("cancelled — build again to resume"),
//!     Err(error) => return Err(error.into()),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The document can also come as text — YAML or JSON —
//! ([`pipeline::Pipeline::from_text`]) or as a parsed or constructed
//! [`document::Document`] ([`pipeline::Pipeline::from_document`]); the
//! `from_*` constructors hand the engine's boundary — the
//! [`pipeline::Pipeline`] builder — a source VALUE and a destination
//! VALUE. In production those values are the [`runtime`]'s process
//! adapters over spawned connector binaries; an embedder with its own
//! provider (a pool, a remote scheduler) supplies it through
//! [`pipeline::Pipeline::from_document_with`]. Hand-rolled `impl Source`
//! / `impl Destination` values are test doubles.
//!
//! Every name lives behind its noun: [`commit`], [`cursor`], [`error`],
//! [`event`], [`id`], [`metrics`], [`policy`], [`report`] are the
//! vocabulary; [`prelude`] glob-imports the author's share of it (never
//! `Error` — spell [`error::Error`]); [`sdk`] is connector authoring and
//! [`sdk::spi`] the SPI itself. This crate computes nothing — it names,
//! documents, and constructs.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

pub mod commit;
pub mod cursor;
pub mod document;
pub mod error;
pub mod event;
pub mod id;
pub mod metrics;
pub mod pipeline;
pub mod policy;
pub mod prelude;
pub mod report;
pub mod runtime;
pub mod sdk;

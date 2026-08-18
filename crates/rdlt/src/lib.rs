//! # rdlt — the crate an embedder depends on
//!
//! A pipeline is ONE YAML document: pipeline-wide settings, a source arm
//! and a destination arm, each arm naming an out-of-process connector by
//! id (`connector: {id, config, …}`). Read it, parse it, build it, run it.
//! Construction spawns and handshakes both connectors — the connector's
//! own gate validates its config — and every configuration refusal dies
//! there, before a row moves:
//!
//! ```no_run
//! use std::path::Path;
//!
//! use rdlt::document;
//! use rdlt::error::Error;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let path = Path::new("pipeline.yaml");
//! let text = document::read(path)?;
//! let doc = document::parse(&text)?;
//! let base = path.parent().unwrap_or(Path::new(""));
//! let pipeline = document::build(&doc, base).await?;
//! match pipeline.run().await {
//!     Ok(report) => println!("{} rows", report.total_rows()),
//!     Err(Error::Cancelled) => println!("cancelled — build again to resume"),
//!     Err(error) => return Err(error.into()),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! [`document::build`] hands the engine's boundary — the
//! [`pipeline::Pipeline`] builder — a source VALUE and a destination
//! VALUE. In production those values are the [`runtime`]'s process
//! adapters over spawned connector binaries; an embedder with its own
//! provider (a pool, a remote scheduler) supplies it through
//! [`document::build_with`]. Hand-rolled `impl Source` /
//! `impl Destination` values are test doubles.
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

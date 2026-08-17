//! # rdlt-testkit — the connector certification kit
//!
//! What connector and embedder authors certify and demonstrate with:
//! the conformance suites ("certified = passes conformance"), the
//! black-box memory pair, the shared fixtures, the container gate, the
//! spawn scaffold, and the crash-point registry scanner. Depends on the
//! SPI only — anything here that needed engine internals would mean the
//! SPI is missing a seam. Connector-agnostic by the same rule: a
//! system-specific fixture (a postgres container, a credential
//! convention) lives with its connector; this crate carries only what
//! every connector shares.
//!
//! Certification runs anywhere — no network, no containers, no
//! credentials. Build the connector (here: the bundled memory source),
//! hand it to the suite, and assert the verdict:
//!
//! ```
//! use rdlt_testkit::conformance::{self, assert_conformant};
//! use rdlt_testkit::memory;
//! use serde_json::json;
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let source = memory::Source::new(vec![memory::Stream::new(
//!     rdlt_connector::source::StreamSpec::new("events"),
//!     vec![
//!         memory::Batch::new(vec![json!({"id": 1}), json!({"id": 2})]).with_checkpoint(1),
//!         memory::Batch::new(vec![json!({"id": 3})]).with_checkpoint(2),
//!         memory::Batch::new(vec![json!({"id": 4})]).with_checkpoint(3),
//!     ],
//! )]);
//! assert_conformant(conformance::source::verify(&source).await.expecting_no_skips());
//! # });
//! ```
//!
//! Destinations certify through [`conformance::destination::verify`]
//! with a [`conformance::destination::TableProbe`] the author implements
//! so the suite can read back what a warehouse query would see.
//!
//! - [`conformance`] — the suites, their verdict, and the assert entry.
//! - [`memory`] — the in-memory source/destination pair.
//! - [`fixtures`] — the canonical schema, batch, and commit envelope.
//! - [`gate`] — the container-runtime probe and skip-not-fail posture.
//! - [`spawn`] — locating a connector crate's built binary.
//! - [`scanner`] — the crash-point registry scanner.

pub mod conformance;
pub mod fixtures;
pub mod gate;
pub mod memory;
pub mod scanner;
pub mod spawn;

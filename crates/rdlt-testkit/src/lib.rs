//! # rdlt-testkit — memory connectors, conformance suites, crash harness
//!
//! Public and shipped: connector authors certify against the conformance suites here
//! ("certified = passes conformance"). Depends on the SPI only — if
//! something in here needs engine internals, the SPI is wrong; raise it.
//! Connector-agnostic by the same rule: system-specific fixtures (a postgres
//! container, a credential convention) live with their connectors; this crate
//! carries only what every connector shares.
//!
//! The primary workflow is certification, and it runs anywhere — no network,
//! no containers, no credentials. Build your connector (here: the bundled
//! memory source), hand it to the suite, and assert the verdict:
//!
//! ```
//! use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream, assert_conformant};
//! use rdlt_testkit::conformance::source::verify_source;
//! use serde_json::json;
//!
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let source = MemorySource::new(vec![MemoryStream::new(
//!     rdlt_connector::StreamSpec::new("events"),
//!     vec![
//!         MemoryBatch::new(vec![json!({"id": 1}), json!({"id": 2})]).with_checkpoint(1),
//!         MemoryBatch::new(vec![json!({"id": 3})]).with_checkpoint(2),
//!         MemoryBatch::new(vec![json!({"id": 4})]).with_checkpoint(3),
//!     ],
//! )]);
//! assert_conformant(verify_source(&source).await);
//! # });
//! ```
//!
//! Destinations certify the same way through [`verify_destination`], with a
//! [`TableProbe`] you implement so the suite can read back what a warehouse
//! query would see. Around certification sit the rest of the kit:
//! [`CrashDestination`] injects a deterministic fault at a chosen
//! [`FaultPoint`] to prove crash recovery; [`assert_registry_matches_sources`]
//! keeps a crash-point sweep honest against the sources it sweeps; and
//! [`gate`] holds the one container-runtime probe and the skip-not-fail /
//! demand-and-fail posture every resource-gated suite shares.

pub mod conformance;
pub mod crash;
pub mod fixtures;
pub mod gate;
pub mod memory;

pub use conformance::{
    ConformanceFailure, assert_conformant, dest::TableProbe, dest::verify_destination,
    source::verify_source,
};
pub use crash::{
    CrashDestination, FaultPoint, armed_crash_points, assert_registry_matches_sources,
};
pub use fixtures::{batch_of, commit_meta_for, schema_for};
pub use memory::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream, Row};

//! # rdlt-testkit — memory connectors, conformance suites, crash harness
//!
//! Public and shipped: connector authors certify against the conformance suites here
//! ("certified = passes conformance"). Depends on the SPI only — if
//! something in here needs engine internals, the SPI is wrong; raise it.
//! Connector-agnostic by the same rule: system-specific fixtures (a postgres
//! container, a credential convention) live with their connectors; this crate
//! carries only what every connector shares.

pub mod conformance;
pub mod crash;
pub mod fixtures;
pub mod gate;
pub mod memory;

pub use conformance::{
    ConformanceFailure, assert_conformant, dest::TableProbe, dest::verify_destination,
    source::verify_source,
};
pub use crash::{CrashDestination, FaultPoint, assert_registry_is_armed, scan_arming_sites};
pub use fixtures::{batch_of, meta_for, schema_for};
pub use memory::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream, Row};

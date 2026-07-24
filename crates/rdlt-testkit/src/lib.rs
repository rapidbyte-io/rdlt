//! # rdlt-testkit — memory connectors, conformance suites, crash harness
//!
//! Public and shipped: connector authors certify against the conformance suites here
//! ("certified = passes conformance"). Depends on the SPI only — if
//! something in here needs engine internals, the SPI is wrong; raise it.

pub mod conformance;
#[cfg(feature = "containers")]
pub mod containers;
pub mod crash;
pub mod fixtures;
pub mod memory;
pub mod util;

pub use conformance::{ConformanceFailure, assert_conformant, dest::TableProbe};
#[cfg(feature = "containers")]
pub use containers::{CdcPgFixture, PgFixture, runtime_available};
pub use crash::{FaultPoint, FlakyDestination};
pub use fixtures::{batch_of, meta_for, schema_for};
pub use memory::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};

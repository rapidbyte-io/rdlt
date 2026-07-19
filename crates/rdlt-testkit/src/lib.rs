//! # rdlt-testkit — memory connectors, conformance suites, crash harness
//!
//! Public and shipped: connector authors certify against the conformance suites here
//! ("certified = passes conformance", spec FR-016). Depends on the SPI only — if
//! something in here needs engine internals, the SPI is wrong; raise it.

pub mod conformance;
pub mod crash;
pub mod memory;
pub mod util;

pub use conformance::{ConformanceFailure, assert_conformant, dest::TableProbe};
pub use crash::{FaultPoint, FlakyDestination};
pub use memory::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};

//! Crash tooling: fault injection for runs, and the registry scanner that
//! keeps crash-point sweeps honest.

mod fault;
mod registry;

pub use fault::{CrashDestination, FaultPoint};
pub use registry::{armed_crash_points, assert_registry_matches_sources};

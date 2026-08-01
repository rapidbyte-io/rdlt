//! Crash tooling: fault injection for runs, and the registry scanner that
//! keeps crash-point sweeps honest.

mod injector;
mod registry;

pub use injector::{CrashDestination, FaultPoint};
pub use registry::{armed_crash_points, assert_registry_matches_sources};

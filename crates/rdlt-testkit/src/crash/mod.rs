//! Crash tooling: fault injection for runs, and the registry scanner that
//! keeps crash-point sweeps honest.

mod fault;
mod registry;

pub use fault::{CrashDestination, FaultPoint};
pub use registry::{assert_registry_is_armed, scan_arming_sites};

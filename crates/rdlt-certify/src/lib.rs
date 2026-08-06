//! # rdlt-certify — the standalone connector certifier (ADR 0001 D8)
//!
//! "Certified = passes conformance", for OUT-OF-PROCESS connectors: the
//! testkit's clause suites certify an SPI object in-process; this crate
//! spawns a connector BINARY, drives it over the wire through
//! [`rdlt_runtime`]'s provider, and folds the same S/D clauses plus the
//! protocol-level P clauses into one [`Report`] whose render spellings
//! are the certifier CLI's stdout contract.
//!
//! The certification bar: every clause is timeout-bounded — a connector
//! that stalls FAILS the clause, the certifier never hangs; failures are
//! actionable ("violates P1: ..."); reports name clauses, never config
//! bytes.
//!
//! The modules are private and the surface below is the one canonical
//! path to every name.

mod destination;
mod report;
#[cfg(test)]
mod rogue;
mod source;
mod target;
mod wire;

pub use destination::certify_destination;
pub use report::{Entry, Report, Verdict};
pub use source::certify_source;
pub use target::Target;

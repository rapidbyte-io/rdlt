//! # rdlt-certify — the wire-side connector certifier
//!
//! "Certified = passes conformance", for OUT-OF-PROCESS connectors:
//! the testkit's clause suites certify an SPI object in-process; this
//! crate spawns a connector BINARY, drives it over the wire, and
//! certifies it clause by clause into one [`report::Report`] whose
//! render spellings are the certifier CLI's stdout contract. It
//! re-derives no in-process suite (the S and D families reuse the
//! testkit's against the managed adapters), owns every protocol (P)
//! and kill (K) probe, and its every clause is timeout-bounded — a
//! connector that stalls FAILS the clause, the certifier never hangs;
//! failures are actionable (`FAIL P1 (<title>): <why>`); reports name
//! clauses, never config bytes.
//!
//! The modules are the map: [`clause`] holds the five clause families
//! (`s`, `d`, `p`, `k`, `c`) over the substrate — [`report`] the vocabulary
//! and renders, [`target`] what a session points at and how it spawns,
//! `wire` the raw-frame probe below the client adapters, `clock` the
//! probe-time-excluded clause budget — with [`probe`] the shell-line
//! table probe the CLI offers and [`contract`] the pins every served
//! connector bin answers to. Every name is reached by its module path;
//! nothing is re-exported at the root.

pub mod clause;
pub(crate) mod clock;
pub mod contract;
pub mod probe;
pub mod report;
#[cfg(test)]
mod rogue;
pub mod target;
pub(crate) mod wire;

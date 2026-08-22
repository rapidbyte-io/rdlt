//! # rdlt-core — the rdlt vocabulary
//!
//! The types every side of an rdlt pipeline names: what is persisted in a
//! destination ([`state`], [`schema`]), what crosses the connector wire
//! ([`commit`], [`cursor`], [`id`], [`types`]), what a run reports back
//! ([`report`], [`event`], [`metrics`], [`error`]), and the one test-only
//! macro production code arms ([`failpoint`]).
//!
//! **Charter: vocabulary that crosses a boundary, and nothing else.** A type
//! lives here because more than one side of a boundary must agree on it —
//! engine and connector, engine and embedder, this engine version and the
//! documents an older one wrote. Machinery with a single owner (row identity,
//! collision-safe naming, schema policy) is engine code and lives in the
//! engine, however pure it is. Dependencies stay narrow — serde/serde_json,
//! blake3, thiserror, and an optional `fail` used only under the `failpoints`
//! feature — and deliberately NOT arrow: the schema vocabulary is rdlt's own,
//! and mapping it onto arrow types is the engine's job.
//!
//! Every serde form here is a persisted or wire format and is byte-stable:
//! field names, tags and renames change only through an explicit, versioned
//! format migration, never incidentally. `cargo semver-checks` gates the crate.
//!
//! Every module path is canonical — nothing is re-exported at the root.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

pub mod commit;
pub mod cursor;
pub mod error;
pub mod event;
pub mod failpoint;
pub mod fs;
pub mod id;
pub mod inventory;
pub mod metrics;
pub mod report;
pub mod schema;
pub mod state;
pub mod types;

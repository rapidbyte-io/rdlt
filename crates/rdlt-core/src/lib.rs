//! # rdlt-core — the rdlt vocabulary
//!
//! Pure data contracts and semantic laws for the rdlt engine and the rapidbyte
//! platform: every type that is persisted, reported, or crosses a wire, plus the pure
//! functions that define rdlt's data semantics (widening lattice, row identity, schema
//! hashing, naming).
//!
//! **Charter**: pure data + pure functions only. If it needs tokio, I/O, or arrow
//! compute, it does not belong here. Dependencies stay narrow — serde/serde_json for
//! serialization, blake3 for hashing, thiserror for the error taxonomy, and an optional
//! `fail` dependency used only for crash-point injection in tests (the `failpoints`
//! feature, never in release builds). Notably NOT arrow: the schema vocabulary here is
//! rdlt's own, and mapping it onto arrow types is the engine's job, not this crate's.
//!
//! This crate is SEMVER-SACRED: `cargo semver-checks` gates it in CI, and the persisted
//! formats it defines (`StateDoc`, schema hashes, `RunReport`) are byte-stable — their
//! on-disk serialization is a compatibility contract that changes only through an
//! explicit, versioned format migration, never incidentally.

// Warn, not deny: an undocumented public item is a gap to fill, not a
// reason to fail a contributor's build. `make docs` is where the
// published surface is held to -D warnings.
#![warn(missing_docs)]

pub mod commit;
pub mod cursor;
pub mod error;
pub mod event;
pub mod failpoint;
pub mod identity;
pub mod ids;
pub mod naming;
pub mod policy;
pub mod report;
pub mod schema;
pub mod state;
pub mod types;

pub use commit::{CommitCounters, CommitMeta, CommitPolicy, CommitReceipt, WriteMode};
pub use cursor::Cursor;
pub use error::{ContractViolation, RdltError};
pub use event::PipelineEvent;
pub use ids::{InvalidHexId, LoadId, PipelineId, RowId, SchemaHash, StreamName, TableName};
pub use policy::{ColumnRef, PolicyAction, SchemaPolicy};
pub use report::{ResumedFrom, RunReport, TableReport};
pub use schema::{
    ColumnDef, ColumnType, ParentLink, Provenance, SchemaChange, SchemaDelta, TableSchema,
};
pub use state::{LastCommit, StateDoc, UnsupportedStateVersion};
pub use types::{LogicalType, is_widening_of, widen};

//! The SQL-destination protocol naming vocabulary — the `_rdlt_` naming
//! conventions for persisted state, defined ONCE so no destination
//! hand-types a prefix.
//!
//! These names are PERSISTED-FORMAT IDENTITY, not configuration: `read_state`
//! must find the table a previous run wrote, so a runtime-configurable prefix
//! would orphan state (cursor loss → double loading) the moment it changed.
//! A product-wide rename (e.g. platform branding) is a deliberate one-time
//! compile-time decision made HERE, before production data exists — never a
//! per-pipeline option. Tests deliberately keep literal spellings: they pin
//! the on-disk contract and catch an accidental rename.
//!
//! (System COLUMN names — `_rdlt_id` etc. — live in
//! `rdlt_core::schema::system_columns`: the engine writes those. scd2
//! validity defaults live in [`crate::options`] and ARE user-overridable —
//! they name user-visible data columns, not protocol state.)

/// State document table — state commits atomically with the data it describes.
pub const STATE_TABLE: &str = "_rdlt_state";

/// Commit receipt table — idempotence key is (load_id, commit_seq).
pub const COMMITS_TABLE: &str = "_rdlt_commits";

/// Stage table name prefix; destinations append their own scoping/hash
/// suffixes.
pub const STAGE_PREFIX: &str = "_rdlt_stage_";

/// Merge-identity index prefixes (auto-ensured per load): plain and unique.
pub const INDEX_PREFIX: &str = "rdlt_ix";
pub const UNIQUE_INDEX_PREFIX: &str = "rdlt_ux";

//! The shared merge planning: survivor selection, scope replacement, strategy
//! [`arms`], hard-delete decisions, open-time [`validate`]ation, [`index`]
//! plans, and the shared plumbing (root walk, column list, per-table merge
//! resolution) below. Every statement's text is produced through the
//! [`MergeDialect`](crate::dialect::MergeDialect) seam; the postgres crate's
//! golden-SQL suite pins that text byte-for-byte, so a change that alters
//! emitted SQL is caught there.

pub mod arms;
pub mod index;
mod table;
pub mod validate;

pub use arms::{
    HardDelete, MergePlan, identity_delete_insert_sql, keyed_delete_insert_sql, keyed_upsert_sql,
    scd2_merge_sql, scope_replace_sql, single_unit_violation,
};
pub use index::{IndexSpec, index_plan};
pub use table::{MergeCtx, ROOT_DEPTH_BOUND, column_list, column_list_with, root_of};
pub use validate::{TableFacts, ValidateError, validate_merge, validate_non_merge};

//! # rdlt-connector-sqlcore — the shared merge-planning core
//!
//! ONE core for every SQL destination: the destination options vocabulary +
//! validation ([`options`]), the plan shapes — dedup/survivor ordering, scope
//! replacement, strategy arms, hard-delete decisions, index plans ([`plan`])
//! — and the [`dialect`] seam through which destinations own SQL TEXT and
//! nothing else.
//!
//! The plan shapes are shared by the postgres and DuckDB destinations; the
//! postgres crate's golden-SQL suite pins them byte-for-byte, so any change
//! here that alters emitted SQL is caught there.

pub mod dialect;
pub mod names;
pub mod options;
pub mod plan;

pub use dialect::MergeDialect;
pub use options::{
    AbsentPolicy, DedupSort, DestOptions, MergeStrategy, Scd2Options, SortOrder, TableOptions,
};
pub use plan::{HardDelete, MergePlan};

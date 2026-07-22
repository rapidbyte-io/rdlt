//! # rdlt-connector-sqlcore — the shared merge-planning core (feature 013)
//!
//! ONE core for every SQL destination (contract shared-merge-core.md SM1):
//! the destination options vocabulary + validation ([`options`]), the plan
//! shapes — dedup/survivor ordering, scope replacement, strategy arms,
//! hard-delete decisions, index plans ([`plan`]) — and the [`dialect`] seam
//! through which destinations own SQL TEXT and nothing else (SM2).
//!
//! History: extracted verbatim from the postgres destination (features
//! 006/008/010/011); the extraction is pinned byte-for-byte by that crate's
//! golden-SQL suite (SM4). The DuckDB destination is the second consumer.

pub mod dialect;
pub mod names;
pub mod options;
pub mod plan;

pub use dialect::MergeDialect;
pub use options::{
    AbsentPolicy, DedupSort, DestOptions, MergeStrategy, Scd2Options, SortOrder, TableOptions,
};
pub use plan::{HardDelete, MergePlan};

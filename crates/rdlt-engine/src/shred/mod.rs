//! The shredder: raw JSON → typed, lineage-stamped Arrow batches, under the
//! schema-change policy.
//!
//! The tape path (`tape`: slab arena, no per-row trees) feeds
//! [`drain::drain_tables`] — the generic resolve/policy/build pipeline over the
//! [`view::JsonView`] seam. The seam stays generic even with one production
//! path: the `&serde_json::Value` view backs the unit tests, and everything
//! semantics-bearing remains representation-independent by construction.

pub(crate) mod arena;
pub(crate) mod build;
pub(crate) mod canon;
mod drain;
mod infer;
pub(crate) mod passthrough;
mod table;
pub(crate) mod tape;
pub(crate) mod view;

pub(crate) use drain::{DrainRow, ShredContext, drain_tables};
pub(crate) use tape::TapeShredder;

/// Cumulative source columns retained for one logical table. System columns
/// sit outside this count.
pub(crate) const MAX_SOURCE_COLUMNS_PER_TABLE: usize = 4096;

/// Distinct child-table source keys retained beneath one parent table.
pub(crate) const MAX_CHILD_TABLES_PER_PARENT: usize = 1024;

/// Total tables (root plus every discovered child, at any depth) retained for
/// one stream. The per-parent cap alone leaves the TOTAL unbounded — nesting
/// multiplies parents — and every push does per-table bookkeeping, so
/// unbounded tables turn one crafted frame into unbounded work and memory.
pub(crate) const MAX_TABLES_PER_STREAM: usize = 64 * 1024;

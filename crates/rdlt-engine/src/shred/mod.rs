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
pub(crate) mod infer;
pub(crate) mod passthrough;
pub(crate) mod table;
pub(crate) mod tape;
pub(crate) mod view;

pub(crate) use drain::{DrainRow, ShredContext, drain_tables};
pub(crate) use tape::TapeShredder;

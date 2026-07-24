//! Destination capabilities — data, not traits.
//!
//! The engine plans around these at build time: lowering strategy (struct passthrough
//! vs flatten), merge validation (fail-fast if requested but unsupported), identifier
//! normalization. Declaring them truthfully is a conformance obligation.

use rdlt_core::naming::IdentRules;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCapabilities {
    /// Supports keyed merge (upsert + subtree replacement by `_rdlt_root_id`).
    pub merge: bool,
    /// Native struct/nested columns; if false the engine flattens collision-safely.
    pub structs: bool,
    /// Native list-of-scalars columns; if false those become child tables.
    pub scalar_lists: bool,
    /// Has a JSON/JSONB type; if false `Json` columns lower to text.
    pub json_type: bool,
    /// Native fixed-point decimal; if false decimals lower to text.
    pub decimal: bool,
    pub ident_rules: IdentRules,
}

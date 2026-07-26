//! Destination capabilities — data, not traits.
//!
//! The engine plans around these at build time: lowering strategy (struct passthrough
//! vs flatten), merge validation (fail-fast if requested but unsupported), identifier
//! normalization. Declaring them truthfully is a conformance obligation.

use rdlt_core::naming::IdentRules;
use serde::{Deserialize, Serialize};

/// What a destination can do natively.
///
/// The engine plans from this at BUILD time and does not re-verify at runtime,
/// so an untruthful declaration is not caught by a nicer error later — it is
/// caught by the destination failing mid-load, or worse, by data arriving in a
/// shape nobody intended. Declaring these honestly is a conformance obligation.
///
/// `Default` is all-false plus default identifier rules: the most conservative
/// destination possible, which the engine can always lower for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCapabilities {
    /// Supports keyed merge (upsert + subtree replacement by `_rdlt_root_id`).
    pub merge: bool,
    /// Native struct/nested columns; if false the engine flattens collision-safely.
    pub structs: bool,
    /// Native list-of-scalars columns; if false those become child tables.
    pub scalar_lists: bool,
    /// Has a native JSON/JSONB type.
    ///
    /// Unlike `structs` and `decimal`, this flag does NOT drive engine
    /// lowering — the engine never rewrites a `Json` column. A destination
    /// that declares `false` is stating that it maps `Json` itself, in its own
    /// schema translation, and is responsible for doing so: the Iceberg
    /// destination maps it to string (format v2 has no JSON type) and the file
    /// destination writes it in the output format's own encoding.
    pub json_type: bool,
    /// Native fixed-point decimal; if false decimals lower to text.
    pub decimal: bool,
    /// Identifier limits and case-folding, used to normalize table and column
    /// names before any DDL is generated. Getting this wrong produces names the
    /// destination silently truncates or folds into collisions.
    pub ident_rules: IdentRules,
}

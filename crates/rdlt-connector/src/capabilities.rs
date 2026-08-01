//! Destination capabilities — data, not traits.
//!
//! The host plans around these at build time: lowering strategy (struct
//! passthrough vs flatten), merge validation (fail fast when requested
//! but unsupported), identifier normalization. Declaring them truthfully
//! is a conformance obligation.

use rdlt_core::naming::IdentRules;
use serde::{Deserialize, Serialize};

/// What a destination can do natively.
///
/// The host plans from this at BUILD time and does not re-verify at
/// runtime, so an untruthful declaration is not caught by a nicer error
/// later — it is caught by the destination failing mid-load, or worse, by
/// data arriving in a shape nobody intended.
///
/// `Default` is the most conservative destination possible — everything
/// false, default identifier rules — which the host can always lower for.
/// `#[non_exhaustive]`, so construction is `Default` plus the `with_*`
/// declarations: a future capability then arrives as one new method with
/// a conservative default, not as a breaking change for every
/// out-of-tree destination.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DestinationCapabilities {
    /// Supports keyed merge (upsert + subtree replacement by
    /// `_rdlt_root_id`).
    pub merge: bool,
    /// Native struct/nested columns; if false the host flattens
    /// collision-safely.
    pub structs: bool,
    /// Native list-of-scalars columns; if false those become child
    /// tables.
    pub scalar_lists: bool,
    /// Has a native JSON/JSONB type.
    ///
    /// Unlike `structs` and `decimal`, this flag does NOT drive host
    /// lowering — the host never rewrites a `Json` column. A destination
    /// declaring `false` states that it maps `Json` itself, in its own
    /// schema translation, and owns doing so (a lakehouse format without
    /// a JSON type maps it to string; a file format writes its own
    /// encoding).
    pub json_type: bool,
    /// Native fixed-point decimal; if false decimals lower to text.
    pub decimal: bool,
    /// Identifier limits and case-folding, used to normalize table and
    /// column names before any DDL is generated. Wrong rules produce
    /// names the destination silently truncates or folds into
    /// collisions.
    pub ident_rules: IdentRules,
}

impl DestinationCapabilities {
    /// Declare keyed-merge support.
    #[must_use]
    pub fn with_merge(mut self, merge: bool) -> Self {
        self.merge = merge;
        self
    }

    /// Declare native struct/nested-column support.
    #[must_use]
    pub fn with_structs(mut self, structs: bool) -> Self {
        self.structs = structs;
        self
    }

    /// Declare native list-of-scalars support.
    #[must_use]
    pub fn with_scalar_lists(mut self, scalar_lists: bool) -> Self {
        self.scalar_lists = scalar_lists;
        self
    }

    /// Declare a native JSON/JSONB type (see the field's caveat: this
    /// flag does not drive host lowering).
    #[must_use]
    pub fn with_json_type(mut self, json_type: bool) -> Self {
        self.json_type = json_type;
        self
    }

    /// Declare native fixed-point decimal support.
    #[must_use]
    pub fn with_decimal(mut self, decimal: bool) -> Self {
        self.decimal = decimal;
        self
    }

    /// Declare the destination's identifier limits and case-folding.
    #[must_use]
    pub fn with_ident_rules(mut self, ident_rules: IdentRules) -> Self {
        self.ident_rules = ident_rules;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Default` is the shape the host can always lower for, and each
    /// builder moves exactly one declaration.
    #[test]
    fn default_is_conservative_and_each_builder_declares_one_thing() {
        let conservative = DestinationCapabilities::default();
        assert!(!conservative.merge);
        assert!(!conservative.structs);
        assert!(!conservative.scalar_lists);
        assert!(!conservative.json_type);
        assert!(!conservative.decimal);

        let declared = DestinationCapabilities::default()
            .with_merge(true)
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(true)
            .with_decimal(true);
        assert!(
            declared.merge
                && declared.structs
                && declared.scalar_lists
                && declared.json_type
                && declared.decimal
        );
        assert_eq!(declared.ident_rules, IdentRules::default());
    }

    /// The declaration is data a platform can store and ship.
    #[test]
    fn the_declaration_round_trips_through_serde() {
        let declared = DestinationCapabilities::default()
            .with_merge(true)
            .with_decimal(true);
        let wire = serde_json::to_value(declared).expect("serializes");
        assert_eq!(wire["merge"], true);
        assert_eq!(wire["structs"], false);
        let back: DestinationCapabilities = serde_json::from_value(wire).expect("round-trips");
        assert_eq!(back, declared);
    }
}

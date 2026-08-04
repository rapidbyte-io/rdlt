//! Build-time validation over the discovered streams, before any session opens.

use std::collections::BTreeMap;

use rdlt_connector::{Destination, DestinationCapabilities, StreamSpec};
use rdlt_core::{
    LogicalType, RdltError, StreamName, TableName, WriteMode, naming::normalize_ident,
};

use crate::EngineConfig;

/// The destination table a stream owns, by name normalization.
///
/// One decision with two consumers that must agree by construction: validation
/// asserts the mapping is injective, and the run wiring builds each stream's
/// shredder against the same mapping.
pub(super) fn root_table(stream: &StreamName, rules: rdlt_core::naming::IdentRules) -> TableName {
    TableName::new(normalize_ident(stream.as_str(), rules))
}

/// Build-time validation over the discovered streams: one owning stream per
/// destination table (two streams writing one table would interleave
/// unowned rows), Merge only where the destination supports it,
/// and structured Merge only against a declared primary key. Fails before any
/// session is opened.
pub(super) fn validate_streams(
    config: &EngineConfig,
    streams: &[StreamSpec],
    capabilities: DestinationCapabilities,
    destination: &dyn Destination,
) -> Result<(), RdltError> {
    let mut root_tables: BTreeMap<TableName, StreamName> = BTreeMap::new();
    for spec in streams {
        let table = root_table(&spec.name, capabilities.ident_rules);
        if let Some(owner) = root_tables.insert(table.clone(), spec.name.clone()) {
            // Clause E2: exactly one stream owns a table.
            return Err(RdltError::config(format!(
                "streams `{owner}` and `{}` both map to table `{table}`",
                spec.name
            )));
        }
        // Child tables are minted at shred time as
        // `{root}__{field}` — a root containing `__` sits inside
        // some stream's child namespace, and two streams writing one
        // physical table is silent corruption (029's headliner). The
        // rule is static and complete: distinct `__`-free roots make
        // every root/child and child/child pair disjoint.
        if table.as_str().contains("__") {
            return Err(RdltError::config(format!(
                "stream `{}` normalizes to table `{table}`, which collides with \
                 the child-table namespace (`__` separates parent from child); \
                 rename the stream so its table carries no `__`",
                spec.name
            )));
        }
        if matches!(config.write_mode_for(&spec.name), WriteMode::Merge { .. })
            && !capabilities.merge
        {
            return Err(RdltError::config(format!(
                "stream `{}` requests Merge but destination `{}` does not support it",
                spec.name,
                destination.spec().name
            )));
        }
        // A hint pins a column's type outright, bypassing the lattice that
        // guarantees every inferred decimal is representable. An unrepresentable
        // hint must therefore be refused HERE — the batch builder cannot, and
        // reaching it with one is a panic.
        for (column, hint) in &spec.type_hints {
            if let LogicalType::Decimal { precision, scale } = hint {
                if *precision == 0 || *precision > rdlt_core::types::DECIMAL_MAX_PRECISION {
                    return Err(RdltError::config(format!(
                        "stream `{}` column `{column}`: decimal precision {precision} is out of \
                         range (1..={})",
                        spec.name,
                        rdlt_core::types::DECIMAL_MAX_PRECISION
                    )));
                }
                if scale > precision {
                    return Err(RdltError::config(format!(
                        "stream `{}` column `{column}`: decimal scale {scale} exceeds its \
                         precision {precision}",
                        spec.name
                    )));
                }
            }
        }
        // Structured streams merge ONLY by a declared key — accepted iff the
        // stream declares a non-empty primary_key AND Merge{key} names exactly
        // that key (the destination's merge capability was checked above).
        // Keyless structured streams keep the original rejection.
        if spec.structured
            && let WriteMode::Merge { key } = config.write_mode_for(&spec.name)
        {
            let declared = spec.primary_key.clone().unwrap_or_default();
            if declared.is_empty() {
                return Err(RdltError::config(format!(
                    "stream `{}` is structured with no declared primary_key and \
                     cannot use Merge; declare a key on the \
                     stream and set Merge {{ key }} to it, or use Append/Replace",
                    spec.name
                )));
            }
            // Order-insensitive: the key is a SET (reflection returns
            // attnum order, users write DDL order).
            let mut key_set = key.clone();
            key_set.sort_unstable();
            let mut declared_set = declared.clone();
            declared_set.sort_unstable();
            if key_set != declared_set {
                return Err(RdltError::config(format!(
                    "stream `{}`: Merge key {:?} must name exactly the stream's \
                     declared primary_key columns {:?} (order does not matter)",
                    spec.name, key, declared
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod hint_validation_tests {
    //! The decimal type-hint bounds, at their EDGES.
    //!
    //! These are refused here or nowhere: the batch builder cannot check a
    //! precision it was handed, and reaching it with an out-of-range one is a
    //! panic. Both comparisons are strict for a reason — `precision == MAX` is
    //! the largest decimal the engine represents and must be ACCEPTED, and
    //! `scale == precision` is an ordinary all-fractional decimal (0.99 at
    //! precision 2, scale 2), not an error. Every off-by-one here rejects a
    //! legitimate configuration at plan time, which is a refusal the operator
    //! cannot work around.
    use super::*;
    use rdlt_testkit::MemoryDestination;

    fn check(precision: u8, scale: u8) -> Result<(), RdltError> {
        let spec = StreamSpec::new("s")
            .with_type_hint("amount", LogicalType::Decimal { precision, scale });
        let dest = MemoryDestination::new();
        validate_streams(
            &EngineConfig::new("hints"),
            std::slice::from_ref(&spec),
            dest.capabilities(),
            &dest,
        )
    }

    #[test]
    fn decimal_hint_bounds_are_inclusive_at_their_edges() {
        let max = rdlt_core::types::DECIMAL_MAX_PRECISION;

        // The largest representable decimal is legal, as is an all-fractional
        // one, as is the smallest.
        assert!(check(max, 0).is_ok(), "precision == MAX must be accepted");
        assert!(check(max, max).is_ok(), "scale == precision is 0.999…");
        assert!(check(1, 1).is_ok(), "the smallest all-fractional decimal");
        assert!(check(10, 2).is_ok(), "an ordinary decimal");

        // And the genuinely out-of-range cases stay refused.
        assert!(check(0, 0).is_err(), "precision 0 has no digits");
        assert!(check(max + 1, 0).is_err(), "beyond 128-bit precision");
        assert!(check(5, 6).is_err(), "scale exceeding precision");
    }

    fn check_streams(names: &[&str]) -> Result<(), RdltError> {
        let specs: Vec<_> = names.iter().map(|&name| StreamSpec::new(name)).collect();
        let dest = MemoryDestination::new();
        validate_streams(
            &EngineConfig::new("streams"),
            &specs,
            dest.capabilities(),
            &dest,
        )
    }

    #[test]
    fn two_streams_normalizing_to_one_root_table_are_refused() {
        // `Users` and `users` both normalize to root table `users`.
        let error = check_streams(&["Users", "users"]).expect_err("E2: one stream owns a table");
        assert!(
            matches!(error, RdltError::Config { .. }),
            "a root-table collision is a config refusal: {error:?}"
        );
        let text = error.to_string();
        assert!(text.contains("both map to table"), "{text}");
    }

    #[test]
    fn a_root_table_containing_the_child_separator_is_refused() {
        // `users..emails` normalizes to `users__emails` (each `.` maps to a
        // single `_`) — the exact name the `users` stream's `emails`
        // list-of-objects child would get.
        let error = check_streams(&["users..emails", "users"])
            .expect_err("a root inside another stream's child namespace");
        let text = error.to_string();
        assert!(
            text.contains("collides with the child-table namespace"),
            "{text}"
        );
    }

    #[test]
    fn the_separator_rule_applies_even_to_a_single_stream() {
        // One stream today, a second tomorrow: the rule must not depend
        // on who else is currently configured.
        assert!(check_streams(&["users__emails"]).is_err());
    }
}

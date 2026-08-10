//! Build-time validation over the discovered streams, before any session opens.

use std::collections::BTreeMap;

use rdlt_connector::{Destination, DestinationCapabilities, StreamSpec};
use rdlt_core::{
    LogicalType, RdltError, StreamName, TableName, WriteMode, naming::normalize_ident,
};

use crate::EngineConfig;

/// Rule 1: `a`'s table plus a trailing `_` equals `b`'s table — a
/// `_`-leading source field mints the same child table under either root.
/// `a` is always the stream owning the shorter (prefix) table.
fn trailing_underscore_collision(a: &StreamName, ta: &str, b: &StreamName, tb: &str) -> RdltError {
    RdltError::config(format!(
        "streams `{a}` and `{b}` normalize to tables `{ta}` and `{tb}`, which differ only by \
         a trailing `_` — a `_`-leading source field would mint the same child table for both; \
         rename one stream"
    ))
}

/// Rule 2: `b`'s table sits inside `a`'s child namespace (`b` starts with
/// `a` + `__`). `a` is always the stream owning the shorter (prefix) table.
fn child_namespace_collision(a: &StreamName, ta: &str, b: &StreamName, tb: &str) -> RdltError {
    RdltError::config(format!(
        "streams `{a}` and `{b}` normalize to tables `{ta}` and `{tb}`, and `{tb}` sits inside \
         `{ta}`'s child-table namespace (`__` separates parent from child); rename one stream \
         so neither table extends the other"
    ))
}

/// The destination table a stream owns, by name normalization.
///
/// One decision with two consumers that must agree by construction: validation
/// asserts the mapping is injective, and the run wiring builds each stream's
/// shredder against the same mapping.
pub(super) fn root_table(stream: &StreamName, rules: rdlt_core::naming::IdentRules) -> TableName {
    TableName::new(normalize_ident(stream.as_str(), rules))
}

/// The mixed snapshot/cursored advisory: a stream declaring no
/// `cursor_field` never checkpoints, so once it writes rows its root
/// stays uncovered for the REST OF THE RUN (nothing but its own
/// checkpoint removes it) and every mid-run commit trigger defers to
/// the final commit — the loader's T7E coverage gate, correct and
/// deliberate. Worth telling the operator at PLAN time, before a long
/// run discovers it at the first deferred trigger: commit policies
/// cannot bound staging/WAL growth in this shape. `Some(advisory)`
/// exactly when the stream set mixes both kinds; pure so the pin
/// beside it can hold the trigger condition without a subscriber.
fn mixed_snapshot_advisory(streams: &[StreamSpec]) -> Option<String> {
    let snapshot: Vec<&str> = streams
        .iter()
        .filter(|s| s.cursor_field.is_none())
        .map(|s| s.name.as_str())
        .collect();
    if snapshot.is_empty() || snapshot.len() == streams.len() {
        return None;
    }
    Some(format!(
        "streams [{}] declare no cursor_field (snapshot streams) beside cursored streams: \
         their rows are never covered by a checkpoint, so mid-run commits defer for the \
         whole run and everything publishes in the final commit — byte/time/checkpoint \
         commit policies cannot bound staging or WAL growth in this shape",
        snapshot.join(", ")
    ))
}

/// Build-time validation over the discovered streams: one owning stream per
/// destination table (two streams writing one table would interleave
/// unowned rows), Merge only where the destination supports it,
/// and structured Merge only against a declared primary key. Fails before any
/// session is opened. Also the home of the plan-time mixed
/// snapshot/cursored ADVISORY (a warning, never a refusal — the shape
/// is legal and the deferral is correct).
pub(super) fn validate_streams(
    config: &EngineConfig,
    streams: &[StreamSpec],
    capabilities: DestinationCapabilities,
    destination: &dyn Destination,
) -> Result<(), RdltError> {
    // Durable-identity destinations declare per-run, not per-stream; refuse
    // early so even a no-op run against such a destination fails cleanly.
    if capabilities.requires_durable_identity && config.workdir.is_none() {
        return Err(RdltError::config(format!(
            "destination `{}` publishes non-atomically and requires a workdir for \
             exactly-once crash recovery; set one with `workdir:` (the CLI defaults \
             to `.rdlt`) — without it a mid-publish failure re-appends committed rows",
            destination.spec().name
        )));
    }

    if let Some(advisory) = mixed_snapshot_advisory(streams) {
        tracing::warn!("{advisory}");
    }

    let mut root_tables: BTreeMap<TableName, StreamName> = BTreeMap::new();
    for spec in streams {
        let table = root_table(&spec.name, capabilities.ident_rules);
        // Child tables are minted at shred time as `{root}__{field}`. A
        // collision between two DISTINCT streams' table spaces needs their
        // roots A (shorter) and B (longer) to satisfy `B = A + "_" + s` for
        // some suffix `s`: if `s` is empty, B is just A's own table with a
        // trailing `_` and a `_`-leading source field mints the identical
        // child table under either (rule 1, `orders_`/`orders`); if `s`
        // starts with `_`, B already sits inside A's child namespace (rule
        // 2, `__` is A's separator plus that leading `_`); any other `s`
        // mismatches at the boundary character right after A and cannot
        // collide. So checking every pair for rules 1 and 2 (both
        // directions — the shorter root is not known in advance) makes
        // every pair of distinct roots' table spaces disjoint, without
        // refusing a table that merely LOOKS dangerous in isolation: a lone
        // root containing `__` or ending in `_` cannot collide with
        // itself, and postgres discovery mints exactly such roots from
        // hostile identifiers (`Order "Items"` -> `order__items_`) that the
        // operator does not own and cannot rename — refusing it outright
        // broke a pinned product capability (rdlt-connector-postgres's
        // `hostile_identifiers_and_column_selection` conformance cell).
        for (existing_table, existing_stream) in &root_tables {
            let et = existing_table.as_str();
            let nt = table.as_str();
            if nt == format!("{et}_") {
                return Err(trailing_underscore_collision(
                    existing_stream,
                    et,
                    &spec.name,
                    nt,
                ));
            }
            if et == format!("{nt}_") {
                return Err(trailing_underscore_collision(
                    &spec.name,
                    nt,
                    existing_stream,
                    et,
                ));
            }
            if nt.starts_with(&format!("{et}__")) {
                return Err(child_namespace_collision(
                    existing_stream,
                    et,
                    &spec.name,
                    nt,
                ));
            }
            if et.starts_with(&format!("{nt}__")) {
                return Err(child_namespace_collision(
                    &spec.name,
                    nt,
                    existing_stream,
                    et,
                ));
            }
        }
        if let Some(owner) = root_tables.insert(table.clone(), spec.name.clone()) {
            // Clause E2: exactly one stream owns a table.
            return Err(RdltError::config(format!(
                "streams `{owner}` and `{}` both map to table `{table}`",
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
    fn a_root_table_inside_another_streams_child_namespace_is_refused() {
        // `users..emails` normalizes to `users__emails` (each `.` maps to a
        // single `_`) — the exact name the `users` stream's `emails`
        // list-of-objects child would get. Refused because it is PAIRED
        // with the actual `users` stream; see the lone-stream capability
        // pin below for the same table name with nothing to collide
        // against.
        let error = check_streams(&["users..emails", "users"])
            .expect_err("a root inside another stream's child namespace");
        let text = error.to_string();
        assert!(
            text.contains("sits inside") && text.contains("child-table namespace"),
            "{text}"
        );
    }

    #[test]
    fn a_lone_root_containing_the_child_separator_is_accepted() {
        // The bare `__`-substring is not dangerous in isolation — a lone
        // root cannot collide with itself, and postgres table discovery
        // mints exactly this shape from hostile identifiers the operator
        // does not own and cannot rename (`rdlt-connector-postgres`'s
        // `hostile_identifiers_and_column_selection` conformance cell:
        // `Order "Items"` normalizes to `order__items_`). An earlier
        // version of this gate refused any `__`-containing root outright
        // and broke that pinned capability; only PAIRWISE ambiguity
        // between distinct streams is refused now.
        assert!(check_streams(&["users..emails"]).is_ok());
    }

    #[test]
    fn two_roots_differing_only_by_a_trailing_separator_are_refused() {
        // Both roots are legal in isolation — neither `orders_` nor
        // `orders` contains `__`. But together, a `_`-leading raw source
        // key (Mongo's `_id`) mints an identical child table from either:
        // `child_table_name("orders_", "id")` and
        // `child_table_name("orders", "_id")` both produce `orders___id`.
        let error = check_streams(&["orders_", "orders"])
            .expect_err("roots differing only by a trailing `_` collide via a `_`-leading field");
        let text = error.to_string();
        assert!(
            text.contains("differ only by") && text.contains("trailing `_`"),
            "{text}"
        );
    }

    #[test]
    fn a_lone_root_ending_in_the_separator_is_accepted() {
        // Same reasoning as the lone `__`-containing case above: `orders_`
        // cannot collide with itself when it is the only stream, and a
        // hostile source identifier can normalize to exactly this shape
        // too.
        assert!(check_streams(&["orders_"]).is_ok());
    }

    #[test]
    fn a_lone_root_normalizing_to_a_bare_separator_is_accepted() {
        // `?` (one character with no letter/digit/underscore mapping)
        // normalizes to the single character `_` — the degenerate case of
        // "ends with `_`", legal alone for the same reason as the other
        // lone-root pins: nothing exists to collide with.
        assert!(check_streams(&["?"]).is_ok());
    }

    fn no_workdir_config() -> EngineConfig {
        EngineConfig::new("test")
    }

    fn workdir_config() -> EngineConfig {
        EngineConfig::new("test").with_workdir("/tmp/rdlt-test")
    }

    fn durable_identity_dest() -> MemoryDestination {
        MemoryDestination::new().with_capabilities(
            DestinationCapabilities::default().with_requires_durable_identity(true),
        )
    }

    fn check_with(config: EngineConfig, destination: MemoryDestination) -> Result<(), RdltError> {
        let spec = StreamSpec::new("s");
        validate_streams(
            &config,
            std::slice::from_ref(&spec),
            destination.capabilities(),
            &destination,
        )
    }

    #[test]
    fn the_mixed_snapshot_advisory_fires_exactly_on_the_mixed_shape() {
        let snapshot = |name: &str| StreamSpec::new(name);
        let cursored = |name: &str| StreamSpec::new(name).with_cursor_field("updated_at");

        let advisory = mixed_snapshot_advisory(&[snapshot("orders"), cursored("events")])
            .expect("the mixed shape earns the advisory");
        assert!(
            advisory.contains("[orders]") && advisory.contains("mid-run commits defer"),
            "the advisory names the snapshot streams and the consequence: {advisory}"
        );

        assert!(
            mixed_snapshot_advisory(&[cursored("a"), cursored("b")]).is_none(),
            "all-cursored pipelines commit mid-run and need no warning"
        );
        assert!(
            mixed_snapshot_advisory(&[snapshot("a"), snapshot("b")]).is_none(),
            "an all-snapshot pipeline always published in one final commit — nothing changed"
        );
        assert!(mixed_snapshot_advisory(&[]).is_none());
    }

    #[test]
    fn a_durable_identity_destination_without_a_workdir_is_refused() {
        let error = check_with(no_workdir_config(), durable_identity_dest())
            .expect_err("N2: no workdir means duplication on mid-publish retry");
        let text = error.to_string();
        assert!(text.contains("requires a workdir"), "{text}");
        assert!(text.contains("workdir"), "names the fix: {text}");
    }

    #[test]
    fn the_same_destination_with_a_workdir_passes() {
        assert!(check_with(workdir_config(), durable_identity_dest()).is_ok());
    }

    #[test]
    fn empty_stream_list_with_durable_identity_destination_without_workdir_is_refused() {
        let dest = durable_identity_dest();
        let error = validate_streams(&no_workdir_config(), &[], dest.capabilities(), &dest)
            .expect_err("per-run check fires even with empty streams");
        let text = error.to_string();
        assert!(text.contains("requires a workdir"), "{text}");
    }
}

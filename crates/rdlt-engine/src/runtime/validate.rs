//! Build-time validation over the discovered streams, before any session opens.

use std::collections::BTreeMap;

use rdlt_connector::destination::{Capabilities, Destination};

use rdlt_connector::source::StreamSpec;
use rdlt_core::commit::WriteMode;
use rdlt_core::error::Error;
use rdlt_core::id::{StreamName, TableName};
use rdlt_core::types::LogicalType;

use crate::EngineConfig;

/// Rule 1: `a`'s table plus a trailing `_` equals `b`'s table — a
/// `_`-leading source field mints the same child table under either root.
/// `a` is always the stream owning the shorter (prefix) table.
fn trailing_underscore_collision(a: &StreamName, ta: &str, b: &StreamName, tb: &str) -> Error {
    Error::config(format!(
        "streams `{a}` and `{b}` normalize to tables `{ta}` and `{tb}`, which differ only by \
         a trailing `_` — a `_`-leading source field would mint the same child table for both; \
         rename one stream"
    ))
}

/// Rule 2: `b`'s table sits inside `a`'s child namespace (`b` starts with
/// `a` + `__`). `a` is always the stream owning the shorter (prefix) table.
fn child_namespace_collision(a: &StreamName, ta: &str, b: &StreamName, tb: &str) -> Error {
    Error::config(format!(
        "streams `{a}` and `{b}` normalize to tables `{ta}` and `{tb}`, and `{tb}` sits inside \
         `{ta}`'s child-table namespace (`__` separates parent from child); rename one stream \
         so neither table extends the other"
    ))
}

/// The destination table a stream owns — the crate's one attribution
/// mapping ([`crate::coverage::root_table`]), re-exported here because
/// validation is where the mapping is PROVEN injective before the run
/// wiring, the loader, and the recovery scan all build on it.
pub(super) use crate::coverage::root_table;

/// The mixed cursor-less/cursored advisory — CONDITIONAL truth, by
/// design (round-4 fix): a stream declaring no `cursor_field` MAY be a
/// snapshot stream that never checkpoints, but it may equally
/// checkpoint through another mechanism (postgres CDC streams declare
/// no cursor field yet checkpoint via LSN), and the declaration alone
/// cannot tell them apart at plan time. So the advisory says what is
/// conditionally true — IF such a stream never checkpoints, mid-run
/// commits defer for the whole run (the loader's T7E coverage gate) —
/// and defers the verdict to the loader's own run-time warning, which
/// fires on what actually checkpoints and is the authoritative signal.
/// `Some(advisory)` exactly when the stream set mixes both kinds; pure
/// so the pin beside it can hold the trigger condition without a
/// subscriber.
fn mixed_snapshot_advisory(streams: &[StreamSpec]) -> Option<String> {
    let cursorless: Vec<&str> = streams
        .iter()
        .filter(|s| s.cursor_field.is_none())
        .map(|s| s.name.as_str())
        .collect();
    // Fires for ANY multi-stream pipeline with a cursor-less member
    // (round-7 fix: the all-cursorless arm was suppressed, silencing
    // the CDC-beside-snapshot shape whose commits defer all run); a
    // single-stream pipeline stays silent — there is no co-stream to
    // defer against.
    if cursorless.is_empty() || streams.len() < 2 {
        return None;
    }
    // Each arm's opening sentence tells its own truth (round-8 fix:
    // the all-cursorless arm borrowed the mixed arm's "beside cursored
    // streams" — false when zero cursored streams exist); the deferral
    // consequence after it is one shape for both.
    let opening = if cursorless.len() == streams.len() {
        format!(
            "no stream in this pipeline declares a cursor_field ([{}]): each MAY be a \
             snapshot stream (some cursor-less streams — CDC, for one — still checkpoint \
             through their own mechanism).",
            cursorless.join(", ")
        )
    } else {
        format!(
            "streams [{}] declare no cursor_field beside cursored streams: they MAY be \
             snapshot streams (some cursor-less streams — CDC, for one — still checkpoint \
             through their own mechanism).",
            cursorless.join(", ")
        )
    };
    Some(format!(
        "{opening} Any multi-stream run defers individual commit triggers while a \
         co-stream holds rows its own checkpoint has not covered — commits resume at a \
         checkpoint boundary where every busy stream has checkpointed since its last \
         rows, and under continuously interleaving busy streams such boundaries can be \
         rare; a stream that NEVER checkpoints makes the deferral last the whole run, and \
         byte/time/checkpoint commit policies then cannot bound staging or WAL growth. The \
         run-time deferral warning is the authoritative signal — it fires on what actually \
         checkpoints"
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
    capabilities: Capabilities,
    destination: &dyn Destination,
) -> Result<(), Error> {
    // Durable-identity destinations declare per-run, not per-stream; refuse
    // early so even a no-op run against such a destination fails cleanly.
    if capabilities.requires_durable_identity && config.workdir.is_none() {
        return Err(Error::config(format!(
            "destination `{}` publishes non-atomically and requires a workdir for \
             exactly-once crash recovery; set one with `workdir:` (the CLI defaults \
             to `.rdlt`) — without it a mid-publish failure re-appends committed rows",
            destination.spec().name
        )));
    }

    // 5M6: `ident_rules.max_len` drives the naming probe loop — an
    // exhaustible bound makes its assert a data-reachable host panic.
    // The client validates wire-declared capabilities at the handshake;
    // this seat covers IN-PROCESS destinations, so every path into the
    // engine validates once.
    if let Err(reason) = capabilities.ident_rules.validate() {
        return Err(Error::config(format!(
            "destination `{}` declares out-of-range identifier rules: {reason}",
            destination.spec().name
        )));
    }

    // The stream-count cap (4H2) sits BEFORE everything per-stream: nothing
    // below may scale unboundedly with a source-declared list length.
    if streams.len() > config.max_streams_per_source {
        return Err(Error::config(format!(
            "source declares {} streams, over the {}-stream cap — every declared stream \
             costs plan-time validation and its own share of the run's in-flight budget, \
             so the one discovery axis a source controls directly is bounded like every \
             other; an honestly larger discovery can raise the cap with \
             `EngineConfig::with_max_streams_per_source` (the facade's \
             `PipelineBuilder::max_streams_per_source` plumbs the same knob)",
            streams.len(),
            config.max_streams_per_source
        )));
    }

    if let Some(advisory) = mixed_snapshot_advisory(streams) {
        tracing::warn!("{advisory}");
    }

    let mut root_tables: BTreeMap<TableName, StreamName> = BTreeMap::new();
    for spec in streams {
        // 6.3: the wire gate caps declared stream names at the shared
        // identifier ceiling; this seat mirrors it for in-process
        // sources, so an embedded mega-name cannot ride into plan
        // diagnostics (and the WAL lines that name streams) unbounded.
        if spec.name.as_str().len() > rdlt_connector::gate::MAX_WIRE_IDENTIFIER_BYTES {
            return Err(Error::config(format!(
                "stream name of {} bytes exceeds the {}-byte identifier ceiling — a \
                 name is vocabulary, not a data channel",
                spec.name.as_str().len(),
                rdlt_connector::gate::MAX_WIRE_IDENTIFIER_BYTES
            )));
        }
        let table = root_table(&spec.name, capabilities.ident_rules);
        if let Some(owner) = root_tables.insert(table.clone(), spec.name.clone()) {
            // Clause E2: exactly one stream owns a table.
            return Err(Error::config(format!(
                "streams `{owner}` and `{}` both map to table `{table}`",
                spec.name
            )));
        }
        if matches!(config.write_mode_for(&spec.name), WriteMode::Merge { .. })
            && !capabilities.merge
        {
            return Err(Error::config(format!(
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
                    return Err(Error::config(format!(
                        "stream `{}` column `{column}`: decimal precision {precision} is out of \
                         range (1..={})",
                        spec.name,
                        rdlt_core::types::DECIMAL_MAX_PRECISION
                    )));
                }
                if scale > precision {
                    return Err(Error::config(format!(
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
                return Err(Error::config(format!(
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
                return Err(Error::config(format!(
                    "stream `{}`: Merge key {:?} must name exactly the stream's \
                     declared primary_key columns {:?} (order does not matter)",
                    spec.name, key, declared
                )));
            }
        }
    }

    // ---- Cross-stream table-space collision rules ----
    // Child tables are minted at shred time as `{root}__{field}`. A
    // collision between two DISTINCT streams' table spaces needs their
    // roots A (shorter) and B (longer) to satisfy `B = A + "_" + s` for
    // some suffix `s`: if `s` is empty, B is just A's own table with a
    // trailing `_` and a `_`-leading source field mints the identical
    // child table under either (rule 1, `orders_`/`orders`); if `s`
    // starts with `_`, B already sits inside A's child namespace (rule
    // 2, `__` is A's separator plus that leading `_`); any other `s`
    // mismatches at the boundary character right after A and cannot
    // collide. Both rules are PREFIX-SHAPED, so membership questions
    // against the root set answer them without a pairwise scan (4H2 —
    // the O(S²) loop with four `format!`s per pair turned one large
    // `streams()` reply into hours of synchronous CPU before any budget
    // or deadline could engage):
    //
    // - rule 1 fires iff some root ends in `_` and that root minus the
    //   trailing `_` is also a root;
    // - rule 2 fires iff some root contains `__` and the prefix up to
    //   one of those occurrences is also a root.
    //
    // Each direction is exact: a membership hit IS the pair the pairwise
    // form would have found, so no collision is missed and none is
    // invented. And only PAIRS refuse: a lone root containing `__` or
    // ending in `_` cannot collide with itself, and postgres discovery
    // mints exactly such roots from hostile identifiers
    // (`Order "Items"` -> `order__items_`) that the operator does not
    // own and cannot rename — refusing it outright broke a pinned
    // product capability (the postgres connector's
    // `hostile_identifiers_and_column_selection` conformance cell).
    for (table, stream) in &root_tables {
        let tb = table.as_str();
        if let Some(prefix) = tb.strip_suffix('_')
            && !prefix.is_empty()
            && let Some(owner) = root_tables.get(&TableName::new(prefix))
        {
            return Err(trailing_underscore_collision(owner, prefix, stream, tb));
        }
        let bytes = tb.as_bytes();
        for i in 0..bytes.len().saturating_sub(1) {
            if bytes[i] == b'_'
                && bytes[i + 1] == b'_'
                && let Some(owner) = root_tables.get(&TableName::new(&tb[..i]))
            {
                return Err(child_namespace_collision(owner, &tb[..i], stream, tb));
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
    use rdlt_testkit::memory;

    fn check(precision: u8, scale: u8) -> Result<(), Error> {
        let spec = StreamSpec::new("s")
            .with_type_hint("amount", LogicalType::Decimal { precision, scale });
        let dest = memory::Destination::new();
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

    fn check_streams(names: &[&str]) -> Result<(), Error> {
        let specs: Vec<_> = names.iter().map(|&name| StreamSpec::new(name)).collect();
        let dest = memory::Destination::new();
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
            matches!(error, Error::Config { .. }),
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
        // does not own and cannot rename (the postgres connector's
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

    /// 4H2: the declared stream list is the one discovery axis a source
    /// controls directly, so it is capped like every other axis — a typed
    /// plan-time refusal, before any per-stream work.
    #[test]
    fn a_source_declaring_more_streams_than_the_cap_is_refused() {
        let specs: Vec<_> = (0..crate::DEFAULT_MAX_STREAMS_PER_SOURCE + 1)
            .map(|index| StreamSpec::new(format!("s{index}")))
            .collect();
        let dest = memory::Destination::new();
        let error = validate_streams(
            &EngineConfig::new("streams"),
            &specs,
            dest.capabilities(),
            &dest,
        )
        .expect_err("a stream list past the cap must refuse");
        let text = error.to_string();
        assert!(
            text.contains("stream cap"),
            "the refusal names the cap: {text}"
        );
        assert!(
            text.contains(&(crate::DEFAULT_MAX_STREAMS_PER_SOURCE + 1).to_string()),
            "the refusal reports the declared count: {text}"
        );
        assert!(
            text.contains("with_max_streams_per_source"),
            "the refusal names the operator override (5L9): {text}"
        );

        let at_cap: Vec<_> = (0..crate::DEFAULT_MAX_STREAMS_PER_SOURCE)
            .map(|index| StreamSpec::new(format!("s{index}")))
            .collect();
        validate_streams(
            &EngineConfig::new("streams"),
            &at_cap,
            dest.capabilities(),
            &dest,
        )
        .expect("exactly the cap validates");

        // 5L9: the cap is a knob — an honestly larger discovery raises it.
        let raised = EngineConfig::new("streams").with_max_streams_per_source(2048);
        let over_default: Vec<_> = (0..crate::DEFAULT_MAX_STREAMS_PER_SOURCE + 1)
            .map(|index| StreamSpec::new(format!("s{index}")))
            .collect();
        validate_streams(&raised, &over_default, dest.capabilities(), &dest)
            .expect("a raised cap admits the larger discovery");
    }

    /// The membership formulation of rules 1 and 2 must decide EXACTLY what
    /// the replaced pairwise scan did — including pairs separated by other
    /// roots in sorted order, which a naive adjacent-pair scan would miss:
    /// `a0` sorts between `a` and `a_`, yet `a`/`a_` still collide.
    #[test]
    fn prefix_collisions_are_caught_even_when_not_adjacent_in_sorted_order() {
        let error = check_streams(&["a", "a0", "a_"])
            .expect_err("`a` and `a_` collide through an intervening root");
        assert!(error.to_string().contains("trailing `_`"), "{error}");

        let error = check_streams(&["x", "x0", "x..y"])
            .expect_err("`x` and `x..y` (-> `x__y`) collide through an intervening root");
        assert!(
            error.to_string().contains("child-table namespace"),
            "{error}"
        );

        // And the non-adjacent NON-collision stays accepted: `a0` is
        // neither `a_` nor inside `a__…`.
        assert!(check_streams(&["a", "a0", "ab"]).is_ok());
    }

    /// Rule 2 through NESTED separators: a root whose own name contains
    /// `__` collides with the owner of ANY of its `__`-split prefixes, not
    /// just the outermost — the membership scan checks every occurrence.
    #[test]
    fn a_root_collides_with_the_owner_of_any_double_underscore_prefix() {
        // `x__y__z` sits inside `x__y`'s namespace (and `x`'s); either
        // pairing must refuse.
        assert!(check_streams(&["x", "x__y__z"]).is_err());
        assert!(check_streams(&["x__y", "x__y__z"]).is_err());
        // No prefix owner, no refusal — the lone-root capability.
        assert!(check_streams(&["x__y__z"]).is_ok());
    }

    fn no_workdir_config() -> EngineConfig {
        EngineConfig::new("test")
    }

    fn workdir_config() -> EngineConfig {
        EngineConfig::new("test").with_workdir("/tmp/rdlt-test")
    }

    fn durable_identity_dest() -> memory::Destination {
        memory::Destination::new()
            .with_capabilities(Capabilities::default().with_requires_durable_identity(true))
    }

    fn check_with(config: EngineConfig, destination: memory::Destination) -> Result<(), Error> {
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
        let cursorless = |name: &str| StreamSpec::new(name);
        let cursored = |name: &str| StreamSpec::new(name).with_cursor_field("updated_at");

        let advisory = mixed_snapshot_advisory(&[cursorless("orders"), cursored("events")])
            .expect("the mixed shape earns the advisory");
        assert!(
            advisory.contains("[orders]") && advisory.contains("MAY be snapshot"),
            "the advisory names the cursor-less streams and stays CONDITIONAL — a \
             cursor-less stream can checkpoint through its own mechanism (CDC): {advisory}"
        );
        assert!(
            advisory.contains("beside cursored streams"),
            "the mixed arm says which shape it saw — cursor-less members beside cursored \
             ones: {advisory}"
        );
        assert!(
            advisory.contains("run-time deferral warning is the authoritative signal"),
            "the advisory defers the verdict to the truth-driven run-time warning: {advisory}"
        );
        // Round-9 honesty: the overlap is NOT promised to pass — busy
        // cursored co-streams can interleave so that no covering
        // boundary ever aligns, and the advisory says so instead of
        // calling the overlap transient.
        assert!(
            advisory.contains("such boundaries can be rare") && !advisory.contains("transient"),
            "the deferral consequence states the boundary condition, never a 'transient' \
             promise: {advisory}"
        );

        assert!(
            mixed_snapshot_advisory(&[cursored("a"), cursored("b")]).is_none(),
            "all-cursored pipelines commit mid-run and need no warning"
        );
        let advisory = mixed_snapshot_advisory(&[cursorless("a"), cursorless("b")]).expect(
            "an all-cursor-less multi-stream pipeline warns too — a CDC stream beside a \
             snapshot stream is exactly the shape whose commits defer all run (round-7 fix)",
        );
        assert!(
            advisory.contains("no stream in this pipeline declares a cursor_field ([a, b])"),
            "the all-cursor-less arm tells its own truth (round-8 fix): {advisory}"
        );
        assert!(
            !advisory.contains("beside cursored streams"),
            "with zero cursored streams the advisory must not claim any exist: {advisory}"
        );
        assert!(
            advisory.contains("run-time deferral warning is the authoritative signal"),
            "both arms share the deferral consequence and the run-time handoff: {advisory}"
        );
        assert!(
            mixed_snapshot_advisory(&[cursorless("only")]).is_none(),
            "a single stream has no co-stream to defer against"
        );
        assert!(mixed_snapshot_advisory(&[]).is_none());
    }

    /// 5M6's engine seat: an IN-PROCESS destination declaring an
    /// exhaustible `max_len` refuses at plan time — the wire seat
    /// validates at the handshake, this one covers destinations that
    /// never cross a wire.
    #[test]
    fn an_out_of_range_ident_rules_declaration_is_refused() {
        let dest = memory::Destination::new().with_capabilities(
            Capabilities::default().with_ident_rules(rdlt_core::schema::IdentRules { max_len: 2 }),
        );
        let error = check_with(no_workdir_config(), dest)
            .expect_err("an exhaustible max_len refuses at plan time");
        assert!(
            error.to_string().contains("identifier rules"),
            "the refusal names the rules: {error}"
        );
        // The edges: the floor and the default are both fine.
        for max_len in [rdlt_core::schema::MIN_IDENT_MAX_LEN, 63, 255] {
            let dest = memory::Destination::new().with_capabilities(
                Capabilities::default().with_ident_rules(rdlt_core::schema::IdentRules { max_len }),
            );
            check_with(no_workdir_config(), dest).expect("in-range rules validate");
        }
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

    /// 6.3: the wire gate caps declared stream names at the shared
    /// identifier ceiling; this seat mirrors it for in-process sources
    /// — an embedded mega-name refuses at plan time with the same
    /// vocabulary the wire edge uses.
    #[test]
    fn an_in_process_stream_name_past_the_identifier_ceiling_refuses() {
        let spec = StreamSpec::new("n".repeat(rdlt_connector::gate::MAX_WIRE_IDENTIFIER_BYTES + 1));
        let dest = memory::Destination::new();
        let error = validate_streams(
            &EngineConfig::new("names"),
            std::slice::from_ref(&spec),
            dest.capabilities(),
            &dest,
        )
        .expect_err("an over-ceiling stream name must refuse");
        let text = error.to_string();
        assert!(
            text.contains("identifier ceiling"),
            "the refusal names the ceiling: {text}"
        );
    }
}

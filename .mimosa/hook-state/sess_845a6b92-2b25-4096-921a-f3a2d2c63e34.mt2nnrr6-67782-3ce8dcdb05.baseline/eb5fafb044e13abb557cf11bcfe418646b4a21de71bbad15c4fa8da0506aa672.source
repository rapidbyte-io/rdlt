//! Collision-safe naming as executable properties: distinct source names
//! never silently merge, and assigned names are stable.

use proptest::prelude::*;
use rdlt_core::schema::IdentRules;
use rdlt_engine::naming::{UniqueNamer, child_table_name, normalize_ident};

proptest! {
    /// UniqueNamer is injective over distinct source names.
    #[test]
    fn distinct_sources_get_distinct_names(sources in proptest::collection::hash_set(".{1,40}", 1..30)) {
        let mut namer = UniqueNamer::new(IdentRules::default());
        let names: Vec<String> = sources.iter().map(|s| namer.name_for(s)).collect();
        let unique: std::collections::HashSet<&String> = names.iter().collect();
        prop_assert_eq!(unique.len(), names.len(), "collision in {:?}", names);
    }

    /// Asking again for a name already assigned returns the same identifier.
    #[test]
    fn assignment_is_stable(sources in proptest::collection::vec(".{1,40}", 1..30)) {
        let mut namer = UniqueNamer::new(IdentRules::default());
        let first: Vec<String> = sources.iter().map(|s| namer.name_for(s)).collect();
        let second: Vec<String> = sources.iter().map(|s| namer.name_for(s)).collect();
        prop_assert_eq!(first, second);
    }

    /// Normalized identifiers respect the destination's constraints.
    #[test]
    fn normalized_idents_are_destination_safe(source in ".{1,200}", max_len in 16usize..64) {
        let rules = IdentRules { max_len };
        let ident = normalize_ident(&source, rules);
        prop_assert!(!ident.is_empty());
        prop_assert!(ident.len() <= max_len);
        prop_assert!(ident.chars().all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_')));
        prop_assert!(!ident.as_bytes()[0].is_ascii_digit());
    }

    /// Normalization itself is deterministic.
    #[test]
    fn normalize_is_deterministic(source in ".{1,200}") {
        let rules = IdentRules::default();
        prop_assert_eq!(normalize_ident(&source, rules), normalize_ident(&source, rules));
    }

    /// The engine's collision gate is complete: for any two distinct stream
    /// names whose normalized roots A, B pass BOTH pairwise rules the run
    /// validator applies (rule 1: `B != A + "_"` and `A != B + "_"`; rule 2:
    /// neither root starts with the other's `+ "__"`), no chain of child
    /// derivations from one can equal the other root or any child of the
    /// other root — even against a raw source field that itself starts with
    /// `_` (Mongo's `_id`). A root that ITSELF contains `__` or ends in `_`
    /// is not tested here as a solo precondition — the gate is deliberately
    /// pairwise (an absolute per-root rule would refuse single
    /// hostile-but-harmless roots that postgres discovery mints from
    /// identifiers the operator does not own, e.g. `Order "Items"` ->
    /// `order__items_`), so this property only ever compares TWO roots
    /// against each other, matching what the gate actually checks.
    ///
    /// `(a, b)` come from [`root_pair_strategy`] rather than two independent
    /// raw regex classes, for two measured reasons. First: a regex class
    /// containing `_`/`.` at any meaningful length makes `normalize_ident`'s
    /// output contain `__` in the large majority of samples (proptest's
    /// regex-derived string strategy does not sample chars i.i.d. — a class
    /// containing `_` lands adjacent separators far more often than uniform
    /// sampling would), which starves the `prop_assume!` below and aborts
    /// the run on "too many global rejects" before real coverage happens;
    /// building each root as alnum segments joined by exactly one separator
    /// per boundary makes `__` unreachable in the raw string by
    /// construction, no filtering needed. Second: two fully independent
    /// random roots essentially never land in the `rb == ra + "_"` relation
    /// (`orders_`/`orders`) that rule 1 exists to refuse — verified live by
    /// temporarily dropping that half of the `prop_assume!` below with fully
    /// independent roots and watching the run pass anyway.
    /// `root_pair_strategy` therefore deliberately mints that exact boundary
    /// shape a quarter of the time, alongside independent pairs for general
    /// coverage. `field` allows a leading `_` (`_?[a-z][a-z0-9_]{0,20}`) so
    /// the child derivation itself can also land on the `_id`-shaped
    /// boundary. The literal-separator edge cases (`users__emails`,
    /// `users..emails`, `orders_`/`orders`, a bare `_` root, alone AND
    /// paired) live in the run validator's unit tests too.
    #[test]
    fn distinct_separator_free_roots_have_disjoint_table_spaces(
        (a, b) in root_pair_strategy(),
        field in "_?[a-z][a-z0-9_]{0,20}",
    ) {
        let rules = IdentRules::default();
        let ra = normalize_ident(&a, rules);
        let rb = normalize_ident(&b, rules);
        prop_assume!(
            ra != rb
                && rb != format!("{ra}_") && ra != format!("{rb}_")
                && !rb.starts_with(&format!("{ra}__")) && !ra.starts_with(&format!("{rb}__"))
        );
        let child_a = child_table_name(&ra, &field, rules);
        prop_assert_ne!(&child_a, &rb, "child of a equals root b");
        prop_assert!(
            !child_a.starts_with(&format!("{rb}__")),
            "child of a ({child_a}) sits inside b's child space"
        );
    }
}

/// A root name built from alnum segments joined by exactly one `_`/`.`
/// separator per boundary — never two adjacent, so the raw string (and
/// hence its normalized form, since both separators map to a lone `_`) can
/// never contain `__`. A constructive alternative to filtering a raw regex
/// class down with `prop_assume!`; see the doc comment on
/// `distinct_separator_free_roots_have_disjoint_table_spaces` for why
/// filtering starves on "too many global rejects" instead.
fn base_root_strategy() -> impl Strategy<Value = String> {
    (
        prop::collection::vec("[a-z][a-z0-9]{0,10}", 1..=3),
        prop::collection::vec(prop_oneof!["_", "."], 2),
    )
        .prop_map(|(segments, seps)| {
            let mut out = segments[0].clone();
            for (seg, sep) in segments[1..].iter().zip(seps.iter()) {
                out.push_str(sep);
                out.push_str(seg);
            }
            out
        })
}

/// Root pairs for the disjointness property: three parts independent
/// (general coverage, drawn from [`base_root_strategy`] with an occasional
/// trailing separator on EACH side independently), one part DELIBERATELY
/// correlated so `a` is exactly `b` plus one trailing separator — the
/// precise `orders_`/`orders` boundary shape the trailing-`_` clause exists
/// to refuse. Two independently generated roots essentially never land in
/// that relation by chance (confirmed live: dropping the `ends_with('_')`
/// assumption with only independent pairs never produced a failure), so
/// without this deliberate pairing the property could not find the
/// counterexample its own doc comment describes.
fn root_pair_strategy() -> impl Strategy<Value = (String, String)> {
    prop_oneof![
        3 => (
            with_occasional_trailing_separator(base_root_strategy()),
            with_occasional_trailing_separator(base_root_strategy()),
        ),
        1 => base_root_strategy().prop_map(|base| (format!("{base}_"), base)),
    ]
}

/// Appends a single trailing separator about 15% of the time, so
/// independently generated roots still occasionally exercise the
/// ends-with-`_` clause on their own (not just via the deliberately
/// correlated pairing in [`root_pair_strategy`]).
fn with_occasional_trailing_separator(
    strategy: impl Strategy<Value = String>,
) -> impl Strategy<Value = String> {
    (strategy, proptest::bool::weighted(0.15)).prop_map(|(mut base, trailing)| {
        if trailing {
            base.push('_');
        }
        base
    })
}

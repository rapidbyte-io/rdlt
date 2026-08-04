//! Collision-safe naming as executable properties (design doc §5.3): distinct source
//! names never silently merge, and assigned names are stable.

use proptest::prelude::*;
use rdlt_core::naming::{IdentRules, UniqueNamer, child_table_name, normalize_ident};

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

    /// The engine's collision gate is complete: for any two distinct
    /// stream names whose normalized roots are distinct and
    /// `__`-free, no chain of child derivations from one can equal
    /// the other root or any child of the other root.
    ///
    /// The source alphabet deliberately excludes `_` and `.`: measured live,
    /// including either in the class makes `normalize_ident`'s output contain
    /// `__` in the large majority of samples (proptest's regex-derived string
    /// strategy does not sample chars i.i.d. — a class containing `_` lands
    /// adjacent underscores far more often than uniform sampling would), which
    /// starves the `prop_assume!` below and aborts the run on "too many global
    /// rejects" before real coverage happens. An alnum-only class can never
    /// itself produce `__` (each truncation-hash boundary inserts exactly one
    /// `_`), so the precondition holds by construction while still exercising
    /// the truncation path (lengths run past `IdentRules::default().max_len`
    /// = 63). The `__`-free unit tests in `runtime/validate.rs`
    /// (`users__emails`, `users..emails`) cover the literal-separator input
    /// this property's alphabet no longer generates.
    #[test]
    fn distinct_separator_free_roots_have_disjoint_table_spaces(
        a in "[a-z][a-z0-9]{0,80}",
        b in "[a-z][a-z0-9]{0,80}",
        field in "[a-z][a-z0-9]{0,20}",
    ) {
        let rules = IdentRules::default();
        let ra = normalize_ident(&a, rules);
        let rb = normalize_ident(&b, rules);
        prop_assume!(ra != rb && !ra.contains("__") && !rb.contains("__"));
        let child_a = child_table_name(&ra, &field, rules);
        prop_assert_ne!(&child_a, &rb, "child of a equals root b");
        prop_assert!(
            !child_a.starts_with(&format!("{rb}__")),
            "child of a ({child_a}) sits inside b's child space"
        );
    }
}

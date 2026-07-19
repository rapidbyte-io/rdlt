//! Collision-safe naming as executable properties (design doc §5.3): distinct source
//! names never silently merge, and assigned names are stable.

use proptest::prelude::*;
use rdlt_core::naming::{IdentRules, UniqueNamer, normalize_ident};

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
}

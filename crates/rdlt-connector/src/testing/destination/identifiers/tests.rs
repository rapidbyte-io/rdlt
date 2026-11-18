use std::collections::BTreeSet;
use std::num::NonZeroU16;

use super::Identifiers;
use crate::capabilities::{Capabilities, IdentifierCase, IdentifierRules};

/// The least rules, with identifiers of `longest` bytes folded as `case` says.
fn rules(longest: u16, case: IdentifierCase) -> IdentifierRules {
    IdentifierRules {
        case,
        max_len: NonZeroU16::new(longest).expect("not zero"),
        ..Capabilities::minimal().identifiers
    }
}

/// The names a clause gives its tables, as a run's clause names them.
fn tables() -> Vec<String> {
    let clause = "certify_18a2b3c4d5e6f708_12";
    let suffixes = [
        "",
        "_generation",
        "_widened_10",
        "_dropped_append",
        "_dropped_append__items",
        "_roots__items",
        "_roots__tags",
    ];
    suffixes
        .iter()
        .map(|suffix| format!("{clause}{suffix}"))
        .collect()
}

#[test]
fn a_table_keeps_its_name_where_it_fits_and_a_digest_of_it_where_not() {
    let names = Identifiers::new(&rules(63, IdentifierCase::Preserve));
    for table in tables() {
        assert_eq!(*names.table(&table), table);
    }
    // A name exactly as long as identifiers go fits.
    let fitting = Identifiers::new(&rules(27, IdentifierCase::Preserve));
    assert_eq!(*fitting.table(&tables()[0]), tables()[0]);
    for longest in [16, 30] {
        let names = Identifiers::new(&rules(longest, IdentifierCase::Preserve));
        let identifiers: BTreeSet<String> = tables()
            .iter()
            .map(|table| names.table(table).to_string())
            .collect();
        assert_eq!(identifiers.len(), tables().len(), "{identifiers:?}");
        for identifier in &identifiers {
            assert!(identifier.len() <= usize::from(longest), "{identifier}");
            assert!(identifier.starts_with("certify_"), "{identifier}");
        }
    }
    let upper = Identifiers::new(&rules(16, IdentifierCase::Upper));
    let shortened = upper.table(&tables()[4]);
    assert!(shortened.starts_with("CERTIFY_"), "{shortened}");
    assert_eq!(*shortened, shortened.to_uppercase());
}

#[test]
fn a_column_is_folded_and_lengthened_past_the_words_reserved() {
    let mut reserving = rules(16, IdentifierCase::Upper);
    reserving.reserved = ["id", "ID_", "_rdlt_seq"].map(str::to_owned).into();
    let names = Identifiers::new(&reserving);
    assert_eq!(&*names.column("id"), "ID__");
    assert_eq!(&*names.column("name"), "NAME");
    assert_eq!(&*names.column("_rdlt_seq"), "_RDLT_SEQ_");
    assert_eq!(names.unnamed(), None);
    // No lengthening of a name of sixteen bytes fits: it is left as it folds.
    reserving.reserved.insert("_rdlt_deleted_at".to_owned());
    let names = Identifiers::new(&reserving);
    assert_eq!(&*names.column("_rdlt_deleted_at"), "_RDLT_DELETED_AT");
    assert!(names.unnamed().is_some());
}

#[test]
fn rules_that_reserve_how_certification_names_tables_leave_its_tables_unnamed() {
    let cases = [
        (vec!["cert"], Vec::new(), true),
        (vec!["certify_18"], Vec::new(), true),
        (vec!["CERTIFY"], Vec::new(), true),
        (vec!["sqlite_", "pg_"], Vec::new(), false),
        (Vec::new(), vec!["certify_1"], true),
        (Vec::new(), vec!["certify"], false),
    ];
    for (prefixes, words, unnamed) in cases {
        let mut reserving = rules(63, IdentifierCase::Lower);
        reserving.reserved_table_prefixes =
            prefixes.iter().map(|prefix| (*prefix).to_owned()).collect();
        reserving.reserved = words.iter().map(|word| (*word).to_owned()).collect();
        let names = Identifiers::new(&reserving);
        assert_eq!(names.unnamed().is_some(), unnamed, "{prefixes:?} {words:?}");
    }
}

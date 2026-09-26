use std::collections::BTreeSet;
use std::num::NonZeroU16;

use rdlt_connector::{
    ColumnKey, ColumnPath, IdentifierCase, IdentifierChars, IdentifierRules, NameMap,
};

use super::{fits, fits_table, unnamed};

fn rules(case: IdentifierCase) -> IdentifierRules {
    IdentifierRules {
        case,
        max_len: NonZeroU16::new(6).unwrap(),
        chars: IdentifierChars::AsciiWord,
        reserved: BTreeSet::from(["value".to_owned()]),
        reserved_table_prefixes: BTreeSet::new(),
    }
}

#[test]
fn identifiers_follow_the_destinations_rules() {
    let lower = rules(IdentifierCase::Lower);
    assert!(fits(&lower, "id_2"));
    assert!(!fits(&lower, "Id"), "the case");
    assert!(!fits(&lower, "toolong"), "the length");
    assert!(!fits(&lower, "a-b"), "the characters");
    assert!(!fits(&lower, "value"), "reserved");
    assert!(fits(&rules(IdentifierCase::Upper), "ID"));
    assert!(!fits(&rules(IdentifierCase::Upper), "id"));
}

#[test]
fn a_column_no_name_map_names_and_no_metadata_column_is_unnamed() {
    let mut names = NameMap::default();
    names
        .insert(ColumnKey::Source(ColumnPath::from("id")), "id".to_owned())
        .unwrap();
    let fields: Vec<String> = ["id", "stray", "_load_id", "_loaded_at"]
        .map(ToOwned::to_owned)
        .into();
    assert_eq!(unnamed(&fields, &names, 2), ["stray"]);
    assert!(unnamed(&fields[..1], &names, 0).is_empty());
}

#[test]
fn reserved_words_compare_as_the_rules_fold_case() {
    let upper = rules(IdentifierCase::Upper);
    assert!(
        !fits(&upper, "VALUE"),
        "the reserved word folds to upper case"
    );
    let preserve = rules(IdentifierCase::Preserve);
    assert!(!fits(&preserve, "value"));
    assert!(
        fits(&preserve, "Value"),
        "a case-preserving destination reserves the word as written"
    );
}

#[test]
fn a_table_identifier_starts_with_no_reserved_prefix() {
    let mut rules = rules(IdentifierCase::Lower);
    rules.reserved_table_prefixes = BTreeSet::from(["S".to_owned(), "tmp_".to_owned()]);
    assert!(!fits_table(&rules, "s0"), "prefixes fold like identifiers");
    assert!(!fits_table(&rules, "tmp_a"));
    assert!(fits_table(&rules, "_s0"));
    assert!(!fits_table(&rules, "_s0_long"), "and follow the rules");
}

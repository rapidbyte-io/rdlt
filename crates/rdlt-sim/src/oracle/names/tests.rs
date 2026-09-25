use std::collections::BTreeSet;
use std::num::NonZeroU16;

use rdlt_connector::{
    ColumnKey, ColumnPath, IdentifierCase, IdentifierChars, IdentifierRules, NameMap,
};

use super::{fits, unnamed};

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
    assert!(!fits(&lower, "VALUE"), "reserved, whatever its case");
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

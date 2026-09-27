use std::collections::BTreeSet;
use std::num::NonZeroU16;

use super::{names, padded};
use crate::capabilities::{IdentifierCase, IdentifierChars, IdentifierRules};

fn rules(case: IdentifierCase, chars: IdentifierChars) -> IdentifierRules {
    IdentifierRules {
        case,
        max_len: NonZeroU16::new(40).expect("not zero"),
        chars,
        reserved: BTreeSet::new(),
        reserved_table_prefixes: BTreeSet::new(),
    }
}

#[test]
fn the_names_follow_the_destinations_rules() {
    let long = |prefix: &str| format!("{prefix}{}", "g".repeat(40 - prefix.len()));
    assert_eq!(
        names(&rules(IdentifierCase::Lower, IdentifierChars::AsciiWord)),
        ["id".to_owned(), long("long_")]
    );
    assert_eq!(
        names(&rules(IdentifierCase::Upper, IdentifierChars::AsciiWord)),
        ["ID".to_owned(), long("LONG_").replace('g', "G")]
    );
    assert_eq!(
        names(&rules(IdentifierCase::Preserve, IdentifierChars::Any)),
        [
            "id".to_owned(),
            long("long_"),
            "données_名前".to_owned(),
            "MixedCase".to_owned()
        ]
    );
}

#[test]
fn a_name_is_lengthened_to_the_longest_and_never_cut() {
    assert_eq!(padded("ab".to_owned(), 5, 'x'), "abxxx");
    assert_eq!(padded("ab".to_owned(), 2, 'x'), "ab");
    assert_eq!(padded("abcdef".to_owned(), 3, 'x'), "abcdef");
}

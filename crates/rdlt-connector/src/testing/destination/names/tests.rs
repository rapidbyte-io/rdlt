use std::collections::BTreeSet;
use std::num::NonZeroU16;

use super::{names, padded, unreserved};
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
        names(&rules(IdentifierCase::Lower, IdentifierChars::AsciiWord)).unwrap(),
        ["id".to_owned(), long("long_")]
    );
    assert_eq!(
        names(&rules(IdentifierCase::Upper, IdentifierChars::AsciiWord)).unwrap(),
        ["ID".to_owned(), long("LONG_").replace('g', "G")]
    );
    assert_eq!(
        names(&rules(IdentifierCase::Preserve, IdentifierChars::Any)).unwrap(),
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

#[test]
fn the_names_keep_clear_of_the_destinations_reserved_words() {
    let mut reserved = rules(IdentifierCase::Upper, IdentifierChars::AsciiWord);
    reserved.reserved = ["ID".to_owned(), "ID_".to_owned()].into();
    let names = names(&reserved).unwrap();
    assert!(
        names.iter().all(|name| !reserved.reserved.contains(name)),
        "{names:?}"
    );
    assert_eq!(names[0], "ID__");
}

#[test]
fn a_long_reserved_list_is_folded_once_however_many_names_it_takes() {
    // Words that sort before the name, and the name lengthened five hundred times over.
    let mut long = rules(IdentifierCase::Lower, IdentifierChars::AsciiWord);
    long.max_len = NonZeroU16::new(600).expect("not zero");
    long.reserved = (0..200_000).map(|word| format!("A{word:06}")).collect();
    long.reserved
        .extend((0..500).map(|more| format!("ID{}", "_".repeat(more))));
    let began = std::time::Instant::now();
    let names = names(&long).unwrap();
    // Folding every word for every candidate takes a hundred million foldings.
    assert!(
        began.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );
    assert_eq!(names[0], format!("id{}", "_".repeat(500)));
}

#[test]
fn a_name_is_lengthened_no_further_than_identifiers_go() {
    let chain = |forms: usize| -> BTreeSet<String> {
        (0..forms)
            .map(|more| format!("id{}", "_".repeat(more)))
            .collect()
    };
    // The longest identifier is free: it is the name.
    assert_eq!(
        unreserved(&chain(3), 5, "id".to_owned()),
        Some("id___".to_owned())
    );
    // It is reserved too: no form of the name is left.
    assert_eq!(unreserved(&chain(4), 5, "id".to_owned()), None);
    assert_eq!(unreserved(&chain(9), 5, "id".to_owned()), None);
    // A name already as long as identifiers go is never lengthened.
    assert_eq!(unreserved(&chain(1), 2, "id".to_owned()), None);
    assert_eq!(
        unreserved(&chain(0), 2, "id".to_owned()),
        Some("id".to_owned())
    );
    let mut every = rules(IdentifierCase::Preserve, IdentifierChars::Any);
    every.max_len = NonZeroU16::new(32).expect("not zero");
    every.reserved = (0..=23)
        .map(|more| format!("MixedCase{}", "_".repeat(more)))
        .collect();
    assert_eq!(names(&every), None);
    every.reserved.pop_last();
    assert_eq!(names(&every).unwrap()[3].len(), 32);
}

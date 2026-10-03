use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;

use proptest::prelude::*;
use rdlt_connector::{
    ColumnKey, ColumnPath, IdentifierCase, IdentifierChars, IdentifierRules, NameMap, TablePath,
    TypeKind,
};

use super::Naming;

fn rules(case: IdentifierCase, chars: IdentifierChars, max_len: u16) -> IdentifierRules {
    IdentifierRules {
        case,
        max_len: NonZeroU16::new(max_len).unwrap(),
        chars,
        reserved: BTreeSet::from(["select".to_owned()]),
        reserved_table_prefixes: BTreeSet::new(),
    }
}

fn lower() -> Naming {
    Naming::new(rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 63))
}

fn source(segments: &[&str]) -> ColumnKey {
    ColumnKey::Source(ColumnPath::new(segments.iter().copied()).unwrap())
}

/// The identifiers `naming` assigns to `keys` in one push, by key.
fn assign(naming: &Naming, keys: &[ColumnKey]) -> BTreeMap<ColumnKey, String> {
    let mut names = NameMap::default();
    let keys: BTreeSet<ColumnKey> = keys.iter().cloned().collect();
    naming.assign_columns(&mut names, &keys).unwrap();
    names
        .iter()
        .map(|(key, name)| (key.clone(), name.to_owned()))
        .collect()
}

fn hashed(name: &str, base: &str) -> bool {
    name.strip_prefix(base)
        .and_then(|rest| rest.strip_prefix('_'))
        .is_some_and(|hash| {
            hash.len() == 6
                && hash
                    .chars()
                    .all(|c| "abcdefghijklmnopqrstuvwxyz234567".contains(c))
        })
}

#[test]
fn identifiers_follow_the_destination_rules() {
    use IdentifierCase as Case;
    use IdentifierChars as Chars;
    let cases = [
        (Case::Lower, Chars::AsciiWord, "Order Id", "order_id"),
        (Case::Lower, Chars::AsciiWord, "1st", "_1st"),
        (Case::Lower, Chars::AsciiWord, "", "_"),
        (Case::Lower, Chars::AsciiWord, "naïve", "na_ve"),
        (Case::Preserve, Chars::Any, "Café", "Café"),
        (Case::Preserve, Chars::Any, "a\tb", "a_b"),
        (Case::Upper, Chars::AsciiWord, "a-b", "A_B"),
        (Case::Preserve, Chars::AsciiWord, "a__b", "a__b"),
    ];
    for (case, chars, name, expected) in cases {
        let naming = Naming::new(rules(case, chars, 63));
        let assigned = assign(&naming, &[source(&[name])]);
        assert_eq!(assigned[&source(&[name])], expected, "{name:?}");
    }
}

#[test]
fn any_character_rules_keep_none_that_hides_or_reorders_text() {
    let naming = Naming::new(rules(IdentifierCase::Preserve, IdentifierChars::Any, 63));
    let assigned = assign(
        &naming,
        &[source(&["is_admin\u{200b}"]), source(&["é\u{202e}x"])],
    );
    assert_eq!(assigned[&source(&["is_admin\u{200b}"])], "is_admin_");
    assert_eq!(assigned[&source(&["é\u{202e}x"])], "é_x");
}

#[test]
fn a_taken_identifier_gets_a_hash_of_its_source_path() {
    let assigned = assign(&lower(), &[source(&["a"]), source(&["A"])]);
    assert_eq!(
        assigned[&source(&["A"])],
        "a",
        "sorted first, so assigned first"
    );
    assert!(hashed(&assigned[&source(&["a"])], "a"), "{assigned:?}");
    let again = assign(&lower(), &[source(&["A"]), source(&["a"])]);
    assert_eq!(assigned, again, "arrival order does not matter");
}

#[test]
fn reserved_words_are_never_assigned() {
    let assigned = assign(&lower(), &[source(&["SELECT"])]);
    assert!(
        hashed(&assigned[&source(&["SELECT"])], "select"),
        "{assigned:?}"
    );
}

#[test]
fn long_names_are_cut_to_fit_with_their_hash() {
    let naming = Naming::new(rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 16));
    let first = source(&["abcdefghijklmnopqrstuv"]);
    let second = source(&["abcdefghijklmnopXYZ"]);
    let assigned = assign(&naming, &[first.clone(), second.clone()]);
    assert_eq!(assigned[&second], "abcdefghijklmnop");
    assert!(hashed(&assigned[&first], "abcdefghi"), "{assigned:?}");
}

#[test]
fn variants_are_named_after_their_column_and_kind() {
    let variant = ColumnKey::Variant {
        column: ColumnPath::from("Amount"),
        kind: TypeKind::Json,
    };
    let assigned = assign(&lower(), &[source(&["Amount"]), variant.clone()]);
    assert_eq!(assigned[&source(&["Amount"])], "amount");
    assert_eq!(assigned[&variant], "amount__json");
}

#[test]
fn nested_paths_join_with_escaped_separators() {
    let assigned = assign(
        &lower(),
        &[
            source(&["a", "b"]),
            source(&["a__b"]),
            source(&["a", "b__c"]),
        ],
    );
    assert_eq!(assigned[&source(&["a", "b__c"])], "a__b_x5f_c");
    let pair = [
        &assigned[&source(&["a", "b"])],
        &assigned[&source(&["a__b"])],
    ];
    assert!(pair.contains(&&"a__b".to_owned()), "{assigned:?}");
    assert!(pair.iter().any(|name| hashed(name, "a__b")), "{assigned:?}");
}

#[test]
fn assigned_names_never_move() {
    let naming = lower();
    let mut names = NameMap::default();
    let first = BTreeSet::from([source(&["b"])]);
    naming.assign_columns(&mut names, &first).unwrap();
    let second = BTreeSet::from([source(&["B"]), source(&["b"])]);
    naming.assign_columns(&mut names, &second).unwrap();
    assert_eq!(names.get(&source(&["b"])), Some("b"));
    assert!(hashed(names.get(&source(&["B"])).unwrap(), "b"));
}

#[test]
fn table_names_follow_the_same_rules() {
    let naming = lower();
    let path = TablePath::new(["public.Orders"]).unwrap();
    assert_eq!(
        naming.table(&path, &BTreeSet::new()).unwrap(),
        "public_orders"
    );
    let taken = BTreeSet::from(["public_orders".to_owned()]);
    assert!(hashed(
        &naming.table(&path, &taken).unwrap(),
        "public_orders"
    ));
    let nested = TablePath::new(["orders", "items"]).unwrap();
    assert_eq!(
        naming.table(&nested, &BTreeSet::new()).unwrap(),
        "orders__items"
    );
}

#[test]
fn table_identifiers_never_start_with_a_reserved_prefix() {
    let mut reserving = rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 63);
    reserving.reserved_table_prefixes = BTreeSet::from(["sqlite_".to_owned(), "_rdlt_".to_owned()]);
    let naming = Naming::new(reserving.clone());
    let table = |name: &str| naming.table(&TablePath::new([name]).unwrap(), &BTreeSet::new());
    assert_eq!(table("SQLite_Stat").unwrap(), "_sqlite_stat");
    assert_eq!(
        table("_rdlt_staging__orders").unwrap(),
        "__rdlt_staging__orders"
    );
    assert_eq!(table("orders").unwrap(), "orders");
    let taken = BTreeSet::from(["_sqlite_stat".to_owned()]);
    let path = TablePath::new(["sqlite_stat"]).unwrap();
    assert!(hashed(
        &naming.table(&path, &taken).unwrap(),
        "_sqlite_stat"
    ));
    let columns = assign(&naming, &[source(&["sqlite_x"])]);
    assert_eq!(
        columns[&source(&["sqlite_x"])],
        "sqlite_x",
        "columns keep the prefix"
    );
    reserving.reserved_table_prefixes = BTreeSet::from(["_".to_owned()]);
    let error = Naming::new(reserving)
        .table(&TablePath::new(["_x"]).unwrap(), &BTreeSet::new())
        .unwrap_err();
    assert_eq!(error.code(), Some("identifier_exhausted"));
}

fn any_rules() -> impl Strategy<Value = IdentifierRules> {
    let case = prop_oneof![
        Just(IdentifierCase::Preserve),
        Just(IdentifierCase::Lower),
        Just(IdentifierCase::Upper),
    ];
    let chars = prop_oneof![Just(IdentifierChars::AsciiWord), Just(IdentifierChars::Any)];
    (case, chars, 16_u16..24).prop_map(|(case, chars, max_len)| rules(case, chars, max_len))
}

fn any_keys() -> impl Strategy<Value = Vec<ColumnKey>> {
    let name = "[aAbB_ .\\-é]{0,12}";
    let key = prop_oneof![
        3 => name.prop_map(|name| source(&[name.as_str()])),
        1 => (name, name).prop_map(|(a, b)| source(&[a.as_str(), b.as_str()])),
        1 => name.prop_map(|name| ColumnKey::Variant {
            column: ColumnPath::from(name.as_str()),
            kind: TypeKind::Json,
        }),
    ];
    proptest::collection::vec(key, 0..24)
}

proptest! {
    #[test]
    fn assignment_is_injective_stable_bounded_and_idempotent(
        rules in any_rules(),
        keys in any_keys(),
        split in 0_usize..24,
    ) {
        let naming = Naming::new(rules.clone());
        let split = split.min(keys.len());
        let mut names = NameMap::default();
        let first: BTreeSet<ColumnKey> = keys[..split].iter().cloned().collect();
        naming.assign_columns(&mut names, &first).unwrap();
        let before = names.clone();
        let all: BTreeSet<ColumnKey> = keys.iter().cloned().collect();
        naming.assign_columns(&mut names, &all).unwrap();
        for (key, name) in before.iter() {
            prop_assert_eq!(names.get(key), Some(name), "stable");
        }
        let mut seen = BTreeSet::new();
        for (key, name) in names.iter() {
            prop_assert!(seen.insert(name.to_owned()), "{} is assigned twice", name);
            prop_assert!(!name.is_empty() && name.len() <= usize::from(rules.max_len.get()), "{}", name);
            prop_assert!(!naming.is_metadata(name) && name.to_lowercase() != "select", "{}", name);
            if rules.chars == IdentifierChars::AsciiWord {
                prop_assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'), "{}", name);
            }
            prop_assert!(all.contains(key));
        }
        let settled = names.clone();
        naming.assign_columns(&mut names, &all).unwrap();
        prop_assert_eq!(names, settled, "idempotent");
    }
}

#[test]
fn metadata_columns_follow_the_rules_and_never_share_an_identifier() {
    let upper = Naming::new(rules(IdentifierCase::Upper, IdentifierChars::AsciiWord, 63));
    assert_eq!(&*upper.metadata("_rdlt_load_id"), "_RDLT_LOAD_ID");
    let mut reserving = rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 16);
    reserving.reserved.insert("_rdlt_seq".to_owned());
    let short = Naming::new(reserving);
    let names: BTreeSet<String> = super::METADATA
        .iter()
        .map(|column| short.metadata(column).to_string())
        .collect();
    assert_eq!(names.len(), super::METADATA.len(), "{names:?}");
    assert!(names.iter().all(|name| name.len() <= 16), "{names:?}");
    assert!(hashed(&short.metadata("_rdlt_seq"), "_rdlt_seq"));
    assert_eq!(&*short.metadata("_rdlt_is_current"), "_rdlt_is_current");
}

/// The hash that tells collisions apart is the xxh3 of the exact source identity, so it never
/// changes between runs or builds.
#[test]
fn collision_hashes_are_fixed_by_the_exact_source_identity() {
    let variant = ColumnKey::Variant {
        column: ColumnPath::from("a"),
        kind: TypeKind::Json,
    };
    let assigned = assign(
        &lower(),
        &[
            source(&["A"]),
            source(&["a"]),
            source(&["a", "b"]),
            source(&["a__b"]),
            source(&["a__json"]),
            variant.clone(),
        ],
    );
    let table = lower()
        .table(
            &TablePath::new(["a"]).unwrap(),
            &BTreeSet::from(["a".to_owned()]),
        )
        .unwrap();
    let names = [
        assigned[&source(&["a"])].as_str(),
        assigned[&source(&["a__b"])].as_str(),
        assigned[&variant].as_str(),
        table.as_str(),
    ];
    assert_eq!(
        names,
        ["a_cdrbrn", "a__b_gx5iju", "a__json_fuh367", "a_n25c7x"]
    );
}

#[test]
#[expect(clippy::disallowed_methods, reason = "the test times real work")]
fn names_are_checked_against_every_reserved_word_once_folded() {
    use rdlt_connector::limits::{MAX_COLUMNS, MAX_RESERVED_WORDS};
    let mut reserving = rules(IdentifierCase::Upper, IdentifierChars::AsciiWord, 63);
    reserving.reserved = (0..MAX_RESERVED_WORDS)
        .map(|word| format!("word{word}"))
        .collect();
    let naming = Naming::new(reserving);
    let columns = usize::try_from(MAX_COLUMNS).unwrap();
    let keys: Vec<ColumnKey> = (0..columns)
        .map(|column| source(&[format!("column{column}").as_str()]))
        .chain([source(&["Word7"])])
        .collect();
    // Folding every reserved word again for each name takes many seconds at the limits.
    let started = std::time::Instant::now();
    let assigned = assign(&naming, &keys);
    let elapsed = started.elapsed();
    assert!(elapsed < std::time::Duration::from_secs(2), "{elapsed:?}");
    let reserved = &assigned[&source(&["Word7"])];
    assert!(hashed(&reserved.to_lowercase(), "word7"), "{reserved}");
    assert_eq!(assigned[&source(&["column3"])], "COLUMN3");
}

#[test]
fn a_table_name_escapes_every_reserved_prefix_it_meets_with_underscores() {
    use rdlt_connector::limits::MAX_RESERVED_PREFIXES;
    let mut reserving = rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 512);
    // Each prefix traps the name behind as many underscores as it has: the name needs one more
    // than the most.
    reserving.reserved_table_prefixes = (0..MAX_RESERVED_PREFIXES)
        .map(|underscores| format!("{}o", "_".repeat(underscores)))
        .collect();
    let naming = Naming::new(reserving.clone());
    let named = naming
        .table(&TablePath::new(["Orders"]).unwrap(), &BTreeSet::new())
        .unwrap();
    assert_eq!(
        named,
        format!("{}orders", "_".repeat(MAX_RESERVED_PREFIXES))
    );
    let untrapped = naming
        .table(&TablePath::new(["items"]).unwrap(), &BTreeSet::new())
        .unwrap();
    assert_eq!(untrapped, "items");
}

proptest! {
    #[test]
    fn every_table_name_given_is_one_the_rules_admit(
        rules in any_rules(),
        prefixes in proptest::sample::subsequence(
            vec!["pragma_", "_", "_p", "p", "sqlite_", "_pragma_"], 0..4),
        segments in proptest::collection::vec("[pP]ragma|[aAbB_ ]{1,6}", 1..3),
        taken in proptest::collection::btree_set("_{0,2}pragma|[ab_]{1,4}", 0..6),
    ) {
        let mut rules = rules;
        rules.reserved_table_prefixes = prefixes.iter().map(|prefix| (*prefix).to_owned()).collect();
        let naming = Naming::new(rules);
        let path = TablePath::new(&segments).unwrap();
        if let Ok(name) = naming.table(&path, &taken) {
            prop_assert!(naming.admits_table(&name), "{} for {:?}", name, segments);
            prop_assert!(!taken.contains(&name), "{} is taken", name);
        }
    }
}

#[test]
fn a_name_whose_hash_falls_under_a_reserved_prefix_is_escaped_instead() {
    let mut lower = rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 63);
    lower.reserved_table_prefixes = BTreeSet::from(["pragma_".to_owned()]);
    let naming = Naming::new(lower);
    let taken = BTreeSet::from(["pragma".to_owned()]);
    let path = TablePath::new(["Pragma"]).unwrap();
    let name = naming.table(&path, &taken).unwrap();
    assert_eq!(name, "_pragma");
    assert_eq!(
        naming.table(&path, &taken).unwrap(),
        name,
        "the same every time"
    );
}

#[test]
fn a_source_column_asking_for_a_metadata_name_is_refused_under_every_rule() {
    use IdentifierCase as Case;
    use IdentifierChars as Chars;
    for case in [Case::Preserve, Case::Lower, Case::Upper] {
        for chars in [Chars::Any, Chars::AsciiWord] {
            let naming = Naming::new(rules(case, chars, 63));
            for column in super::METADATA {
                let spellings = [
                    column.to_owned(),
                    column.to_uppercase(),
                    column.replacen('_', "-", 2),
                    format!("{column}_"),
                ];
                for spelling in spellings {
                    let mut names = NameMap::default();
                    let keys = BTreeSet::from([source(&[&spelling])]);
                    let assigned = naming.assign_columns(&mut names, &keys);
                    let asks = naming.clean(&spelling) == naming.clean(column);
                    if let Err(error) = assigned {
                        assert!(asks, "{case:?} {chars:?} {spelling:?}: {error}");
                        assert_eq!(error.code(), Some(super::COLUMN_NAME_RESERVED));
                        assert_eq!(error.kind(), crate::ErrorKind::Schema);
                        assert!(names.is_empty());
                    } else {
                        assert!(!asks, "{case:?} {chars:?} {spelling:?} is not refused");
                        let name = names.get(&source(&[&spelling])).unwrap();
                        assert!(!naming.is_metadata(name), "{name}");
                    }
                }
            }
        }
    }
}

#[test]
fn a_source_column_never_takes_a_metadata_identifier_its_table_lacks() {
    let mut reserving = rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 63);
    reserving.reserved.insert("_rdlt_seq".to_owned());
    let naming = Naming::new(reserving);
    let seq = naming.metadata("_rdlt_seq");
    assert!(hashed(&seq, "_rdlt_seq"), "{seq}");
    let assigned = assign(&naming, &[source(&[&seq])]);
    let name = &assigned[&source(&[&seq])];
    assert_ne!(name.as_str(), &*seq);
    assert!(!naming.is_metadata(name), "{name}");
}

#[test]
fn state_naming_a_column_as_a_metadata_column_is_refused() {
    let naming = lower();
    for column in super::METADATA {
        let mut names = NameMap::default();
        names
            .insert(source(&["note"]), naming.metadata(column).to_string())
            .unwrap();
        let table = rdlt_connector::TableState {
            names,
            ..rdlt_connector::TableState::default()
        };
        let state = rdlt_connector::PipelineState {
            tables: BTreeMap::from([(TablePath::new(["orders"]).unwrap(), table)]),
            ..rdlt_connector::PipelineState::default()
        };
        let error = super::recorded::check(&naming, &state).unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (crate::ErrorKind::Destination, Some("state_invalid")),
            "{column}"
        );
    }
}

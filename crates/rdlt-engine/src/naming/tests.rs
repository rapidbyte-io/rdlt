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
    naming
        .assign_columns(&mut names, &keys, &["_rdlt_load_id"])
        .unwrap();
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
fn reserved_words_and_metadata_columns_are_never_assigned() {
    let assigned = assign(&lower(), &[source(&["SELECT"]), source(&["_rdlt_load_id"])]);
    assert!(
        hashed(&assigned[&source(&["SELECT"])], "select"),
        "{assigned:?}"
    );
    assert!(
        hashed(&assigned[&source(&["_rdlt_load_id"])], "_rdlt_load_id"),
        "{assigned:?}"
    );
}

#[test]
fn long_names_are_cut_to_fit_with_their_hash() {
    let naming = Naming::new(rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 10));
    let first = source(&["abcdefghijklmnop"]);
    let second = source(&["abcdefghijXYZ"]);
    let assigned = assign(&naming, &[first.clone(), second.clone()]);
    assert_eq!(assigned[&second], "abcdefghij");
    assert!(hashed(&assigned[&first], "abc"), "{assigned:?}");
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
    naming.assign_columns(&mut names, &first, &[]).unwrap();
    let second = BTreeSet::from([source(&["B"]), source(&["b"])]);
    naming.assign_columns(&mut names, &second, &[]).unwrap();
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
    (case, chars, 8_u16..16).prop_map(|(case, chars, max_len)| rules(case, chars, max_len))
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
        naming.assign_columns(&mut names, &first, &["_rdlt_load_id"]).unwrap();
        let before = names.clone();
        let all: BTreeSet<ColumnKey> = keys.iter().cloned().collect();
        naming.assign_columns(&mut names, &all, &["_rdlt_load_id"]).unwrap();
        for (key, name) in before.iter() {
            prop_assert_eq!(names.get(key), Some(name), "stable");
        }
        let mut seen = BTreeSet::new();
        for (key, name) in names.iter() {
            prop_assert!(seen.insert(name.to_owned()), "{} is assigned twice", name);
            prop_assert!(!name.is_empty() && name.len() <= usize::from(rules.max_len.get()), "{}", name);
            prop_assert!(name != "_rdlt_load_id" && name.to_lowercase() != "select", "{}", name);
            if rules.chars == IdentifierChars::AsciiWord {
                prop_assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'), "{}", name);
            }
            prop_assert!(all.contains(key));
        }
        let settled = names.clone();
        naming.assign_columns(&mut names, &all, &["_rdlt_load_id"]).unwrap();
        prop_assert_eq!(names, settled, "idempotent");
    }
}

#[test]
fn metadata_columns_follow_the_rules_and_never_share_an_identifier() {
    let upper = Naming::new(rules(IdentifierCase::Upper, IdentifierChars::AsciiWord, 63));
    assert_eq!(
        upper.metadata("_rdlt_load_id", &BTreeSet::new()).unwrap(),
        "_RDLT_LOAD_ID"
    );
    let short = Naming::new(rules(IdentifierCase::Lower, IdentifierChars::AsciiWord, 9));
    let first = short.metadata("_rdlt_load_id", &BTreeSet::new()).unwrap();
    let second = short
        .metadata("_rdlt_loaded_at", &BTreeSet::from([first.clone()]))
        .unwrap();
    assert_eq!(first, "_rdlt_loa");
    assert_ne!(first, second);
    assert!(second.len() <= 9);
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

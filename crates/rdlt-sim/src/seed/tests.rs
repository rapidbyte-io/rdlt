use super::{SEED_VAR, SEEDS_FROM_VAR, SEEDS_VAR, Seed, SeedRange, select};

fn seeds(range: SeedRange) -> Vec<u64> {
    range.seeds().map(Seed::value).collect()
}

#[test]
fn seed_selection_follows_the_variables() {
    type Case<'a> = (Option<&'a str>, Option<&'a str>, Option<&'a str>, &'a [u64]);
    let cases: &[Case<'_>] = &[
        (None, None, None, &[0, 1, 2]),
        (None, Some("2"), None, &[0, 1]),
        (None, Some(""), None, &[0, 1, 2]),
        (Some("42"), Some("9"), None, &[42]),
        (Some(" 7 "), None, None, &[7]),
        (Some(""), Some("1"), None, &[0]),
        (Some("18446744073709551615"), None, None, &[u64::MAX]),
        (None, Some("0"), None, &[]),
        // A shard of many seeds starts where the shard before it ended.
        (None, Some("2"), Some("100"), &[100, 101]),
        (None, None, Some(" 5 "), &[5, 6, 7]),
        (None, Some("2"), Some(""), &[0, 1]),
        (Some("42"), None, Some("100"), &[42]),
        (
            None,
            Some("2"),
            Some("18446744073709551615"),
            &[u64::MAX, 0],
        ),
    ];
    for (single, count, from, expected) in cases {
        let selected = select(*single, *count, *from, 3).unwrap();
        assert_eq!(
            seeds(selected),
            *expected,
            "single {single:?}, count {count:?}, from {from:?}"
        );
    }
}

#[test]
fn malformed_variables_name_the_variable() {
    let cases = [
        (Some("abc"), None, None, SEED_VAR),
        (None, Some("-1"), None, SEEDS_VAR),
        (None, None, Some("x"), SEEDS_FROM_VAR),
    ];
    for (single, count, from, variable) in cases {
        let error = select(single, count, from, 3).unwrap_err();
        assert_eq!(error.variable, variable);
    }
}

#[test]
fn a_seed_displays_as_its_value() {
    assert_eq!(Seed::new(1234).to_string(), "1234");
}

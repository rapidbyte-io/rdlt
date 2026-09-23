use super::{SEED_VAR, SEEDS_VAR, Seed, SeedRange, select};

fn seeds(range: SeedRange) -> Vec<u64> {
    range.seeds().map(Seed::value).collect()
}

#[test]
fn seed_selection_follows_the_variables() {
    let cases: &[(Option<&str>, Option<&str>, &[u64])] = &[
        (None, None, &[0, 1, 2]),
        (None, Some("2"), &[0, 1]),
        (None, Some(""), &[0, 1, 2]),
        (Some("42"), Some("9"), &[42]),
        (Some(" 7 "), None, &[7]),
        (Some(""), Some("1"), &[0]),
        (Some("18446744073709551615"), None, &[u64::MAX]),
        (None, Some("0"), &[]),
    ];
    for (single, count, expected) in cases {
        let selected = select(*single, *count, 3).unwrap();
        assert_eq!(
            seeds(selected),
            *expected,
            "single {single:?}, count {count:?}"
        );
    }
}

#[test]
fn malformed_variables_name_the_variable() {
    let cases = [(Some("abc"), None, SEED_VAR), (None, Some("-1"), SEEDS_VAR)];
    for (single, count, variable) in cases {
        let error = select(single, count, 3).unwrap_err();
        assert_eq!(error.variable, variable);
    }
}

#[test]
fn a_seed_displays_as_its_value() {
    assert_eq!(Seed::new(1234).to_string(), "1234");
}

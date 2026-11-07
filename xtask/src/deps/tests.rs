use super::{Edge, Violation, absent, check};

fn edge(from: &str, to: &str, dev: bool) -> Edge {
    Edge {
        from: from.to_owned(),
        to: to.to_owned(),
        dev,
    }
}

fn crates(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn allowed_edges_pass() {
    let edges = [
        edge("rdlt-sim", "rdlt-engine", false),
        edge("rdlt-engine", "rdlt-connector", false),
        edge("rdlt-host", "rdlt-connector-reference", true),
    ];
    let names = crates(&[
        "rdlt-engine",
        "rdlt-sim",
        "rdlt-connector",
        "rdlt-host",
        "rdlt-connector-reference",
    ]);
    assert_eq!(check(&names, &edges), Vec::new());
}

#[test]
fn forbidden_edges_are_reported() {
    let cases = [
        edge("rdlt-engine", "rdlt-host", false),
        edge("rdlt-connector", "rdlt-engine", false),
        edge("rdlt-sim", "xtask", true),
        edge("rdlt-engine", "rdlt-sim", true),
    ];
    for case in cases {
        let names = crates(&[&case.from, &case.to]);
        assert_eq!(
            check(&names, std::slice::from_ref(&case)),
            vec![Violation::Forbidden(case)]
        );
    }
}

#[test]
fn a_crate_missing_from_the_rules_is_reported() {
    let names = crates(&["rdlt-engine", "rdlt-new"]);
    assert_eq!(
        check(&names, &[]),
        vec![Violation::Unlisted("rdlt-new".to_owned())]
    );
}

#[test]
fn a_crate_the_rules_name_that_the_workspace_lacks_is_reported() {
    let rules: &[(&str, &[&str])] = &[
        ("rdlt-a", &["rdlt-b"]),
        ("rdlt-b", &[]),
        ("rdlt-c", &["rdlt-gone"]),
    ];
    let leaves = ["rdlt-a", "rdlt-leaf"];
    let every = crates(&["rdlt-a", "rdlt-b", "rdlt-c", "rdlt-gone", "rdlt-leaf"]);
    assert_eq!(absent(&every, rules, &leaves), Vec::<&str>::new());
    let lacking = crates(&["rdlt-a", "rdlt-b", "rdlt-c"]);
    assert_eq!(absent(&lacking, rules, &leaves), ["rdlt-gone", "rdlt-leaf"]);
}

// The audited crate's functions are sound only as their callers use them: the connector crate
// adopts a host's socket, the host crate spawns with no inherited descriptor, and no other
// crate may reach either, in its tests either.
#[test]
fn only_the_connector_and_host_crates_use_the_audited_crate() {
    let names = crates(&["rdlt-adopt", "rdlt-connector", "rdlt-host", "rdlt-engine"]);
    let allowed = [
        edge("rdlt-connector", "rdlt-adopt", false),
        edge("rdlt-host", "rdlt-adopt", false),
    ];
    assert_eq!(check(&names, &allowed), Vec::new());
    for case in [
        edge("rdlt-engine", "rdlt-adopt", false),
        edge("rdlt-host", "rdlt-adopt", true),
        edge("rdlt-connector", "rdlt-adopt", true),
    ] {
        assert_eq!(
            check(&names, std::slice::from_ref(&case)),
            vec![Violation::Forbidden(case)]
        );
    }
}

#[test]
fn the_python_extension_uses_the_sdk_and_certification_and_nothing_uses_it() {
    let crates = crates(&[
        "rdlt-certify",
        "rdlt-connector",
        "rdlt-engine",
        "rdlt-host",
        "rdlt-python",
    ]);
    for allowed in [
        edge("rdlt-python", "rdlt-certify", false),
        edge("rdlt-python", "rdlt-connector", false),
        edge("rdlt-python", "rdlt-engine", true),
    ] {
        let forbidden = check(&crates, std::slice::from_ref(&allowed));
        assert!(
            !forbidden.contains(&Violation::Forbidden(allowed.clone())),
            "{allowed:?}"
        );
    }
    for refused in [
        edge("rdlt-python", "rdlt-engine", false),
        edge("rdlt-python", "rdlt-host", false),
        edge("rdlt-connector", "rdlt-python", false),
        edge("rdlt-certify", "rdlt-python", true),
    ] {
        let found = check(&crates, std::slice::from_ref(&refused));
        assert!(
            found.contains(&Violation::Forbidden(refused.clone())),
            "{refused:?}"
        );
    }
}

#[test]
fn rdlt_sql_builds_on_the_engine_and_nothing_below_it_uses_it_even_in_tests() {
    let names = crates(&[
        "rdlt-sql",
        "rdlt-engine",
        "rdlt-connector",
        "rdlt-host",
        "rdlt-wire",
        "rdlt-certify",
    ]);
    let allowed = [
        edge("rdlt-sql", "rdlt-engine", false),
        edge("rdlt-sql", "rdlt-connector", false),
        edge("rdlt-sql", "rdlt-certify", true),
    ];
    assert_eq!(check(&names, &allowed), Vec::new());
    for from in ["rdlt-engine", "rdlt-connector", "rdlt-host", "rdlt-wire"] {
        for dev in [false, true] {
            let case = edge(from, "rdlt-sql", dev);
            assert_eq!(
                check(&names, std::slice::from_ref(&case)),
                vec![Violation::Forbidden(case)]
            );
        }
    }
}

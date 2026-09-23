use super::{Edge, Violation, check};

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
        edge("rdlt-engine", "rdlt-sim", true),
    ];
    let names = crates(&["rdlt-engine", "rdlt-sim", "rdlt-connector"]);
    assert_eq!(check(&names, &edges), Vec::new());
}

#[test]
fn forbidden_edges_are_reported() {
    let cases = [
        edge("rdlt-engine", "rdlt-host", false),
        edge("rdlt-connector", "rdlt-engine", false),
        edge("rdlt-engine", "rdlt-cli", true),
        edge("rdlt-sim", "xtask", true),
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

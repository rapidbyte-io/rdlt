use std::path::Path;

use super::{changed, committed};

#[test]
fn a_change_lists_what_was_added_then_what_was_removed() {
    let committed = "pub fn rdlt::a()\npub fn rdlt::b()\n";
    let listed = "pub fn rdlt::b()\npub fn rdlt::c()\n";
    assert_eq!(
        changed(committed, listed),
        ["+ pub fn rdlt::c()", "- pub fn rdlt::a()"]
    );
    assert!(changed(listed, listed).is_empty());
}

#[test]
fn a_snapshot_with_the_same_lines_says_how_it_differs() {
    let listed = "pub fn rdlt::a()\npub fn rdlt::b()\n";
    for (snapshot, said) in [
        ("pub fn rdlt::a()\r\npub fn rdlt::b()\r\n", "CRLF"),
        ("pub fn rdlt::a()\npub fn rdlt::b()", "last line"),
        ("pub fn rdlt::b()\npub fn rdlt::a()\n", "order"),
        (
            "pub fn rdlt::a()\npub fn rdlt::a()\npub fn rdlt::b()\n",
            "repeats",
        ),
    ] {
        let differences = changed(snapshot, listed);
        assert!(
            differences.iter().any(|note| note.contains(said)),
            "{differences:?}"
        );
    }
}

#[test]
fn a_snapshot_that_cannot_be_read_is_an_error_naming_it() {
    let missing = Path::new("/nonexistent/public-api.txt");
    let refused = committed(missing).expect_err("no such file");
    assert!(
        refused.to_string().contains("/nonexistent/public-api.txt"),
        "{refused}"
    );
}

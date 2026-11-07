use super::changed;

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

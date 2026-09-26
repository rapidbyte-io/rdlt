use crate::workspace_root;

#[test]
fn the_committed_code_is_what_the_protocol_generates() {
    let root = workspace_root();
    let committed = std::fs::read_to_string(root.join(super::GENERATED)).unwrap();
    assert!(
        super::generate(&root).unwrap() == committed,
        "run `cargo xtask codegen`"
    );
}

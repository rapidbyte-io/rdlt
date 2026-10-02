use crate::workspace_root;

#[test]
fn the_committed_code_is_what_the_protocol_generates() {
    let root = workspace_root();
    let (generated, forms) = super::generate(&root).unwrap();
    for (file, code) in [(super::GENERATED, generated), (super::FORMS, forms)] {
        let committed = std::fs::read_to_string(root.join(file)).unwrap();
        assert!(code == committed, "{file}: run `cargo xtask codegen`");
    }
}

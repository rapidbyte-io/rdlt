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

#[test]
fn the_data_calls_are_no_calls_of_the_service_and_their_messages_have_forms() {
    let (generated, forms) = super::generate(&workspace_root()).unwrap();
    for method in ["Write", "Read", "ReadPublished"] {
        assert!(
            !generated.contains(&format!("Connector/{method}\"")),
            "{method} is a stream of its own, no call of the service"
        );
    }
    assert!(
        generated.contains("pub async fn heartbeat("),
        "the client has the rest"
    );
    assert!(
        generated.contains("async fn heartbeat("),
        "so does the server"
    );
    assert!(
        generated.contains("pub async fn read_acknowledged("),
        "a read's probe is a call of the service"
    );
    for message in [
        "READ_START",
        "READ_CONTROL",
        "READ_FRAME",
        "WRITE_START",
        "WRITE_FRAME",
        "WRITE_ACK",
        "READ_PUBLISHED_REQUEST",
    ] {
        assert!(
            forms.contains(&format!("pub(crate) static {message}: Form")),
            "a stream's receiver scans its messages by their forms: {message}"
        );
    }
}

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
fn the_data_plane_is_left_out_of_the_generated_service_and_its_messages_are_not() {
    let (generated, forms) = super::generate(&workspace_root()).unwrap();
    assert!(
        !generated.contains("Connector/Write\""),
        "neither end of the service calls Write"
    );
    assert!(
        !generated.contains("fn write("),
        "no Write in client or server"
    );
    assert!(
        generated.contains("pub async fn heartbeat("),
        "the client has the rest"
    );
    assert!(
        generated.contains("async fn heartbeat("),
        "so does the server"
    );
    for form in [
        "\"Write\" => Some(&WRITE_FRAME)",
        "\"Write\" => Some(&WRITE_ACK)",
    ] {
        assert!(
            forms.contains(form),
            "the forms keep the data plane's messages: {form}"
        );
    }
}

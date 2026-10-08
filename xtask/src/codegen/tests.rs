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
    for method in ["Write", "Read", "ReadPublished"] {
        assert!(
            !generated.contains(&format!("Connector/{method}\"")),
            "neither end of the service calls {method}"
        );
    }
    for method in ["fn write(", "fn read(", "fn read_published("] {
        assert!(
            !generated.contains(method),
            "no {method} in client or server"
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
        "a read's probe is no data-plane call"
    );
    for form in [
        "\"Write\" => Some(&WRITE_FRAME)",
        "\"Write\" => Some(&WRITE_ACK)",
        "\"Read\" => Some(&READ_CONTROL)",
        "\"Read\" => Some(&READ_FRAME)",
        "\"ReadPublished\" => Some(&READ_PUBLISHED_REQUEST)",
        "\"ReadPublished\" => Some(&READ_FRAME)",
    ] {
        assert!(
            forms.contains(form),
            "the forms keep the data plane's messages: {form}"
        );
    }
}

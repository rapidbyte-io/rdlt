use rdlt_connector::ConnectContext;

use crate::embedded;
use crate::source::NativeSourceFactory;

#[test]
fn a_served_python_connector_serves_no_probe() {
    let factory = embedded::with(|py| {
        let connect = py
            .eval(c"lambda config: None", None, None)
            .unwrap()
            .unbind();
        NativeSourceFactory::new("io.example.served", "1.0.0".to_owned(), "{}", connect)
            .unwrap()
            .factory()
    });
    assert!(!factory.acknowledges());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let asked = runtime
        .block_on(factory.connect_acknowledging(serde_json::json!({}), ConnectContext::new()));
    assert_eq!(asked.err().expect("refused").code(), Some("acknowledged"));
}

#[test]
fn bad_arguments_are_answered_without_serving() {
    embedded::with(|py| {
        let connect = py
            .eval(c"lambda config: None", None, None)
            .unwrap()
            .unbind();
        let factory =
            NativeSourceFactory::new("io.example.served", "1.0.0".to_owned(), "{}", connect)
                .unwrap();
        let said = super::serve(py, vec!["--no-such-flag".to_owned()], &factory).expect("refused");
        assert!(said.contains("--no-such-flag"));
    });
}

use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, Py, PyAny, Python};
use rdlt_connector::ConnectContext;

use crate::destination::NativeDestinationFactory;
use crate::embedded;
use crate::source::NativeSourceFactory;

fn connect(py: Python<'_>) -> Py<PyAny> {
    py.eval(c"lambda config, host: None", None, None)
        .unwrap()
        .unbind()
}

fn source(py: Python<'_>, acknowledges: bool) -> NativeSourceFactory {
    NativeSourceFactory::new(
        "io.example.served",
        "1.0.0".to_owned(),
        "{}",
        acknowledges,
        connect(py),
    )
    .unwrap()
}

fn destination(py: Python<'_>) -> NativeDestinationFactory {
    NativeDestinationFactory::new(
        "io.example.served",
        "1.0.0".to_owned(),
        "{}",
        true,
        connect(py),
    )
    .unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

#[test]
fn a_served_python_connector_serves_no_probe_unless_its_launcher_asks() {
    // Even a source whose class says it tells where it stands.
    for acknowledges in [false, true] {
        let factory = embedded::with(|py| super::source_served(&source(py, acknowledges), false));
        assert!(!factory.acknowledges());
        let asked = runtime()
            .block_on(factory.connect_acknowledging(serde_json::json!({}), ConnectContext::new()));
        assert_eq!(asked.err().expect("refused").code(), Some("acknowledged"));
        let asked = embedded::with(|py| super::source_served(&source(py, acknowledges), true));
        assert_eq!(asked.acknowledges(), acknowledges, "as its class says");
    }
}

#[test]
fn a_served_python_destination_reads_back_only_where_its_launcher_asks() {
    let factory = embedded::with(|py| super::destination_served(&destination(py), false));
    assert!(!factory.reads_back());
    let asked =
        runtime().block_on(factory.connect_reading(serde_json::json!({}), ConnectContext::new()));
    assert_eq!(asked.err().expect("refused").code(), Some("published"));
    let asked = embedded::with(|py| super::destination_served(&destination(py), true));
    assert!(asked.reads_back());
}

#[test]
fn bad_arguments_are_answered_before_serving_either_role_or_both() {
    embedded::with(|py| {
        let source = || Some((Bound::new(py, source(py, false)).unwrap(), false));
        let destination = || Some((Bound::new(py, destination(py)).unwrap(), true));
        for (source, destination) in [
            (source(), None),
            (None, destination()),
            (source(), destination()),
        ] {
            let args = vec!["--no-such-flag".to_owned()];
            let said = super::serve(py, args, source, destination).expect("refused");
            assert!(said.contains("--no-such-flag"), "{said}");
        }
        let nothing = super::serve(py, vec!["--rdlt-fd".to_owned(), "3".to_owned()], None, None);
        assert!(nothing.expect("refused").contains("nothing is served"));
    });
}

#[test]
fn a_source_s_and_a_destination_s_factories_alone_are_served() {
    embedded::with(|py| {
        let served = py.import("rdlt._native").unwrap().getattr("serve").unwrap();
        let text = || {
            pyo3::types::PyString::new(py, "a factory")
                .into_any()
                .unbind()
        };
        for given in [(text(), py.None()), (py.None(), text())] {
            let refused = served
                .call1((vec!["--no-such-flag"], given.0, given.1))
                .unwrap_err();
            assert!(
                refused.is_instance_of::<pyo3::exceptions::PyTypeError>(py),
                "{refused}"
            );
        }
    });
}

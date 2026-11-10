use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, Py, PyAny, Python};
use rdlt_connector::ConnectContext;

use super::Factory;
use crate::destination::{NativeDestinationFactory, minimal_capabilities};
use crate::embedded;
use crate::source::NativeSourceFactory;

fn connect(py: Python<'_>) -> Py<PyAny> {
    py.eval(c"lambda config: None", None, None)
        .unwrap()
        .unbind()
}

fn source(py: Python<'_>) -> NativeSourceFactory {
    NativeSourceFactory::new("io.example.served", "1.0.0".to_owned(), "{}", connect(py)).unwrap()
}

fn destination(py: Python<'_>) -> NativeDestinationFactory {
    let capabilities = minimal_capabilities();
    NativeDestinationFactory::new(
        "io.example.served",
        "1.0.0".to_owned(),
        "{}",
        &capabilities,
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
fn a_served_python_connector_serves_no_probe() {
    let factory = embedded::with(|py| source(py).factory());
    assert!(!factory.acknowledges());
    let asked = runtime()
        .block_on(factory.connect_acknowledging(serde_json::json!({}), ConnectContext::new()));
    assert_eq!(asked.err().expect("refused").code(), Some("acknowledged"));
}

#[test]
fn a_served_python_destination_reads_nothing_back() {
    let factory = embedded::with(|py| destination(py).factory());
    assert!(!factory.reads_back());
    let asked =
        runtime().block_on(factory.connect_reading(serde_json::json!({}), ConnectContext::new()));
    assert_eq!(asked.err().expect("refused").code(), Some("published"));
}

#[test]
fn bad_arguments_are_answered_before_serving_for_either_factory() {
    embedded::with(|py| {
        let factories = [
            Factory::Source(Bound::new(py, source(py)).unwrap()),
            Factory::Destination(Bound::new(py, destination(py)).unwrap()),
        ];
        for factory in factories {
            let said =
                super::serve(py, vec!["--no-such-flag".to_owned()], factory).expect("refused");
            assert!(said.contains("--no-such-flag"), "{said}");
        }
    });
}

#[test]
fn only_a_source_or_a_destination_is_served() {
    embedded::with(|py| {
        let served = py.import("rdlt._native").unwrap().getattr("serve").unwrap();
        let refused = served
            .call1((vec!["--no-such-flag"], "a factory"))
            .unwrap_err();
        assert!(
            refused.is_instance_of::<pyo3::exceptions::PyTypeError>(py),
            "{refused}"
        );
    });
}

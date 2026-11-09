use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};
use rdlt_connector::ConnectorErrorKind;

use super::{Failure, from_report, type_name};
use crate::embedded;

fn report(kind: &str) -> String {
    serde_json::json!({
        "kind": kind, "origin": "source", "stream": "orders", "code": "source_failed",
        "connector_code": "ValueError", "message": "stream orders: ValueError: bad row",
        "causes": ["Traceback (most recent call last):\n  ...\nValueError: bad row\n"],
        "retryable": false,
    })
    .to_string()
}

#[test]
fn each_kind_raises_its_class_and_an_unknown_one_the_base() {
    embedded::with(|py| {
        for (kind, class) in [
            ("config", "ConfigError"),
            ("schema", "SchemaError"),
            ("source", "SourceError"),
            ("destination", "DestinationError"),
            ("transform", "TransformError"),
            ("fenced", "FencedError"),
            ("memory", "MemoryBudgetError"),
            ("cancelled", "StoppedError"),
            ("internal", "InternalError"),
            ("io", "Error"),
        ] {
            let error = from_report(py, &report(kind));
            assert_eq!(
                error.get_type(py).name().unwrap().to_string(),
                class,
                "{kind}"
            );
            let value = error.value(py);
            assert_eq!(
                value.getattr("kind").unwrap().extract::<String>().unwrap(),
                kind
            );
            assert_eq!(
                value
                    .getattr("stream")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "orders"
            );
            assert_eq!(
                value.getattr("code").unwrap().extract::<String>().unwrap(),
                "source_failed"
            );
            assert!(
                !value
                    .getattr("retryable")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
            let causes = value
                .call_method0("report")
                .unwrap()
                .getattr("causes")
                .unwrap();
            assert_eq!(causes.len().unwrap(), 1, "{kind}");
        }
    });
}

#[test]
fn a_type_is_named_with_its_module_unless_that_is_builtins_or_empty() {
    embedded::with(|py| {
        for (code, name) in [
            (c"raise ValueError('bad row 7')", "ValueError"),
            (
                c"class ApiError(Exception): pass\nApiError.__module__ = 'tickets'\nraise ApiError('gone')",
                "tickets.ApiError",
            ),
            (
                c"class Bare(Exception): pass\nBare.__module__ = ''\nraise Bare()",
                "Bare",
            ),
        ] {
            let raised = py.run(code, None, None).unwrap_err();
            assert_eq!(type_name(py, &raised), name, "{code:?}");
        }
    });
}

#[test]
fn a_python_exception_says_its_type_its_message_and_its_traceback() {
    embedded::with(|py| {
        let raised = py
            .run(c"raise ValueError('bad row 7')", None, None)
            .unwrap_err();
        let failure = Failure::of(py, &raised);
        assert_eq!(failure.type_name, "ValueError");
        assert_eq!(failure.message, "bad row 7");
        assert!(
            failure
                .traceback
                .starts_with("Traceback (most recent call last):")
        );
        assert!(
            failure
                .traceback
                .trim_end()
                .ends_with("ValueError: bad row 7")
        );
        let error = failure.connector_error();
        assert_eq!(error.kind(), ConnectorErrorKind::Internal);
        assert_eq!(error.code(), Some("ValueError"));
        let cause = std::error::Error::source(&error).expect("the traceback is its cause");
        assert_eq!(cause.to_string(), failure.traceback);
    });
}

#[test]
fn a_class_of_its_module_is_named_with_its_module() {
    embedded::with(|py| {
        let code = c"import types\nclass ApiError(Exception): pass\nApiError.__module__ = 'tickets'\nraise ApiError('gone')";
        let failure = Failure::of(py, &py.run(code, None, None).unwrap_err());
        assert_eq!(failure.type_name, "tickets.ApiError");
    });
}

#[test]
fn timeouts_and_lost_connections_are_transient_and_nothing_else_is() {
    embedded::with(|py| {
        for (code, transient) in [
            (c"raise TimeoutError()", true),
            (c"raise ConnectionResetError()", true),
            (c"raise OSError()", false),
            (c"raise KeyError('k')", false),
        ] {
            let failure = Failure::of(py, &py.run(code, None, None).unwrap_err());
            let kind = failure.connector_error().kind();
            let expected = if transient {
                ConnectorErrorKind::Transient
            } else {
                ConnectorErrorKind::Internal
            };
            assert_eq!(kind, expected, "{code:?}");
        }
    });
}

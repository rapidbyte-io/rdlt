use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};

use super::{from_report, type_name};
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

use proptest::prelude::*;
use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};
use rdlt_connector::ConnectorErrorKind;
use rdlt_connector::limits::MAX_ERROR_CODE_BYTES;

use super::{Failure, coded, from_report, type_name};
use crate::embedded;

fn report(kind: &str) -> String {
    serde_json::json!({
        "kind": kind, "origin": "source", "stream": "orders", "code": "source_failed",
        "connector_code": "value_error", "message": "stream orders: ValueError: bad row",
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
        assert_eq!(error.code(), Some("value_error"));
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

/// The connector error `code`, which raises, fails with.
fn connector_error(py: pyo3::Python<'_>, code: &str) -> rdlt_connector::ConnectorError {
    let code = format!("from rdlt.connector import *\n{code}");
    let raised = py.run(&std::ffi::CString::new(code).unwrap(), None, None);
    Failure::of(py, &raised.unwrap_err()).connector_error()
}

const FAILURES: &[(&str, ConnectorErrorKind, &str)] = &[
    (
        "raise Transient('the API is restarting')",
        ConnectorErrorKind::Transient,
        "transient",
    ),
    (
        "raise RateLimited('slow down', retry_after=2.5, code='http_429')",
        ConnectorErrorKind::RateLimited,
        "http_429",
    ),
    (
        "raise InvalidConfig('its value is wrong', code='config_invalid')",
        ConnectorErrorKind::Config,
        "config_invalid",
    ),
    (
        "raise Unauthorized('the token was refused')",
        ConnectorErrorKind::Auth,
        "unauthorized",
    ),
    (
        "raise InvalidData('row 7 has no id')",
        ConnectorErrorKind::Data,
        "invalid_data",
    ),
    (
        "raise Unsupported('no such endpoint')",
        ConnectorErrorKind::Unsupported,
        "unsupported",
    ),
    (
        // A class's own name that is one of the host's codes is qualified by its module.
        "class DeadlineExceeded(Transient): pass\nraise DeadlineExceeded('the API timed out')",
        ConnectorErrorKind::Transient,
        "__main__.deadline_exceeded",
    ),
    (
        "raise Fenced('a newer session holds it')",
        ConnectorErrorKind::Fenced,
        "fenced",
    ),
    (
        "raise Fenced('it expects another landed', code='commit_stale')",
        ConnectorErrorKind::Fenced,
        "commit_stale",
    ),
    (
        "raise TimeoutError()",
        ConnectorErrorKind::Transient,
        "timeout_error",
    ),
    (
        "raise ValueError('bad')",
        ConnectorErrorKind::Internal,
        "value_error",
    ),
];

#[test]
fn each_failure_class_is_its_kind_with_its_code() {
    embedded::with(|py| {
        for (code, kind, coded) in FAILURES {
            let error = connector_error(py, code);
            assert_eq!(error.kind(), *kind, "{code}");
            assert_eq!(error.code(), Some(*coded), "{code}");
        }
    });
}

#[test]
fn a_declared_failure_says_its_message_and_a_rate_limit_its_wait() {
    embedded::with(|py| {
        let limited = connector_error(py, "raise RateLimited('slow down', retry_after=2.5)");
        assert_eq!(
            limited.retry_after(),
            Some(std::time::Duration::from_millis(2500))
        );
        assert_eq!(limited.to_string(), "slow down");
        assert_eq!(
            connector_error(py, "raise RateLimited('slow down')").retry_after(),
            None
        );
        assert_eq!(
            connector_error(
                py,
                "raise RateLimited('slow down', retry_after=float('nan'))"
            )
            .retry_after(),
            None,
            "a wait that is not a duration is no hint"
        );
        let refused = connector_error(
            py,
            "raise InvalidConfig('config field port: its value is of the wrong type')",
        );
        assert_eq!(
            refused.to_string(),
            "config field port: its value is of the wrong type"
        );
        assert!(
            std::error::Error::source(&refused).is_none(),
            "a refused configuration carries no traceback"
        );
    });
}

#[test]
fn a_type_s_name_is_its_code_in_snake_case() {
    for (name, code) in [
        ("RuntimeError", "runtime_error"),
        ("tickets.ApiError", "tickets.api_error"),
        ("HTTPError", "http_error"),
        ("Error2Retry", "error2_retry"),
        ("__main__.Boom", "__main__.boom"),
        ("pkg.Outer.Inner", "pkg.outer.inner"),
        ("Ошибка", "______"),
    ] {
        assert_eq!(coded(name), code, "{name}");
    }
}

proptest! {
    #[test]
    fn every_code_is_one_the_protocol_admits(name in "\\PC{1,200}") {
        let code = coded(&name);
        prop_assert!(!code.is_empty());
        prop_assert!(code.len() <= MAX_ERROR_CODE_BYTES);
        let admitted =
            |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || "_.-".contains(c);
        prop_assert!(code.chars().all(admitted), "{code}");
    }
}

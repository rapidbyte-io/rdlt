//! Errors across the boundary: the facade's as `rdlt.errors`' classes, and a Python exception's
//! type as a store's refusal names it.

#[cfg(test)]
mod tests;

use pyo3::exceptions::{PyConnectionError, PyTimeoutError};
use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};
use pyo3::{PyErr, Python};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};

/// The `rdlt.errors` exception for `error`.
pub(crate) fn raised(py: Python<'_>, error: &rdlt::Error) -> PyErr {
    let report = serde_json::to_string(&error.report()).expect("an error report serializes");
    from_report(py, &report)
}

/// The `rdlt.errors` exception `report`, an `ErrorReport` as JSON, describes.
pub(crate) fn from_report(py: Python<'_>, report: &str) -> PyErr {
    let built = py
        .import("rdlt.errors")
        .and_then(|errors| errors.call_method1("_from_report", (report,)));
    match built {
        Ok(exception) => PyErr::from_value(exception),
        Err(error) => error,
    }
}

/// An `rdlt.InternalError` saying `message`, for a failure of this module's own.
pub(crate) fn internal(py: Python<'_>, message: &str) -> PyErr {
    own(py, "internal", None, message)
}

/// An `rdlt.ConfigError` coded `code` and saying `message`, for a refusal of this module's own.
pub(crate) fn config(py: Python<'_>, code: rdlt::Code, message: &str) -> PyErr {
    own(py, "config", Some(code.as_str()), message)
}

/// The `rdlt.StoppedError` a run cancelled before its end answers.
pub(crate) fn stopped(py: Python<'_>) -> PyErr {
    own(
        py,
        "cancelled",
        None,
        "the run was cancelled before it ended",
    )
}

/// The exception of `kind` for a failure of this module's own.
fn own(py: Python<'_>, kind: &str, code: Option<&str>, message: &str) -> PyErr {
    let report = serde_json::json!({
        "kind": kind, "origin": "engine", "stream": null, "code": code,
        "connector_code": null, "message": message, "causes": [], "retryable": false,
    });
    from_report(py, &report.to_string())
}

/// `error`'s type, with its module where that is not `builtins`: `tickets.ApiError`.
pub(crate) fn type_name(py: Python<'_>, error: &PyErr) -> String {
    let kind = error.get_type(py);
    let name = kind
        .qualname()
        .map_or_else(|_| "Exception".to_owned(), |name| name.to_string());
    match kind.module().map(|module| module.to_string()) {
        Ok(module) if module != "builtins" && !module.is_empty() => format!("{module}.{name}"),
        _ => name,
    }
}

/// A Python exception as a connector's error says it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Failure {
    /// The exception's type, with its module where that is not `builtins`: `tickets.ApiError`.
    pub(crate) type_name: String,
    /// What `str()` of the exception says.
    pub(crate) message: String,
    /// The traceback as `traceback.format_exception` writes it, its last line the type and
    /// message.
    pub(crate) traceback: String,
    /// The kind of connector error it is: `Transient` for a `TimeoutError` or a
    /// `ConnectionError`, `Internal` for any other.
    pub(crate) kind: ConnectorErrorKind,
}

impl Failure {
    /// What `error` says.
    pub(crate) fn of(py: Python<'_>, error: &PyErr) -> Self {
        let value = error.value(py);
        let message = value.str().map(|text| text.to_string()).unwrap_or_default();
        let traceback = py
            .import("traceback")
            .and_then(|traceback| traceback.call_method1("format_exception", (value,)))
            .and_then(|lines| lines.extract::<Vec<String>>())
            .map(|lines| lines.concat())
            .unwrap_or_default();
        let transient = error.is_instance_of::<PyTimeoutError>(py)
            || error.is_instance_of::<PyConnectionError>(py);
        let kind = if transient {
            ConnectorErrorKind::Transient
        } else {
            ConnectorErrorKind::Internal
        };
        Self {
            type_name: type_name(py, error),
            message,
            traceback,
            kind,
        }
    }

    /// The connector error the failure is, of its kind, its code the exception's type and its
    /// traceback its cause; the host shows and bounds each as connector text.
    pub(crate) fn connector_error(&self) -> ConnectorError {
        let message = format!("{}: {}", self.type_name, self.message);
        ConnectorError::new(self.kind, message)
            .with_code(self.type_name.clone())
            .with_source(Traceback(self.traceback.clone()))
    }
}

/// A Python traceback, as the cause of the error it explains.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct Traceback(pub(crate) String);

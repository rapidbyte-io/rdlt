//! Errors across the boundary: the facade's as `rdlt.errors`' classes, and a Python exception's
//! type as a store's refusal names it.

#[cfg(test)]
mod tests;

use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};
use pyo3::{PyErr, Python};

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

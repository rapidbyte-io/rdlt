//! Errors across the boundary: the facade's as `rdlt.errors`' classes, and a Python exception's
//! type as a store's refusal names it.

#[cfg(test)]
mod tests;

use std::time::Duration;

use pyo3::exceptions::{PyConnectionError, PyTimeoutError};
use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};
use pyo3::{Bound, PyAny, PyErr, Python};
use rdlt_connector::limits::MAX_ERROR_CODE_BYTES;
use rdlt_connector::wire::HOST_CODES;
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
    /// The kind of connector error it is: what a failure class of `rdlt.connector` declares,
    /// else `Transient` for a `TimeoutError` or a `ConnectionError`, `Internal` for any other.
    pub(crate) kind: ConnectorErrorKind,
    /// The machine code the host keeps beside it, as the protocol admits one: a failure class's
    /// own, else the exception's type, `tickets.ApiError` as `tickets.api_error`; a failure
    /// class's name alone where that is none of the host's own codes.
    code: String,
    /// How long before a retry, where a remote system said.
    retry_after: Option<Duration>,
    /// Whether it is a failure class of `rdlt.connector`: said as its author wrote it, with no
    /// traceback.
    declared: bool,
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
        let type_name = type_name(py, error);
        let mut failure = Self {
            code: coded(&type_name),
            type_name,
            message,
            traceback,
            kind,
            retry_after: None,
            declared: false,
        };
        if let Some((kind, code, retry_after)) = declared(py, value) {
            failure.kind = kind;
            let class = coded(failure.type_name.rsplit('.').next().unwrap_or_default());
            failure.code = match code {
                Some(code) => coded(&code),
                // A class named as one of the host's codes keeps a code of its own, served too.
                None if HOST_CODES.contains(&class.as_str()) => coded(&failure.type_name),
                None => class,
            };
            failure.retry_after = retry_after;
            failure.declared = true;
        }
        failure
    }

    /// The connector error the failure is, of its kind and code; the host shows and bounds each
    /// as connector text.
    ///
    /// A failure class of `rdlt.connector` says its message as written, with no cause: its
    /// author chose what it says, and a refused configuration must carry nothing of its input.
    /// Any other exception's message says its type, with its traceback its cause.
    pub(crate) fn connector_error(&self) -> ConnectorError {
        if self.declared {
            let error = if self.kind == ConnectorErrorKind::RateLimited {
                ConnectorError::rate_limited(self.message.clone(), self.retry_after)
            } else {
                ConnectorError::new(self.kind, self.message.clone())
            };
            return error.with_code(self.code.clone());
        }
        let message = format!("{}: {}", self.type_name, self.message);
        ConnectorError::new(self.kind, message)
            .with_code(self.code.clone())
            .with_source(Traceback(self.traceback.clone()))
    }
}

/// `name`, a Python type's name, as a code the protocol admits, so a connector's code is the same
/// in process and served: snake case, `tickets.ApiError` as `tickets.api_error`, any character a
/// code cannot hold as `_`, within [`MAX_ERROR_CODE_BYTES`].
fn coded(name: &str) -> String {
    let mut code = String::with_capacity(name.len() + 4);
    let mut previous: Option<char> = None;
    let mut chars = name.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_uppercase() {
            let next = chars.peek().copied();
            let after_word = previous
                .is_some_and(|before| before.is_ascii_lowercase() || before.is_ascii_digit());
            // The last capital of a run that begins a word: `HTTPError` is `http_error`.
            let begins_word = previous.is_some_and(|before| before.is_ascii_uppercase())
                && next.is_some_and(|after| after.is_ascii_lowercase());
            if after_word || begins_word {
                code.push('_');
            }
            code.push(c.to_ascii_lowercase());
        } else if c.is_ascii_lowercase() || c.is_ascii_digit() || "_.-".contains(c) {
            code.push(c);
        } else {
            code.push('_');
        }
        previous = Some(c);
    }
    code.truncate(MAX_ERROR_CODE_BYTES);
    code
}

/// What a failure class of `rdlt.connector` declares of `value`: its kind, its code and its retry
/// hint; none for any other exception.
fn declared(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> Option<(ConnectorErrorKind, Option<String>, Option<Duration>)> {
    let class = py
        .import("rdlt.connector._failures")
        .and_then(|failures| failures.getattr("ConnectorError"))
        .ok()?;
    if !value.is_instance(&class).unwrap_or(false) {
        return None;
    }
    let kind = match value
        .getattr("kind")
        .and_then(|kind| kind.extract::<String>())
        .ok()?
        .as_str()
    {
        "transient" => ConnectorErrorKind::Transient,
        "rate_limited" => ConnectorErrorKind::RateLimited,
        "config" => ConnectorErrorKind::Config,
        "auth" => ConnectorErrorKind::Auth,
        "data" => ConnectorErrorKind::Data,
        "unsupported" => ConnectorErrorKind::Unsupported,
        "fenced" => ConnectorErrorKind::Fenced,
        _ => ConnectorErrorKind::Internal,
    };
    let code = value
        .getattr("code")
        .and_then(|code| code.extract::<Option<String>>())
        .ok()
        .flatten();
    let retry_after = value
        .getattr("retry_after")
        .and_then(|after| after.extract::<Option<f64>>())
        .ok()
        .flatten()
        .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok());
    Some((kind, code, retry_after))
}

/// A Python traceback, as the cause of the error it explains.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct Traceback(pub(crate) String);

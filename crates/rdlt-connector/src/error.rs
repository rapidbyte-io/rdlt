//! The error type every connector returns.

#[cfg(test)]
mod tests;

use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;

/// A connector result.
pub type Result<T, E = ConnectorError> = std::result::Result<T, E>;

/// What kind of failure a [`ConnectorError`] reports; the engine decides retries from it.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectorErrorKind {
    /// The configuration is invalid or refers to something that does not exist.
    Config,
    /// Credentials are missing, wrong or lack a permission.
    Auth,
    /// A failure that may succeed on retry.
    Transient,
    /// The remote system asked the caller to slow down.
    RateLimited,
    /// A value or batch is malformed or exceeds a limit.
    Data,
    /// The request needs a capability the connector does not have.
    Unsupported,
    /// A newer session holds the destination; this session may not commit.
    Fenced,
    /// The engine asked the connector to stop.
    Stopped,
    /// A bug in the connector.
    Internal,
}

/// A numeric limit that a value exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LimitExceeded {
    /// The limit's name, as in [`crate::limits`].
    pub name: &'static str,
    /// The largest allowed value.
    pub limit: u64,
    /// The value seen.
    pub actual: u64,
}

/// A connector failure: a kind, a one-line message, an optional machine code and the cause.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ConnectorError {
    kind: ConnectorErrorKind,
    message: String,
    code: Option<Arc<str>>,
    retry_after: Option<Duration>,
    limit: Option<LimitExceeded>,
    #[source]
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl ConnectorError {
    /// An error of `kind` with a one-line message naming its subject.
    pub fn new(kind: ConnectorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code: None,
            retry_after: None,
            limit: None,
            source: None,
        }
    }

    /// A [`ConnectorErrorKind::Config`] error.
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::Config, message)
    }

    /// A [`ConnectorErrorKind::Data`] error.
    pub fn data(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::Data, message)
    }

    /// A [`ConnectorErrorKind::Internal`] error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::Internal, message)
    }

    /// A [`ConnectorErrorKind::RateLimited`] error, with the wait the remote asked for.
    pub fn rate_limited(message: impl Into<String>, retry_after: Option<Duration>) -> Self {
        Self {
            retry_after,
            ..Self::new(ConnectorErrorKind::RateLimited, message)
        }
    }

    /// The error a connector returns when a newer session has fenced this one.
    pub fn fenced(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::Fenced, message)
    }

    /// The error emitting returns once the engine has asked the connector to stop.
    pub fn stopped() -> Self {
        Self::new(
            ConnectorErrorKind::Stopped,
            "the engine asked the connector to stop",
        )
    }

    /// A [`ConnectorErrorKind::Data`] error for a value over a limit, with code `limit_exceeded`.
    pub fn exceeds(limit: LimitExceeded) -> Self {
        let message = format!(
            "{} is {}, over the limit of {}",
            limit.name, limit.actual, limit.limit
        );
        Self {
            limit: Some(limit),
            ..Self::data(message).with_code("limit_exceeded")
        }
    }

    /// Attaches a stable machine code, such as `pg.permission_denied`.
    #[must_use]
    pub fn with_code(mut self, code: impl Into<Arc<str>>) -> Self {
        self.code = Some(code.into());
        self
    }

    /// Names `stream` as the subject, for errors that would otherwise not say which stream failed.
    #[must_use]
    pub(crate) fn in_stream(mut self, stream: &crate::id::StreamName) -> Self {
        self.message = format!("stream {stream}: {}", self.message);
        self
    }

    /// Attaches the underlying cause.
    #[must_use]
    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// The failure's kind.
    pub fn kind(&self) -> ConnectorErrorKind {
        self.kind
    }

    /// The machine code, if one was attached.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// How long the remote asked the caller to wait.
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// The limit a value exceeded, for [`ConnectorError::exceeds`] errors.
    pub fn limit(&self) -> Option<LimitExceeded> {
        self.limit
    }

    /// Whether retrying the operation may succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.kind,
            ConnectorErrorKind::Transient | ConnectorErrorKind::RateLimited
        )
    }
}

/// Classifies a foreign error at the call site, keeping it as the cause.
pub trait ResultExt<T> {
    /// Classifies the error as [`ConnectorErrorKind::Config`].
    fn config(self, context: impl Into<String>) -> Result<T>;
    /// Classifies the error as [`ConnectorErrorKind::Auth`].
    fn auth(self, context: impl Into<String>) -> Result<T>;
    /// Classifies the error as [`ConnectorErrorKind::Transient`].
    fn transient(self, context: impl Into<String>) -> Result<T>;
    /// Classifies the error as [`ConnectorErrorKind::Data`].
    fn data(self, context: impl Into<String>) -> Result<T>;
    /// Classifies the error as [`ConnectorErrorKind::Unsupported`].
    fn unsupported(self, context: impl Into<String>) -> Result<T>;
    /// Classifies the error as [`ConnectorErrorKind::Internal`].
    fn internal(self, context: impl Into<String>) -> Result<T>;
}

impl<T, E: StdError + Send + Sync + 'static> ResultExt<T> for std::result::Result<T, E> {
    fn config(self, context: impl Into<String>) -> Result<T> {
        classify(self, ConnectorErrorKind::Config, context)
    }

    fn auth(self, context: impl Into<String>) -> Result<T> {
        classify(self, ConnectorErrorKind::Auth, context)
    }

    fn transient(self, context: impl Into<String>) -> Result<T> {
        classify(self, ConnectorErrorKind::Transient, context)
    }

    fn data(self, context: impl Into<String>) -> Result<T> {
        classify(self, ConnectorErrorKind::Data, context)
    }

    fn unsupported(self, context: impl Into<String>) -> Result<T> {
        classify(self, ConnectorErrorKind::Unsupported, context)
    }

    fn internal(self, context: impl Into<String>) -> Result<T> {
        classify(self, ConnectorErrorKind::Internal, context)
    }
}

fn classify<T, E: StdError + Send + Sync + 'static>(
    result: std::result::Result<T, E>,
    kind: ConnectorErrorKind,
    context: impl Into<String>,
) -> Result<T> {
    result.map_err(|error| ConnectorError::new(kind, context).with_source(error))
}

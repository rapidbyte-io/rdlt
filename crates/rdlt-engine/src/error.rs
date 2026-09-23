//! The engine's error type.

#[cfg(test)]
mod tests;

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{ConnectorError, ConnectorErrorKind, StreamName};
use serde::Serialize;

use crate::scope::ScopeError;

/// What kind of failure an [`Error`] reports.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// The configuration is invalid, or a connector refused it.
    Config,
    /// Data does not match its table's schema.
    Schema,
    /// The source failed.
    Source,
    /// The destination failed.
    Destination,
    /// A newer run opened the pipeline; this run may not commit.
    Fenced,
    /// The run was stopped or cancelled before it finished.
    Cancelled,
    /// A bug in the engine.
    Internal,
}

/// Which connector a [`ConnectorError`] came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    /// The source.
    Source,
    /// The destination.
    Destination,
}

/// An engine failure: a kind, a one-line context naming its subject, and the cause.
pub struct Error {
    kind: ErrorKind,
    context: String,
    stream: Option<StreamName>,
    code: Option<Arc<str>>,
    retryable: bool,
    retry_after: Option<Duration>,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, context: impl Into<String>) -> Self {
        Self {
            kind,
            context: context.into(),
            stream: None,
            code: None,
            retryable: false,
            retry_after: None,
            source: None,
        }
    }

    pub(crate) fn config(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Config, context)
    }

    pub(crate) fn schema(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Schema, context)
    }

    pub(crate) fn cancelled(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, context)
    }

    pub(crate) fn internal(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, context)
    }

    /// Classifies a connector's `error` from `side`, keeping it as the cause.
    ///
    /// Configuration, credential and capability failures are [`ErrorKind::Config`]; a fenced
    /// session is [`ErrorKind::Fenced`]; a stopped read is [`ErrorKind::Cancelled`]; everything
    /// else belongs to the side. Only transient and rate-limited failures are retryable.
    pub(crate) fn connector(side: Side, context: impl Into<String>, error: ConnectorError) -> Self {
        use ConnectorErrorKind as K;
        let kind = match (error.kind(), side) {
            (K::Config | K::Auth | K::Unsupported, _) => ErrorKind::Config,
            (K::Fenced, _) => ErrorKind::Fenced,
            (K::Stopped, _) => ErrorKind::Cancelled,
            (_, Side::Source) => ErrorKind::Source,
            (_, Side::Destination) => ErrorKind::Destination,
        };
        Self {
            kind,
            context: context.into(),
            stream: None,
            code: error.code().map(Arc::from),
            retryable: error.is_retryable(),
            retry_after: error.retry_after(),
            source: Some(Box::new(error)),
        }
    }

    /// Attaches a stable machine code.
    #[must_use]
    pub(crate) fn with_code(mut self, code: &str) -> Self {
        self.code = Some(Arc::from(code));
        self
    }

    /// Names the stream the failure belongs to.
    #[must_use]
    pub(crate) fn with_stream(mut self, stream: &StreamName) -> Self {
        self.stream = Some(stream.clone());
        self
    }

    /// The failure's kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The stream the failure belongs to, if it belongs to one.
    pub fn stream(&self) -> Option<&StreamName> {
        self.stream.as_ref()
    }

    /// The machine code, if the failure has one.
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Whether a new attempt may succeed.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    /// How long the failing system asked the engine to wait before retrying.
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// The failure with its whole chain of causes, for reports.
    pub fn report(&self) -> ErrorReport {
        let mut causes = Vec::new();
        let mut cause = StdError::source(self);
        while let Some(error) = cause {
            causes.push(error.to_string());
            cause = error.source();
        }
        ErrorReport {
            kind: self.kind,
            stream: self.stream.as_ref().map(ToString::to_string),
            code: self.code.as_deref().map(str::to_owned),
            message: self.context.clone(),
            causes,
            retryable: self.retryable,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.context)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("context", &self.context)
            .field("stream", &self.stream)
            .field("code", &self.code)
            .field("retryable", &self.retryable)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

impl ScopeError for Error {
    fn is_cancelled(&self) -> bool {
        self.kind == ErrorKind::Cancelled
    }

    fn panicked(message: String) -> Self {
        Self::internal(format!("an engine task panicked: {message}"))
    }
}

/// An [`Error`] as data: its kind, stream, code, message and causes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ErrorReport {
    /// The failure's kind.
    pub kind: ErrorKind,
    /// The stream it belongs to.
    pub stream: Option<String>,
    /// Its machine code.
    pub code: Option<String>,
    /// A one-line description naming the failure's subject.
    pub message: String,
    /// Each cause, outermost first.
    pub causes: Vec<String>,
    /// Whether a new attempt could have succeeded.
    pub retryable: bool,
}

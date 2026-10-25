//! The engine's error type.

pub mod codes;
#[cfg(test)]
mod tests;

use std::error::Error as StdError;
use std::fmt;
use std::time::Duration;

use rdlt_connector::limits::{MAX_ERROR_CAUSES, MAX_ERROR_CODE_BYTES, MAX_ERROR_TEXT_BYTES};
use rdlt_connector::{ConnectorError, ConnectorErrorKind, StreamName};
use serde::Serialize;

use crate::budget::Exhausted;
use crate::deadline::Expired;
use crate::scope::ScopeError;
pub use codes::Code;

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
    /// The memory budget had no room for what the run holds, for as long as a request waits.
    Memory,
}

/// Who failed: the engine, or the connector on one side of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    /// The engine: every error it builds, its own wait for a connector's call among them.
    Engine,
    /// The source connector, or the host serving it: an error it or its host raised.
    Source,
    /// The destination connector, or the host serving it: an error it or its host raised.
    Destination,
}

/// Which connector a [`ConnectorError`] came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    /// The source.
    Source,
    /// The destination.
    Destination,
}

impl Side {
    /// Who failed where an error came from this side's connector.
    pub(crate) fn origin(self) -> Origin {
        match self {
            Self::Source => Origin::Source,
            Self::Destination => Origin::Destination,
        }
    }

    /// The kind of a failure of this side's that no other kind describes.
    fn kind(self) -> ErrorKind {
        match self {
            Self::Source => ErrorKind::Source,
            Self::Destination => ErrorKind::Destination,
        }
    }
}

/// Whose code an error carries: the engine's, or the code its connector's error carried, shown
/// and bounded.
enum Coded {
    Engine(Code),
    Connector(Box<str>),
}

/// An engine failure: a kind, who failed, a one-line context naming its subject, and the cause.
pub struct Error {
    kind: ErrorKind,
    origin: Origin,
    context: String,
    stream: Option<StreamName>,
    code: Option<Coded>,
    retryable: bool,
    retry_after: Option<Duration>,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, context: impl Into<String>) -> Self {
        Self {
            kind,
            origin: Origin::Engine,
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

    /// The error for a request that waited on the memory budget until its deadline, `exhausted`
    /// saying what held the budget: retryable, since another attempt holds other bytes.
    pub(crate) fn memory(exhausted: Exhausted) -> Self {
        let mut memory = Self::new(ErrorKind::Memory, exhausted.to_string());
        memory.code = Some(Coded::Engine(codes::MEMORY_BUDGET_WAIT_EXCEEDED));
        memory.retryable = true;
        memory.source = Some(Box::new(exhausted));
        memory
    }

    /// Classifies a connector's `error` from `side`, keeping it as the cause: the side failed.
    ///
    /// Configuration, credential and capability failures are [`ErrorKind::Config`]; a session a
    /// newer one fenced is [`ErrorKind::Fenced`]; everything else belongs to the side, a stop
    /// among it, since the engine knows which reads it stopped. Only transient and rate-limited
    /// failures are retryable, and only a rate limit keeps the wait it asks for. No connector's
    /// error is of the kind a wait on the memory budget fails with, and its code stays the
    /// connector's, [shown](rdlt_connector::text::shown) within a code's limit: the error's
    /// [`connector_code`](Self::connector_code), never its [`code`](Self::code).
    ///
    /// A call the engine's wait ended is no connector's failure: it is the engine's, as
    /// [`expired`](Self::expired) builds it.
    pub(crate) fn connector(side: Side, context: impl Into<String>, error: ConnectorError) -> Self {
        use ConnectorErrorKind as K;
        if let Some(expired) = Expired::of(&error) {
            return Self::expired(side, context, expired);
        }
        let kind = match (error.kind(), side) {
            (K::Config | K::Auth | K::Unsupported, _) => ErrorKind::Config,
            (K::Fenced, Side::Destination) => ErrorKind::Fenced,
            (_, side) => side.kind(),
        };
        let retry_after = error
            .retry_after()
            .filter(|_| error.kind() == K::RateLimited);
        Self {
            kind,
            origin: side.origin(),
            context: context.into(),
            stream: None,
            code: error.code().map(|code| {
                Coded::Connector(rdlt_connector::text::shown(code, MAX_ERROR_CODE_BYTES).into())
            }),
            retryable: error.is_retryable(),
            retry_after,
            source: Some(Box::new(error)),
        }
    }

    /// The error for a call into `side`'s connector that the engine's wait ended, as `expired`
    /// says: of the side's kind, coded `deadline_exceeded`, and retryable, since the next
    /// attempt calls again.
    fn expired(side: Side, context: impl Into<String>, expired: Expired) -> Self {
        Self::new(side.kind(), context)
            .with_code(codes::DEADLINE_EXCEEDED)
            .retryable()
            .with_source(expired)
    }

    /// The failure, which a new attempt may mend.
    #[must_use]
    pub(crate) fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }

    /// Keeps `source` as the error's cause.
    #[must_use]
    pub(crate) fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Attaches the engine's code for the failure, in place of any code a connector's error
    /// carried.
    #[must_use]
    pub(crate) fn with_code(mut self, code: Code) -> Self {
        self.code = Some(Coded::Engine(code));
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

    /// Who failed.
    pub fn origin(&self) -> Origin {
        self.origin
    }

    /// The stream the failure belongs to, if it belongs to one.
    pub fn stream(&self) -> Option<&StreamName> {
        self.stream.as_ref()
    }

    /// The engine's code for the failure, if it has one: never a connector's, which
    /// [`connector_code`](Self::connector_code) returns.
    pub fn code(&self) -> Option<Code> {
        match &self.code {
            Some(Coded::Engine(code)) => Some(*code),
            Some(Coded::Connector(_)) | None => None,
        }
    }

    /// The code a connector's error carried, as a connector's text is
    /// [shown](rdlt_connector::text::shown), within
    /// [`MAX_ERROR_CODE_BYTES`](rdlt_connector::limits::MAX_ERROR_CODE_BYTES).
    pub fn connector_code(&self) -> Option<&str> {
        match &self.code {
            Some(Coded::Connector(code)) => Some(code),
            Some(Coded::Engine(_)) | None => None,
        }
    }

    /// Whether a new attempt may succeed.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    /// How long the failing system asked the engine to wait before retrying.
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// The failure with its chain of causes, for reports: each text
    /// [shown](rdlt_connector::text::shown), since a connector's words may be in any of
    /// them, and at most [`MAX_ERROR_CAUSES`] causes.
    pub fn report(&self) -> ErrorReport {
        let shown =
            |text: &dyn fmt::Display| rdlt_connector::text::shown(text, MAX_ERROR_TEXT_BYTES);
        let mut causes = Vec::new();
        let mut cause = StdError::source(self);
        while let Some(error) = cause {
            if causes.len() == MAX_ERROR_CAUSES {
                break;
            }
            causes.push(shown(&error));
            cause = error.source();
        }
        ErrorReport {
            kind: self.kind,
            origin: self.origin,
            stream: self.stream.as_ref().map(|stream| shown(stream)),
            code: self.code(),
            connector_code: self.connector_code().map(str::to_owned),
            message: shown(&self.context),
            causes,
            retryable: self.retryable,
        }
    }
}

/// Shows the context as connector text is shown: it may name a stream or table a connector
/// chose.
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&rdlt_connector::text::shown(
            &self.context,
            MAX_ERROR_TEXT_BYTES,
        ))
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.kind)
            .field("origin", &self.origin)
            .field("context", &self.context)
            .field("stream", &self.stream)
            .field("code", &self.code())
            .field("connector_code", &self.connector_code())
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

/// An [`Error`] as data: its kind, who failed, its stream, codes, message and causes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ErrorReport {
    /// The failure's kind.
    pub kind: ErrorKind,
    /// Who failed.
    pub origin: Origin,
    /// The stream it belongs to.
    pub stream: Option<String>,
    /// The engine's code for it.
    pub code: Option<Code>,
    /// The code a connector's error carried, shown and bounded.
    pub connector_code: Option<String>,
    /// A one-line description naming the failure's subject.
    pub message: String,
    /// Each cause, outermost first.
    pub causes: Vec<String>,
    /// Whether a new attempt could have succeeded.
    pub retryable: bool,
}

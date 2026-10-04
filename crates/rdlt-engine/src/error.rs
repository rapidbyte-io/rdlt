//! The engine's error type.

#[cfg(test)]
mod tests;

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::limits::{MAX_ERROR_CAUSES, MAX_ERROR_TEXT_BYTES};
use rdlt_connector::{ConnectorError, ConnectorErrorKind, StreamName};
use serde::Serialize;

use crate::budget::Exhausted;
use crate::limits::{
    BUDGET_WAIT_EXCEEDED, WAL_FENCED, WAL_NOT_PRIVATE, WAL_RUNNING, WAL_STORAGE_FULL,
    WAL_STORE_OTHER, WAL_STRAY,
};
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
    /// The write-ahead log could not be written or read.
    Wal,
    /// The memory budget had no room for what the run holds, for as long as a request waits.
    Memory,
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

    pub(crate) fn wal(context: impl Into<String>) -> Self {
        Self::new(ErrorKind::Wal, context)
    }

    /// The error for a request that waited on the memory budget until its deadline, `exhausted`
    /// saying what held the budget: retryable, since another attempt holds other bytes.
    pub(crate) fn memory(exhausted: Exhausted) -> Self {
        let mut memory = Self::new(ErrorKind::Memory, exhausted.to_string());
        memory.code = Some(Arc::from(BUDGET_WAIT_EXCEEDED));
        memory.retryable = true;
        memory.source = Some(Box::new(exhausted));
        memory
    }

    /// The error for `error`, from the write-ahead log's store: retryable where the operation may
    /// succeed if tried again.
    ///
    /// What a local log's store refused, as no user's alone or as a name it never writes, is
    /// `wal_not_private` or `wal_stray`; a full disk or quota is `wal_storage_full`.
    pub(crate) fn from_wal(error: std::io::Error) -> Self {
        use std::io::ErrorKind as Io;
        // A full disk is no end: the failed write gave back what it staged, and the next
        // attempt frees what a crashed load staged before it needs room of its own.
        let full = matches!(error.kind(), Io::StorageFull | Io::QuotaExceeded);
        let transient = full
            || matches!(
                error.kind(),
                Io::Interrupted | Io::TimedOut | Io::WouldBlock | Io::ResourceBusy
            );
        let refused = error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<crate::wal::Refusal>())
            .map(|refusal| match refusal {
                crate::wal::Refusal::NotPrivate { .. } => WAL_NOT_PRIVATE,
                crate::wal::Refusal::Stray { .. } => WAL_STRAY,
            });
        let code = refused
            .or_else(|| crate::wal::object_code(&error))
            .or(full.then_some(WAL_STORAGE_FULL));
        let mut wal = Self::wal(format!("the write-ahead log failed: {error}"));
        wal.retryable = transient;
        wal.code = code.map(Arc::from);
        wal.source = Some(Box::new(error));
        wal
    }

    /// The error for a write-ahead log an earlier failure, `failure`, left unknown: of its kind
    /// and code, and retryable as it was, since the next attempt writes a log of its own.
    pub(crate) fn wal_failed_before(failure: &Self) -> Self {
        let mut wal = Self::new(
            failure.kind,
            format!("the write-ahead log failed before: {failure}"),
        );
        wal.code.clone_from(&failure.code);
        wal.retryable = failure.retryable;
        wal
    }

    /// The error for a load whose log a replay took over, publishing the chunk the load was to
    /// publish next: another attempt runs, which fences this one.
    pub(crate) fn wal_fenced(load: rdlt_connector::LoadId) -> Self {
        Self::new(
            ErrorKind::Fenced,
            format!("the write-ahead log of load {load} was taken over by a replay"),
        )
        .with_code(WAL_FENCED)
    }

    /// The error for a replay that could not fence `load`'s log, which its load kept publishing
    /// chunks to: another attempt of the pipeline runs, and this one waits for it, retryably.
    pub(crate) fn wal_running(load: rdlt_connector::LoadId) -> Self {
        let mut running = Self::wal(format!(
            "load {load} of the pipeline is still writing its write-ahead log"
        ))
        .with_code(WAL_RUNNING);
        running.retryable = true;
        running
    }

    /// The error for an attempt whose log another attempt's replay removed as it was being opened:
    /// another attempt of the pipeline begins, and this one waits for it, retryably.
    pub(crate) fn wal_opening_taken(load: rdlt_connector::LoadId) -> Self {
        let mut taken = Self::wal(format!(
            "another attempt of the pipeline removed the write-ahead log of load {load} as it \
             was opened"
        ))
        .with_code(WAL_RUNNING);
        taken.retryable = true;
        taken
    }

    /// The error for an attempt whose log store, `ours`, is not `named`, the store the
    /// destination names for the pipeline's logs: its runs must keep their logs in one store.
    pub(crate) fn wal_store_other(
        ours: rdlt_connector::LoadId,
        named: rdlt_connector::LoadId,
    ) -> Self {
        Self::config(format!(
            "the pipeline's logs at this destination are kept in store {named}, and this \
             engine's is store {ours}: its runs keep their logs in one store, whose identity \
             file moves with it"
        ))
        .with_code(WAL_STORE_OTHER)
    }

    /// Classifies a connector's `error` from `side`, keeping it as the cause.
    ///
    /// Configuration, credential and capability failures are [`ErrorKind::Config`]; a session a
    /// newer one fenced is [`ErrorKind::Fenced`]; everything else belongs to the side, a stop
    /// among it, since the engine knows which reads it stopped. Only transient and rate-limited
    /// failures are retryable, and only a rate limit keeps the wait it asks for. No connector's
    /// error is of the kind or code a wait on the memory budget fails with.
    pub(crate) fn connector(side: Side, context: impl Into<String>, error: ConnectorError) -> Self {
        use ConnectorErrorKind as K;
        let kind = match (error.kind(), side) {
            (K::Config | K::Auth | K::Unsupported, _) => ErrorKind::Config,
            (K::Fenced, Side::Destination) => ErrorKind::Fenced,
            (_, Side::Source) => ErrorKind::Source,
            (_, Side::Destination) => ErrorKind::Destination,
        };
        let retry_after = error
            .retry_after()
            .filter(|_| error.kind() == K::RateLimited);
        Self {
            kind,
            context: context.into(),
            stream: None,
            // The budget's code is the engine's own: no connector's error carries it.
            code: error
                .code()
                .filter(|code| *code != BUDGET_WAIT_EXCEEDED)
                .map(Arc::from),
            retryable: error.is_retryable(),
            retry_after,
            source: Some(Box::new(error)),
        }
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
            stream: self.stream.as_ref().map(|stream| shown(stream)),
            code: self.code.as_deref().map(|code| shown(&code)),
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

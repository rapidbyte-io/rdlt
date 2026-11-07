//! The facade's error: a pipeline that could not be built, or a run that failed.

#[cfg(test)]
mod tests;

use std::error::Error as StdError;
use std::fmt;

use rdlt_engine::{Code, ErrorKind, ErrorReport, Origin};

/// Why a pipeline could not be built, or why its run failed: a kind, who failed, a code and the
/// stream it belongs to, as the engine's own errors say them.
///
/// Its text never holds a value of a document or a resolved secret, and a connector's words in
/// it are shown and bounded as the host shows them.
pub struct Error(Box<Inner>);

enum Inner {
    /// The engine's error, and the stream it names as text.
    Engine(rdlt_engine::Error, Option<String>),
}

impl Error {
    /// The engine's `error`, as the facade reports it.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the lowering and the runs hand the facade the engine's errors"
        )
    )]
    pub(crate) fn engine(error: rdlt_engine::Error) -> Self {
        let stream = error.stream().map(ToString::to_string);
        Self(Box::new(Inner::Engine(error, stream)))
    }

    /// The failure's kind.
    pub fn kind(&self) -> ErrorKind {
        match &*self.0 {
            Inner::Engine(error, _) => error.kind(),
        }
    }

    /// Who failed.
    pub fn origin(&self) -> Origin {
        match &*self.0 {
            Inner::Engine(error, _) => error.origin(),
        }
    }

    /// The failure's code, the engine's, the host's or the facade's; never a connector's.
    pub fn code(&self) -> Option<Code> {
        match &*self.0 {
            Inner::Engine(error, _) => error.code(),
        }
    }

    /// The code a connector's error carried, shown and bounded.
    pub fn connector_code(&self) -> Option<&str> {
        match &*self.0 {
            Inner::Engine(error, _) => error.connector_code(),
        }
    }

    /// The stream the failure belongs to, where it belongs to one.
    pub fn stream(&self) -> Option<&str> {
        match &*self.0 {
            Inner::Engine(_, stream) => stream.as_deref(),
        }
    }

    /// Whether trying again may succeed.
    pub fn is_retryable(&self) -> bool {
        match &*self.0 {
            Inner::Engine(error, _) => error.is_retryable(),
        }
    }

    /// The failure and its causes as data that serializes, every text shown and bounded.
    pub fn report(&self) -> ErrorReport {
        match &*self.0 {
            Inner::Engine(error, _) => error.report(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self.0 {
            Inner::Engine(error, _) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Error")
            .field("kind", &self.kind())
            .field("origin", &self.origin())
            .field("code", &self.code())
            .field("stream", &self.stream())
            .field("message", &self.to_string())
            .finish_non_exhaustive()
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &*self.0 {
            Inner::Engine(error, _) => error.source(),
        }
    }
}

//! The facade's error: a pipeline that could not be built, or a run that failed.

#[cfg(test)]
mod tests;

use std::error::Error as StdError;
use std::fmt;

use rdlt_connector::limits::{MAX_ERROR_CAUSES, MAX_ERROR_TEXT_BYTES};
use rdlt_connector::{ConnectorErrorKind, Role, text};
use rdlt_engine::{Code, ErrorKind, ErrorReport, Origin};
use rdlt_host::ProviderError;

/// Why a pipeline could not be built, or why its run failed: a kind, who failed, a code and the
/// stream it belongs to, as the engine's own errors say them.
///
/// Its text never holds a value of a document or a resolved secret, and a connector's words in
/// it are shown and bounded as the host shows them.
pub struct Error(Box<Inner>);

enum Inner {
    /// The engine's error, and the stream it names as text.
    Engine(rdlt_engine::Error, Option<String>),
    Built(Built),
}

/// A failure the facade found itself, or met placing or calling a connector.
struct Built {
    kind: ErrorKind,
    origin: Origin,
    code: Option<Code>,
    connector_code: Option<String>,
    stream: Option<String>,
    message: String,
    retryable: bool,
    source: Option<Box<dyn StdError + Send + Sync>>,
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

    fn built(
        kind: ErrorKind,
        origin: Origin,
        code: Code,
        message: String,
        cause: Option<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self(Box::new(Inner::Built(Built {
            kind,
            origin,
            code: Some(code),
            connector_code: None,
            stream: None,
            message,
            retryable: false,
            source: cause,
        })))
    }

    /// A configuration the facade refused, coded `code`.
    pub(crate) fn config(code: Code, message: impl Into<String>) -> Self {
        Self::built(
            ErrorKind::Config,
            Origin::Engine,
            code,
            message.into(),
            None,
        )
    }

    /// A configuration the facade refused, coded `code`, for `cause`.
    pub(crate) fn config_caused(
        code: Code,
        message: impl Into<String>,
        cause: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let cause = Some(Box::new(cause) as Box<dyn StdError + Send + Sync>);
        Self::built(
            ErrorKind::Config,
            Origin::Engine,
            code,
            message.into(),
            cause,
        )
    }

    /// `error`, which placing the connector of `role` met.
    ///
    /// A connector that could not be started or reached fails its side; one that refused its
    /// configuration, credentials or capabilities as it connected, and every placement the host
    /// refused, is a configuration error.
    pub(crate) fn placement(role: Role, error: ProviderError) -> Self {
        let (origin, failed) = side(role);
        let kind = match &error {
            ProviderError::SpawnFailed { .. }
            | ProviderError::Unreachable { .. }
            | ProviderError::Tls { .. } => failed,
            ProviderError::HandshakeFailed { source, .. } => classed(source.kind(), failed),
            _ => ErrorKind::Config,
        };
        let message = format!("placing the {}: {error}", noun(role));
        let code = Code::new(error.code());
        Self::built(kind, origin, code, message, Some(Box::new(error)))
    }

    /// The failure's kind.
    pub fn kind(&self) -> ErrorKind {
        match &*self.0 {
            Inner::Engine(error, _) => error.kind(),
            Inner::Built(built) => built.kind,
        }
    }

    /// Who failed.
    pub fn origin(&self) -> Origin {
        match &*self.0 {
            Inner::Engine(error, _) => error.origin(),
            Inner::Built(built) => built.origin,
        }
    }

    /// The failure's code, the engine's, the host's or the facade's; never a connector's.
    pub fn code(&self) -> Option<Code> {
        match &*self.0 {
            Inner::Engine(error, _) => error.code(),
            Inner::Built(built) => built.code,
        }
    }

    /// The code a connector's error carried, shown and bounded.
    pub fn connector_code(&self) -> Option<&str> {
        match &*self.0 {
            Inner::Engine(error, _) => error.connector_code(),
            Inner::Built(built) => built.connector_code.as_deref(),
        }
    }

    /// The stream the failure belongs to, where it belongs to one.
    pub fn stream(&self) -> Option<&str> {
        match &*self.0 {
            Inner::Engine(_, stream) => stream.as_deref(),
            Inner::Built(built) => built.stream.as_deref(),
        }
    }

    /// Whether trying again may succeed.
    pub fn is_retryable(&self) -> bool {
        match &*self.0 {
            Inner::Engine(error, _) => error.is_retryable(),
            Inner::Built(built) => built.retryable,
        }
    }

    /// The failure and its causes as data that serializes, every text shown and bounded.
    pub fn report(&self) -> ErrorReport {
        match &*self.0 {
            Inner::Engine(error, _) => error.report(),
            Inner::Built(built) => ErrorReport {
                kind: built.kind,
                origin: built.origin,
                stream: built.stream.as_deref().map(shown),
                code: built.code,
                connector_code: built.connector_code.clone(),
                message: shown(&built.message),
                causes: causes(StdError::source(self)),
                retryable: built.retryable,
            },
        }
    }
}

/// Who failed, and the kind of a failure of theirs no other kind describes, for `role`.
fn side(role: Role) -> (Origin, ErrorKind) {
    match role {
        Role::Source => (Origin::Source, ErrorKind::Source),
        Role::Destination => (Origin::Destination, ErrorKind::Destination),
    }
}

/// A connector's failure of `kind` as the engine classes one: configuration, credentials and
/// capabilities are `Config`, anything else `failed`.
fn classed(kind: ConnectorErrorKind, failed: ErrorKind) -> ErrorKind {
    match kind {
        ConnectorErrorKind::Config | ConnectorErrorKind::Auth | ConnectorErrorKind::Unsupported => {
            ErrorKind::Config
        }
        _ => failed,
    }
}

/// The side `role` names, as an error says it.
pub(crate) fn noun(role: Role) -> &'static str {
    match role {
        Role::Source => "source",
        Role::Destination => "destination",
    }
}

fn shown(text: impl fmt::Display) -> String {
    text::shown(text, MAX_ERROR_TEXT_BYTES)
}

/// Each of `cause` and the causes beneath it, outermost first, shown, and at most
/// [`MAX_ERROR_CAUSES`] of them.
fn causes(mut cause: Option<&(dyn StdError + 'static)>) -> Vec<String> {
    let mut causes = Vec::new();
    while let Some(error) = cause {
        if causes.len() == MAX_ERROR_CAUSES {
            break;
        }
        causes.push(shown(error));
        cause = error.source();
    }
    causes
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self.0 {
            Inner::Engine(error, _) => fmt::Display::fmt(error, formatter),
            Inner::Built(built) => formatter.write_str(&shown(&built.message)),
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
            Inner::Built(built) => built
                .source
                .as_deref()
                .map(|source| source as &(dyn StdError + 'static)),
        }
    }
}

//! Connector errors on the wire.

use super::types::{duration, std_duration};
use super::{Invalid, v1};
use crate::error::{ConnectorError, ConnectorErrorKind, LimitExceeded};

/// The names of the limits either end enforces, as [`LimitExceeded`] names them.
const LIMIT_NAMES: &[&str] = &[
    "batch columns",
    "batch rows",
    "config bytes",
    "control string bytes",
    "cursor bytes",
    "frame bytes",
    "json push bytes",
    "nesting depth",
    "schema columns",
];

/// The name a limit that `name` does not match goes by.
const UNKNOWN_LIMIT: &str = "limit";

impl From<ConnectorErrorKind> for v1::ErrorKind {
    fn from(kind: ConnectorErrorKind) -> Self {
        match kind {
            ConnectorErrorKind::Config => Self::Config,
            ConnectorErrorKind::Auth => Self::Auth,
            ConnectorErrorKind::Transient => Self::Transient,
            ConnectorErrorKind::RateLimited => Self::RateLimited,
            ConnectorErrorKind::Data => Self::Data,
            ConnectorErrorKind::Unsupported => Self::Unsupported,
            ConnectorErrorKind::Fenced => Self::Fenced,
            ConnectorErrorKind::Stopped => Self::Stopped,
            ConnectorErrorKind::Internal => Self::Internal,
        }
    }
}

/// The error kind `value` names; one this end does not know is internal.
fn kind(value: i32) -> ConnectorErrorKind {
    match v1::ErrorKind::try_from(value) {
        Ok(v1::ErrorKind::Config) => ConnectorErrorKind::Config,
        Ok(v1::ErrorKind::Auth) => ConnectorErrorKind::Auth,
        Ok(v1::ErrorKind::Transient) => ConnectorErrorKind::Transient,
        Ok(v1::ErrorKind::RateLimited) => ConnectorErrorKind::RateLimited,
        Ok(v1::ErrorKind::Data) => ConnectorErrorKind::Data,
        Ok(v1::ErrorKind::Unsupported) => ConnectorErrorKind::Unsupported,
        Ok(v1::ErrorKind::Fenced) => ConnectorErrorKind::Fenced,
        Ok(v1::ErrorKind::Stopped) => ConnectorErrorKind::Stopped,
        Ok(v1::ErrorKind::Internal | v1::ErrorKind::Unspecified) | Err(_) => {
            ConnectorErrorKind::Internal
        }
    }
}

impl From<&ConnectorError> for v1::Error {
    fn from(error: &ConnectorError) -> Self {
        Self {
            kind: v1::ErrorKind::from(error.kind()) as i32,
            message: error.to_string(),
            code: error.code().map(ToOwned::to_owned),
            retry_after: error.retry_after().map(duration),
            limit: error.limit().map(|limit| v1::LimitExceeded {
                name: limit.name.to_owned(),
                limit: limit.limit,
                actual: limit.actual,
            }),
        }
    }
}

impl TryFrom<v1::Error> for ConnectorError {
    type Error = Invalid;

    fn try_from(error: v1::Error) -> Result<Self, Invalid> {
        let limit = error.limit.map(|limit| LimitExceeded {
            name: LIMIT_NAMES
                .iter()
                .find(|name| **name == limit.name)
                .copied()
                .unwrap_or(UNKNOWN_LIMIT),
            limit: limit.limit,
            actual: limit.actual,
        });
        let mut decoded = Self::new(kind(error.kind), error.message)
            .with_retry_after(error.retry_after.map(std_duration).transpose()?)
            .with_limit(limit);
        if let Some(code) = error.code {
            decoded = decoded.with_code(code);
        }
        Ok(decoded)
    }
}

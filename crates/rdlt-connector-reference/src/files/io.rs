//! Filesystem errors as connector errors.

#[cfg(test)]
mod tests;

use std::io::{self, ErrorKind};
use std::path::Path;

use rdlt_connector::{ConnectorError, ConnectorErrorKind, LimitExceeded, Result};

use crate::limits::PUBLISH_ATTEMPTS;
use crate::rooted::{self, Refusal};

/// The code of an error for a name that is not one path component, or no table identifier.
pub(super) const INVALID_NAME: &str = "invalid_name";

/// The code of an error for a link, a pipe, a device or a directory where a file belongs.
pub(super) const NOT_A_REGULAR_FILE: &str = "not_a_regular_file";

/// Classifies a filesystem error from `what` on `path`: a path the connector may not use is a
/// configuration error, a name or a file it refuses a data error, anything else transient.
pub(super) fn failed<'a>(
    what: &'a str,
    path: &'a Path,
) -> impl Fn(io::Error) -> ConnectorError + 'a {
    move |error| {
        let message = format!("{what} {}: {error}", path.display());
        match rooted::refusal(&error) {
            Some(Refusal::TooLarge {
                name,
                limit,
                actual,
            }) => ConnectorError::exceeds(LimitExceeded {
                name,
                limit,
                actual,
            })
            .with_source(error),
            Some(Refusal::Name) => ConnectorError::data(message)
                .with_code(INVALID_NAME)
                .with_source(error),
            Some(Refusal::NotRegular) => ConnectorError::data(message)
                .with_code(NOT_A_REGULAR_FILE)
                .with_source(error),
            Some(Refusal::Shared) => ConnectorError::config(message).with_source(error),
            None => {
                let kind = match error.kind() {
                    ErrorKind::PermissionDenied
                    | ErrorKind::ReadOnlyFilesystem
                    | ErrorKind::NotADirectory => ConnectorErrorKind::Config,
                    _ => ConnectorErrorKind::Transient,
                };
                ConnectorError::new(kind, message).with_source(error)
            }
        }
    }
}

/// Classifies a filesystem error from `what` on `path`, a file the destination's manifests or
/// staging list: one missing is lost, which no retry finds, a data error coded `file_missing`;
/// anything else as [`failed`] does.
pub(super) fn listed<'a>(
    what: &'a str,
    path: &'a Path,
) -> impl Fn(io::Error) -> ConnectorError + 'a {
    move |error| {
        if error.kind() == ErrorKind::NotFound {
            ConnectorError::data(format!("{what} {}: the file is missing", path.display()))
                .with_code("file_missing")
                .with_source(error)
        } else {
            failed(what, path)(error)
        }
    }
}

/// Runs `work` until it yields a value: each run that yields none lost to another session's
/// write and works its change out again, a bounded number of times; `what` names the work that
/// kept losing in the transient error that follows the last.
pub(super) fn retried<T>(what: &str, mut work: impl FnMut() -> Result<Option<T>>) -> Result<T> {
    for _ in 0..PUBLISH_ATTEMPTS {
        if let Some(done) = work()? {
            return Ok(done);
        }
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::Transient,
        format!("{what}: other sessions kept writing first"),
    ))
}

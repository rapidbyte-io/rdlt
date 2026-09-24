//! Filesystem errors as connector errors, and making a directory's entries durable.

use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;

use rdlt_connector::{ConnectorError, ConnectorErrorKind, Result};

/// Classifies a filesystem error from `what` on `path`: a path the connector may not use is a
/// configuration error, anything else is transient.
pub(super) fn failed<'a>(
    what: &'a str,
    path: &'a Path,
) -> impl Fn(io::Error) -> ConnectorError + 'a {
    move |error| {
        let kind = match error.kind() {
            ErrorKind::PermissionDenied
            | ErrorKind::ReadOnlyFilesystem
            | ErrorKind::NotADirectory => ConnectorErrorKind::Config,
            _ => ConnectorErrorKind::Transient,
        };
        ConnectorError::new(kind, format!("{what} {}: {error}", path.display())).with_source(error)
    }
}

/// Makes the entries of `dir` durable, so a file created or linked in it survives a crash.
pub(super) fn sync_dir(dir: &Path) -> Result<()> {
    fs::File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(failed("syncing", dir))
}

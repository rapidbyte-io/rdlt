//! Filesystem errors as connector errors, and making a directory's entries durable.

#[cfg(test)]
pub(super) mod tests;

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

/// Whether linking a new file at `path` created it: one that exists already is another writer's,
/// which won.
pub(super) fn created(link: io::Result<()>, path: &Path) -> Result<bool> {
    match link {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(failed("publishing", path)(error)),
    }
}

/// Makes the entries of `dir` durable, so a file created or linked in it survives a crash.
pub(super) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(test)]
    tests::SYNCED.with(|synced| synced.borrow_mut().push(dir.to_owned()));
    fs::File::open(dir)
        .and_then(|dir| dir.sync_all())
        .map_err(failed("syncing", dir))
}

/// Creates `dir` and whichever of its ancestors are missing, making each new directory durable
/// in its parent: a crash never loses a directory that synced files sit in.
pub(super) fn create_dirs(dir: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut current = Some(dir);
    while let Some(path) = current {
        if path.try_exists().map_err(failed("inspecting", path))? {
            break;
        }
        missing.push(path);
        current = path.parent();
    }
    fs::create_dir_all(dir).map_err(failed("creating a directory", dir))?;
    for created in missing.iter().rev() {
        if let Some(parent) = created.parent() {
            // A relative directory of one component has the empty path as its parent.
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            sync_dir(parent)?;
        }
    }
    Ok(())
}

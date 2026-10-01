//! Where a database is: the file its configured path names, and whether that place is private.
//!
//! SQLite reads a name that starts with `file:` as a URI, whose parameters choose another file,
//! switch its locks off or keep it in memory, whatever flags it is opened with. A path is
//! therefore refused where it starts so, and SQLite is handed the path from the root, which
//! starts with a separator and is a file's name and nothing else.

#[cfg(test)]
mod tests;

use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Component, Path, PathBuf};

use rdlt_connector::{ConnectorError, Result};

/// The prefix of a name SQLite reads as a URI.
const URI_PREFIX: &[u8] = b"file:";

/// The path from the root of the database `path` names, checked private, with the file created
/// for its user alone where it is missing and `create` asks; none where it is missing and
/// `create` does not.
///
/// A path SQLite would read as a URI, or that names no file, is a `Config` error coded
/// `database_path_invalid`.
pub(super) fn located(path: &Path, create: bool) -> Result<Option<PathBuf>> {
    let path = named(path)?;
    private(&path)?;
    match std::fs::symlink_metadata(&path) {
        Ok(_) => Ok(Some(path)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !create => Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let created = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path);
            created.map_err(unusable(&path))?;
            Ok(Some(path))
        }
        Err(error) => Err(unusable(&path)(error)),
    }
}

/// `path` from the root, where it names a file and nothing SQLite reads another way.
pub(super) fn named(path: &Path) -> Result<PathBuf> {
    let invalid = |why: &str| {
        let message = format!("{} names no database file: {why}", path.display());
        ConnectorError::config(message).with_code("database_path_invalid")
    };
    let text = path.as_os_str().as_bytes();
    let uri = text
        .get(..URI_PREFIX.len())
        .is_some_and(|start| start.eq_ignore_ascii_case(URI_PREFIX));
    if uri {
        return Err(invalid("a name starting with file: is read as a URI"));
    }
    if text.contains(&0) {
        return Err(invalid("the name holds a NUL"));
    }
    let Ok(absolute) = std::path::absolute(path) else {
        return Err(invalid("the name is empty"));
    };
    // The last component is the file's own name: `.` and `..` name a directory.
    let file = text.rsplit(|byte| *byte == b'/').next().unwrap_or_default();
    let last = absolute.components().next_back();
    if file.is_empty() || file == b"." || !matches!(last, Some(Component::Normal(_))) {
        return Err(invalid("the path ends in no file's name"));
    }
    Ok(absolute)
}

/// Refuses the database at `path` where it is no regular file or its group or others reach it.
fn private(path: &Path) -> Result<()> {
    let found = match std::fs::symlink_metadata(path) {
        Ok(found) => found,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(unusable(path)(error)),
    };
    if !found.is_file() {
        let message = format!("{} is no regular file", path.display());
        return Err(ConnectorError::config(message).with_code("not_a_regular_file"));
    }
    let mode = found.mode() & 0o7777;
    if mode & 0o077 != 0 {
        let message = format!(
            "{} has mode {mode:o}: a database is its user's alone, mode 600",
            path.display()
        );
        return Err(ConnectorError::config(message).with_code("not_private"));
    }
    Ok(())
}

/// The error of a database at `path` the system does not let the connector reach.
fn unusable(path: &Path) -> impl Fn(std::io::Error) -> ConnectorError {
    let path = path.to_owned();
    move |error| {
        let message = format!("opening the database {}: {error}", path.display());
        ConnectorError::config(message).with_source(error)
    }
}

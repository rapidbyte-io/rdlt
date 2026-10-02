//! Where a database is: the file its configured path names, and whether that place is private.
//!
//! SQLite reads a name that starts with `file:` as a URI, whose parameters choose another file,
//! switch its locks off or keep it in memory, whatever flags it is opened with. A path is
//! therefore refused where it starts so, and SQLite is handed the path from the root, which
//! starts with a separator and is a file's name and nothing else.
//!
//! SQLite follows a link at any name it opens and gives the files it creates beside a database
//! the database's mode, but adopts one that is already there as it is. So the place is checked
//! before SQLite sees it: a directory only its user writes, holding only private regular files
//! under the database's names.

#[cfg(test)]
mod tests;

use std::ffi::OsString;
use std::io;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Component, Path, PathBuf};

use rdlt_connector::{ConnectorError, ConnectorErrorKind, Result};

use crate::files::io::failed;
use crate::rooted::{self, Dir, Refusal};

/// What follows a database's name in the names of the files SQLite keeps beside it, the
/// database itself first: its write-ahead log, that log's index, and its rollback journal.
pub(super) const SIDE_FILES: [&str; 4] = ["", "-wal", "-shm", "-journal"];

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
    let placed = || -> io::Result<bool> {
        let (dir, name) = private(&path)?;
        match (dir.kind(&name)?, create) {
            (Some(_), _) => Ok(true),
            (None, false) => Ok(false),
            (None, true) => dir.create(&name).map(|_| true),
        }
    };
    match placed() {
        Ok(true) => Ok(Some(path)),
        Ok(false) => Ok(None),
        // A directory that is not there is the configuration's to name, not a wait's to bring.
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let message = format!("opening the database {}: {error}", path.display());
            Err(ConnectorError::config(message).with_source(error))
        }
        Err(error) => Err(refused(&path, error)),
    }
}

/// The error of a database's place that is refused: a configuration error under the code the
/// files connectors give the same refusal, since the path is the operator's to name.
fn refused(path: &Path, error: io::Error) -> ConnectorError {
    // A file where the path names the database's directory is answered by the platform.
    let error = match error.kind() {
        io::ErrorKind::NotADirectory => Refusal::NotDirectory.into(),
        _ => error,
    };
    let refused = failed("opening the database", path)(error);
    match (refused.kind(), refused.code()) {
        (ConnectorErrorKind::Data, Some(code)) => {
            let code = code.to_owned();
            ConnectorError::config(refused.to_string())
                .with_code(code)
                .with_source(refused)
        }
        _ => refused,
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

/// The directory of the database at `path`, open, and the database's name in it, where the
/// place is its user's alone.
///
/// The directory is a root as the files connectors hold theirs: its user's, and writable by no
/// other, so no one else creates, replaces or links a name in it. The database and each file
/// SQLite keeps beside it, where they exist, are regular files of that user's, no links, and
/// within no one else's reach: SQLite adopts a log that is already there as it is, and whoever
/// reads the log's index can hold every writer out.
fn private(path: &Path) -> io::Result<(Dir, OsString)> {
    let directory = path.parent().unwrap_or(Path::new("/"));
    let name = path.file_name().unwrap_or_default().to_owned();
    let dir = Dir::ambient(directory)?;
    for suffix in SIDE_FILES {
        let mut side = name.clone();
        side.push(suffix);
        if dir.kind(&side)?.is_none() {
            continue;
        }
        let file = dir.file(&side)?;
        rooted::private(&file)?;
        let found = file.metadata()?;
        if found.mode() & 0o077 != 0 {
            let reached = Refusal::Shared {
                owner: found.uid(),
                mode: found.mode() & 0o7777,
            };
            return Err(reached.into());
        }
    }
    Ok((dir, name))
}

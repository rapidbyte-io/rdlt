//! Where a source that keeps its positions in a file may keep them.

use std::path::Path;

use rdlt_connector::ConnectorError;

/// Checks that `path` names a keeper's file: one whose name ends in `.` and `extension`.
///
/// A keeper replaces its file whole, so it is given no file but one named as a keeper's: a path
/// to anything else, a key or another connector's file, is refused before it is opened. Which
/// file a path leads to is the keeper's to tell: two paths to one file are one keeper.
pub(crate) fn keeper_path(path: &Path, extension: &str) -> Result<(), ConnectorError> {
    let named = path.file_stem().is_some_and(|stem| !stem.is_empty())
        && path.extension().and_then(|found| found.to_str()) == Some(extension);
    if named {
        return Ok(());
    }
    let message = format!(
        "{} cannot keep positions: the file's name does not end in .{extension}",
        path.display()
    );
    Err(ConnectorError::config(message).with_code("keeper_path_invalid"))
}

/// The error of a stream that forgets what it acknowledged in a source naming no keeper, a
/// `group` or a `slot` as `keeper` says.
///
/// Every source of a process that names no keeper shares the default one, so what another
/// pipeline acknowledged there would be what this stream forgot.
pub(crate) fn unnamed(stream: &str, keeper: &str) -> ConnectorError {
    let message = format!(
        "stream {stream} does not serve again what it acknowledged, so the source names its \
         {keeper} or {keeper}_path"
    );
    ConnectorError::config(message).with_code("keeper_unnamed")
}

/// Refuses the empty `name` as a keeper's, a `group` or a `slot` as `keeper` says.
///
/// The default keeper, which sources naming none share, has the empty name: a source naming it
/// would share that keeper, not have one of its own. Any other name is a keeper of its own,
/// and never one kept in a file, which is known by its file.
pub(crate) fn keeper_name(name: Option<&str>, keeper: &str) -> Result<(), ConnectorError> {
    match name {
        Some("") => {
            let message = format!("the empty name is no {keeper}'s");
            Err(ConnectorError::config(message).with_code("keeper_name_invalid"))
        }
        _ => Ok(()),
    }
}

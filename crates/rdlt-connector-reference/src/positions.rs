//! Where a source that keeps its positions in a file may keep them.

use std::path::{Component, Path, PathBuf};

use rdlt_connector::ConnectorError;

/// Checks that `path` names a keeper's file: from the root, each directory on its way by its
/// name, and a file whose name ends in `.` and `extension`.
///
/// A path so written names its file one way, so two sources naming one file share its keeper,
/// and a keeper replaces no file but one named as a keeper's.
pub(crate) fn keeper_path(path: &Path, extension: &str) -> Result<(), ConnectorError> {
    let refused = |why: &str| {
        let message = format!("{} cannot keep positions: {why}", path.display());
        Err(ConnectorError::config(message).with_code("keeper_path_invalid"))
    };
    if !path.is_absolute() {
        return refused("the path does not start at the root");
    }
    let named = path
        .components()
        .all(|part| !matches!(part, Component::CurDir | Component::ParentDir));
    // What `components` passes over, a doubled or closing separator or a `.` on the way, is
    // found by writing the path again from its components.
    if !named || path.components().collect::<PathBuf>().as_os_str() != path.as_os_str() {
        return refused("the path names a directory other than by its name");
    }
    if path.extension().and_then(|found| found.to_str()) != Some(extension) {
        return refused(&format!("the file's name does not end in .{extension}"));
    }
    Ok(())
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

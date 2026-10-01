//! Temporary directories for tests, private whatever the process they run in inherited.

use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt as _;

use rustix::fs::Mode;

/// A new temporary directory, its user's alone, in a process whose later directories are too.
///
/// The connectors refuse a directory its group or others may write, and a directory is made
/// with the modes the process's mask leaves: a test neither inherits a mask that leaves those
/// nor relies on one that does not.
pub(crate) fn tempdir() -> std::io::Result<tempfile::TempDir> {
    rustix::process::umask(Mode::from_raw_mode(0o022));
    tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
}

//! The base revision's checkout, which the instruction counts build beside this tree.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context as _;

use super::printed;

/// A detached checkout of a revision in a temporary directory outside this tree, so none of this
/// tree's cargo configuration reaches its build; removed when dropped.
pub(super) struct Worktree {
    root: PathBuf,
    directory: tempfile::TempDir,
}

impl Worktree {
    pub(super) fn add(root: &Path, revision: &str) -> anyhow::Result<Self> {
        // A run that was killed leaves its checkout registered after its directory is gone.
        printed(root, "git", &["worktree", "prune"])?;
        let directory = tempfile::Builder::new()
            .prefix("rdlt-instructions-base-")
            .tempdir()
            .context("making a directory for the base")?;
        let path = directory
            .path()
            .to_str()
            .context("the base's directory is not UTF-8")?;
        printed(
            root,
            "git",
            &["worktree", "add", "--detach", path, revision],
        )?;
        Ok(Self {
            root: root.to_path_buf(),
            directory,
        })
    }

    pub(super) fn path(&self) -> &Path {
        self.directory.path()
    }
}

impl Drop for Worktree {
    fn drop(&mut self) {
        let removed = Command::new("git")
            .current_dir(&self.root)
            .args(["worktree", "remove", "--force"])
            .arg(self.directory.path())
            .status();
        drop(removed);
    }
}

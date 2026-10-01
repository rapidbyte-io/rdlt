//! The repository's cargo workspaces, each with a lockfile of its own.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context as _;

/// The manifests of the repository's workspaces, relative to its root.
pub(crate) const MANIFESTS: &[&str] = &["Cargo.toml", "fuzz/Cargo.toml"];

/// The lockfiles among `lockfiles` that belong to no workspace in [`MANIFESTS`].
pub(crate) fn unlisted(lockfiles: &[PathBuf]) -> Vec<PathBuf> {
    let listed = |lockfile: &Path| {
        MANIFESTS
            .iter()
            .any(|manifest| lockfile == Path::new(manifest).with_file_name("Cargo.lock"))
    };
    lockfiles
        .iter()
        .filter(|lockfile| !listed(lockfile))
        .cloned()
        .collect()
}

/// Every lockfile in the repository at `root` that git tracks or would track, relative to `root`.
pub(crate) fn lockfiles(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    tracked(root, &["Cargo.lock", "*/Cargo.lock"])
}

/// The files matching `pathspecs` that git tracks or would track in the repository at `root`,
/// relative to `root`: what it ignores is in no commit.
pub(crate) fn tracked(root: &Path, pathspecs: &[&str]) -> anyhow::Result<Vec<PathBuf>> {
    let listing = Command::new("git")
        .arg("-C")
        .arg(root)
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .arg("--")
        .args(pathspecs)
        .output()
        .context("running git ls-files")?;
    anyhow::ensure!(
        listing.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&listing.stderr).trim()
    );
    let names = String::from_utf8(listing.stdout).context("a tracked path is UTF-8")?;
    let found: BTreeSet<PathBuf> = names.split_terminator('\0').map(PathBuf::from).collect();
    Ok(found.into_iter().collect())
}

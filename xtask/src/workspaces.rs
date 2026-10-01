//! The repository's cargo workspaces, each with a lockfile of its own, and the files git holds.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context as _;
use cargo_metadata::{Metadata, MetadataCommand};

/// The manifests of the repository's workspaces, relative to its root.
pub(crate) const MANIFESTS: &[&str] = &["Cargo.toml", "fuzz/Cargo.toml"];

/// The manifests among `manifests` that are no member of a listed workspace, `members`.
pub(crate) fn unlisted(manifests: &[PathBuf], members: &[PathBuf]) -> Vec<PathBuf> {
    manifests
        .iter()
        .filter(|manifest| !members.contains(manifest))
        .cloned()
        .collect()
}

/// Every manifest in the repository at `root` that git tracks or would track, relative to `root`.
pub(crate) fn manifests(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    tracked(root, &["Cargo.toml", "*/Cargo.toml"])
}

/// What cargo knows of each workspace in [`MANIFESTS`], in the repository at `root`.
pub(crate) fn metadata(root: &Path) -> anyhow::Result<Vec<Metadata>> {
    MANIFESTS
        .iter()
        .map(|manifest| {
            MetadataCommand::new()
                .manifest_path(root.join(manifest))
                .no_deps()
                .exec()
                .with_context(|| format!("running cargo metadata on {manifest}"))
        })
        .collect()
}

/// The manifest of every workspace in [`MANIFESTS`] and of each of its packages, relative to
/// `root`.
pub(crate) fn members(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut members: BTreeSet<PathBuf> = MANIFESTS.iter().map(PathBuf::from).collect();
    for package in metadata(root)?
        .iter()
        .flat_map(|workspace| &workspace.packages)
    {
        let manifest = package.manifest_path.as_std_path();
        // A package outside the repository is no file of it, and matches no tracked manifest.
        if let Ok(relative) = manifest.strip_prefix(root) {
            members.insert(relative.to_path_buf());
        }
    }
    Ok(members.into_iter().collect())
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

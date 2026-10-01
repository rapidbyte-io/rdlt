//! `cargo xtask deny`: checks every workspace's locked dependencies with cargo-deny.

#[cfg(test)]
mod tests;

use std::ffi::OsString;
use std::path::Path;
use std::process::{Command, ExitCode};

use anyhow::Context as _;

use crate::workspaces::{self, MANIFESTS};

/// The arguments of one cargo-deny run for each workspace of the repository at `root`.
pub(crate) fn runs(root: &Path) -> Vec<Vec<OsString>> {
    MANIFESTS
        .iter()
        .map(|manifest| {
            vec![
                "deny".into(),
                "--locked".into(),
                "--manifest-path".into(),
                root.join(manifest).into_os_string(),
                "--config".into(),
                root.join("deny.toml").into_os_string(),
                "check".into(),
            ]
        })
        .collect()
}

/// Checks every workspace of the repository at `root`, and that it has no other.
#[expect(clippy::print_stdout, reason = "failures are the command's output")]
pub(crate) fn run(root: &Path) -> anyhow::Result<ExitCode> {
    let mut failed = false;
    for lockfile in workspaces::unlisted(&workspaces::lockfiles(root)?) {
        println!(
            "error: {} belongs to a workspace xtask's MANIFESTS does not list",
            lockfile.display()
        );
        failed = true;
    }
    for arguments in runs(root) {
        let status = Command::new("cargo")
            .args(&arguments)
            .status()
            .context("running cargo deny")?;
        failed |= !status.success();
    }
    Ok(if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

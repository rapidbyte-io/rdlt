//! `cargo xtask public-api`: the facade's public API, as rustdoc's JSON lists it, against the
//! snapshot beside the crate, so an accidental export shows in review.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::Path;
use std::process::ExitCode;

use anyhow::Context as _;

/// Compares the facade's public API, from rustdoc's JSON of `toolchain`, with
/// `crates/rdlt/public-api.txt`, or writes it there where `update`; blanket impls are left out.
#[expect(
    clippy::print_stdout,
    reason = "the difference is the command's output"
)]
pub(crate) fn run(root: &Path, toolchain: &str, update: bool) -> anyhow::Result<ExitCode> {
    let json = rustdoc_json::Builder::default()
        .toolchain(toolchain)
        .manifest_path(root.join("crates/rdlt/Cargo.toml"))
        .all_features(true)
        .build()
        .context("building rdlt's rustdoc JSON")?;
    // Blanket impls are the dependencies' API, and change with them, not with the facade's.
    let api = public_api::Builder::from_rustdoc_json(json)
        .omit_blanket_impls(true)
        .build()
        .context("reading rdlt's public API")?;
    let mut listed = String::new();
    for item in api.items() {
        // Writing to a `String` cannot fail.
        writeln!(listed, "{item}").ok();
    }
    let snapshot = root.join("crates/rdlt/public-api.txt");
    if update {
        std::fs::write(&snapshot, listed).context("writing the snapshot")?;
        return Ok(ExitCode::SUCCESS);
    }
    let committed = std::fs::read_to_string(&snapshot).unwrap_or_default();
    if committed == listed {
        return Ok(ExitCode::SUCCESS);
    }
    for change in changed(&committed, &listed) {
        println!("{change}");
    }
    println!("rdlt's public API changed: review it, then run `just public-api --update`");
    Ok(ExitCode::FAILURE)
}

/// Each line `listed` has and `committed` lacks, as `+ line`, then each `committed` has and
/// `listed` lacks, as `- line`.
fn changed(committed: &str, listed: &str) -> Vec<String> {
    let kept: BTreeSet<&str> = committed.lines().collect();
    let now: BTreeSet<&str> = listed.lines().collect();
    let added = listed
        .lines()
        .filter(|line| !kept.contains(line))
        .map(|line| format!("+ {line}"));
    let removed = committed
        .lines()
        .filter(|line| !now.contains(line))
        .map(|line| format!("- {line}"));
    added.chain(removed).collect()
}

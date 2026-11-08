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
    let committed = committed(&snapshot)?;
    if committed == listed {
        return Ok(ExitCode::SUCCESS);
    }
    for change in changed(&committed, &listed) {
        println!("{change}");
    }
    println!("rdlt's public API changed: review it, then run `just public-api --update`");
    Ok(ExitCode::FAILURE)
}

/// The snapshot at `path`: one that cannot be read is an error, not an empty API.
fn committed(path: &Path) -> anyhow::Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
}

/// Each line `listed` has and `committed` lacks, as `+ line`, then each `committed` has and
/// `listed` lacks, as `- line`; where they hold the same lines, how they differ all the same.
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
    let changes: Vec<String> = added.chain(removed).collect();
    if changes.is_empty() {
        return reshaped(committed, listed);
    }
    changes
}

/// How `committed` differs from `listed`, which holds the same lines: its line endings, its last
/// line's end, how often it repeats a line, or the lines' order.
fn reshaped(committed: &str, listed: &str) -> Vec<String> {
    let mut differences = Vec::new();
    if committed.contains('\r') {
        differences.push("the snapshot has CRLF line endings".to_owned());
    }
    if committed.ends_with('\n') != listed.ends_with('\n') {
        differences.push("the snapshot's last line ends differently".to_owned());
    }
    let (mut kept, mut now): (Vec<&str>, Vec<&str>) =
        (committed.lines().collect(), listed.lines().collect());
    if kept != now {
        kept.sort_unstable();
        now.sort_unstable();
        differences.push(if kept == now {
            "the snapshot's lines are in another order".to_owned()
        } else {
            "the snapshot repeats a line a different number of times".to_owned()
        });
    }
    differences
}

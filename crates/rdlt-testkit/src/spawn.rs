//! Locating a connector crate's built binary for spawn suites — the 039
//! rdlt-runtime pattern, in ONE place (042 round-2 fix wave: five
//! connector crates carried verbatim copies, so every fix to the shared
//! mechanics had to land five times or the suites diverged, one crate
//! certifying a stale binary while another rebuilt).
//!
//! The mechanics: `CARGO_TARGET_DIR` honored the way rdlt-bench honors
//! it for the CLI, the build itself guarded by
//! `RDLT_BUILD_CONNECTOR_BINS` (the Makefile's spawn-bins lines set
//! it), and a missing bin failing LOUDLY with instructions rather than
//! building behind the runner's back or quietly skipping — either would
//! be the 024 silent-pass class wearing a new hat. The binary feature is
//! the 039 T6 convention every served connector follows: `bin-serve`
//! gates a `[[bin]]` named after the crate.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The workspace root: two levels above the calling crate's manifest —
/// every `crates/<name>` member sits exactly there.
fn workspace_root(manifest_dir: &str) -> PathBuf {
    Path::new(manifest_dir)
        .ancestors()
        .nth(2)
        .expect("a workspace member's manifest sits two levels below the workspace root")
        .to_path_buf()
}

/// Where debug binaries land — `CARGO_TARGET_DIR` honored (absolute
/// used as-is, relative resolved against the repo root, exactly as
/// cargo treats it).
fn target_debug_dir(manifest_dir: &str) -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(target) => {
            let target = PathBuf::from(target);
            if target.is_absolute() {
                target.join("debug")
            } else {
                workspace_root(manifest_dir).join(target).join("debug")
            }
        }
        None => workspace_root(manifest_dir).join("target/debug"),
    }
}

/// The path to `crate_name`'s built connector bin, building it first
/// (once per crate per test process) when `RDLT_BUILD_CONNECTOR_BINS`
/// is set — the Makefile line sets it. Without the env var a missing
/// bin fails with instructions, never silently. `manifest_dir` is the
/// CALLING crate's `env!("CARGO_MANIFEST_DIR")`, which anchors the
/// workspace lookup to the caller's tree rather than wherever this
/// crate's sources live.
pub fn built_connector_bin(manifest_dir: &str, crate_name: &str) -> PathBuf {
    static BUILT: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());
    let mut built = BUILT.lock().expect("spawn-build registry lock");
    if !built.contains(crate_name) {
        if std::env::var_os("RDLT_BUILD_CONNECTOR_BINS").is_none() {
            // Opt-in rebuild, deliberately (039): the gate line sets the
            // var, and a developer running this suite in a loop should
            // not pay a cargo invocation per run. The residue is that a
            // STALE bin certifies green here, so say so out loud —
            // silence is what would make an hours-old binary look like
            // evidence about the current tree.
            eprintln!(
                "note: RDLT_BUILD_CONNECTOR_BINS is unset — spawning the \
                 {crate_name} binary already on disk WITHOUT rebuilding. \
                 Whatever this suite certifies is that binary, not necessarily the \
                 current source. The Makefile's spawn-bins lines set the var."
            );
        } else {
            let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
            let status = std::process::Command::new(&cargo)
                .current_dir(workspace_root(manifest_dir))
                .args([
                    "build",
                    "-p",
                    crate_name,
                    "--features",
                    "bin-serve",
                    "--bin",
                    crate_name,
                ])
                .status()
                .unwrap_or_else(|error| {
                    panic!("cargo build -p {crate_name} did not spawn: {error}")
                });
            assert!(
                status.success(),
                "cargo build -p {crate_name} --features bin-serve failed"
            );
        }
        built.insert(crate_name.to_owned());
    }
    drop(built);
    let path = target_debug_dir(manifest_dir).join(crate_name);
    assert!(
        path.is_file(),
        "connector binary `{}` is not built — run the Makefile's spawn-bins \
         line (it sets RDLT_BUILD_CONNECTOR_BINS=1) or `cargo build -p \
         {crate_name} --features bin-serve` first",
        path.display()
    );
    path
}

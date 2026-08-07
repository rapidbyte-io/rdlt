//! Locating the built `rdlt-connector-postgres` bin — the 039
//! rdlt-runtime pattern, replicated: `CARGO_TARGET_DIR` honored the way
//! rdlt-bench honors it for the CLI, the build itself guarded by
//! `RDLT_BUILD_CONNECTOR_BINS` (the Makefile line sets it), and a
//! missing bin failing LOUDLY with instructions rather than building
//! behind the runner's back or quietly skipping — either would be the
//! 024 silent-pass class wearing a new hat.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The workspace root: two levels above this crate's own manifest.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt-connector-postgres sits two levels below the workspace root")
        .to_path_buf()
}

/// Where debug binaries land — `CARGO_TARGET_DIR` honored (absolute
/// used as-is, relative resolved against the repo root, exactly as
/// cargo treats it).
fn target_debug_dir() -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(target) => {
            let target = PathBuf::from(target);
            if target.is_absolute() {
                target.join("debug")
            } else {
                workspace_root().join(target).join("debug")
            }
        }
        None => workspace_root().join("target/debug"),
    }
}

/// The path to the built `rdlt-connector-postgres` bin, building it
/// first (once per test process) when `RDLT_BUILD_CONNECTOR_BINS` is
/// set — the Makefile line sets it. Without the env var a missing bin
/// fails with instructions, never silently.
pub(crate) fn built_bin() -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        if std::env::var_os("RDLT_BUILD_CONNECTOR_BINS").is_none() {
            // Opt-in rebuild, deliberately (039): the gate line sets the
            // var, and a developer running this suite in a loop should
            // not pay a cargo invocation per run. The residue is that a
            // STALE bin certifies green here, so say so out loud —
            // silence is what would make an hours-old binary look like
            // evidence about the current tree.
            eprintln!(
                "note: RDLT_BUILD_CONNECTOR_BINS is unset — spawning the \
                 rdlt-connector-postgres binary already on disk WITHOUT rebuilding. \
                 Whatever this suite certifies is that binary, not necessarily the \
                 current source. The Makefile's spawn-bins lines set the var."
            );
            return;
        }
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let status = std::process::Command::new(&cargo)
            .current_dir(workspace_root())
            .args([
                "build",
                "-p",
                "rdlt-connector-postgres",
                "--features",
                "bin-serve",
                "--bin",
                "rdlt-connector-postgres",
            ])
            .status()
            .unwrap_or_else(|error| {
                panic!("cargo build -p rdlt-connector-postgres did not spawn: {error}")
            });
        assert!(
            status.success(),
            "cargo build -p rdlt-connector-postgres --features bin-serve failed"
        );
    });
    let path = target_debug_dir().join("rdlt-connector-postgres");
    assert!(
        path.is_file(),
        "connector binary `{}` is not built — run the Makefile's spawn-bins \
         line (it sets RDLT_BUILD_CONNECTOR_BINS=1) or `cargo build -p \
         rdlt-connector-postgres --features bin-serve` first",
        path.display()
    );
    path
}

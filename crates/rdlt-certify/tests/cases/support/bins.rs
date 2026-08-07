//! Locating (and optionally building) the real connector bin the
//! certification cases spawn — the 039 `built_bin` rule replicated from
//! rdlt-runtime's spawn suite: `CARGO_TARGET_DIR` honored (absolute
//! used as-is, relative resolved against the repo root, exactly as
//! cargo treats it), else the repo's own `target/debug`; the build
//! itself is guarded by `RDLT_BUILD_CONNECTOR_BINS` — without the env
//! var a missing bin fails with instructions, never silently (a suite
//! that quietly built or quietly skipped would be the 024 class wearing
//! a new hat).

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The workspace root: two levels above this crate's own manifest.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt-certify sits two levels below the workspace root")
        .to_path_buf()
}

/// Where debug binaries land.
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

/// The path to a built connector bin, building the file bin first (once
/// per test process) when `RDLT_BUILD_CONNECTOR_BINS` is set.
pub(crate) fn built_bin(name: &str) -> PathBuf {
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
                "note: RDLT_BUILD_CONNECTOR_BINS is unset — spawning the connector \
                 binary already on disk WITHOUT rebuilding. Whatever this suite \
                 certifies is that binary, not necessarily the current source. The \
                 Makefile's spawn-bins lines set the var."
            );
            return;
        }
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        // The one bin these cases spawn; a second certification subject
        // joins this list, not a second helper.
        let package = "rdlt-connector-file";
        let status = std::process::Command::new(&cargo)
            .current_dir(workspace_root())
            .args([
                "build",
                "-p",
                package,
                "--features",
                "bin-serve",
                "--bin",
                package,
            ])
            .status()
            .unwrap_or_else(|error| panic!("cargo build -p {package} did not spawn: {error}"));
        assert!(
            status.success(),
            "cargo build -p {package} --features bin-serve failed"
        );
    });
    let path = target_debug_dir().join(name);
    assert!(
        path.is_file(),
        "connector binary `{}` is not built — set RDLT_BUILD_CONNECTOR_BINS=1 \
         (the gate line does) or run `cargo build -p {name} --features bin-serve \
         --bin {name}` first",
        path.display()
    );
    path
}

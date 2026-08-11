//! Locating a connector crate's built binary for spawn suites — the 039
//! rdlt-runtime pattern, in ONE place (042 round-2 fix wave: five
//! connector crates carried verbatim copies, so every fix to the shared
//! mechanics had to land five times or the suites diverged, one crate
//! certifying a stale binary while another rebuilt).
//!
//! The mechanics: `CARGO_TARGET_DIR` honored when absolute and refused
//! when relative (see `target_debug_dir`), the build itself guarded by
//! `RDLT_BUILD_CONNECTOR_BINS` (the Makefile's spawn-bins lines set
//! it), and a missing bin failing LOUDLY with instructions rather than
//! building behind the runner's back or quietly skipping — either would
//! be the 024 silent-pass class wearing a new hat. The binary feature is
//! the 039 T6 convention every served connector follows: `bin-serve`
//! gates a `[[bin]]` named after the crate.

use std::path::{Path, PathBuf};

/// The workspace root: two levels above the calling crate's manifest —
/// every `crates/<name>` member sits exactly there.
fn workspace_root(manifest_dir: &str) -> PathBuf {
    Path::new(manifest_dir)
        .ancestors()
        .nth(2)
        .expect("a workspace member's manifest sits two levels below the workspace root")
        .to_path_buf()
}

/// Where debug binaries land — `CARGO_TARGET_DIR` honored for ABSOLUTE
/// values, refused for relative ones (round-4 fix): cargo resolves a
/// relative value against ITS OWN invocation cwd, and two cargos run
/// here with different cwds — the test process that looks the binary
/// up (the harness's cwd) and this scaffold's own `cargo build` (the
/// workspace root) — so any single resolution would be a guess that
/// makes the lookup and the build disagree about where the binary
/// lives. Refusing with instructions is the truthful option.
fn target_debug_dir(manifest_dir: &str) -> PathBuf {
    match debug_dir_from(
        std::env::var_os("CARGO_TARGET_DIR").as_deref(),
        manifest_dir,
    ) {
        Ok(dir) => dir,
        Err(why) => panic!("{why}"),
    }
}

/// The resolution rule itself, pure so its three arms pin offline.
fn debug_dir_from(
    target_dir: Option<&std::ffi::OsStr>,
    manifest_dir: &str,
) -> Result<PathBuf, String> {
    match target_dir {
        None => Ok(workspace_root(manifest_dir).join("target/debug")),
        Some(target) => {
            let target = PathBuf::from(target);
            if target.is_absolute() {
                Ok(target.join("debug"))
            } else {
                Err(format!(
                    "CARGO_TARGET_DIR `{}` is relative — cargo resolves it against each \
                     invocation's OWN cwd, and the spawn scaffold's lookup and its cargo \
                     build run from different directories, so no single resolution is \
                     truthful; set an absolute CARGO_TARGET_DIR for the spawn suites",
                    target.display()
                ))
            }
        }
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
    // NO once-per-process guard, deliberately (round-7 honesty fix: a
    // per-process registry sat here and guarded nothing — the gate's
    // runner is nextest, whose process-per-test model gives every test
    // its own process). The truth: in rebuild mode EVERY call invokes
    // cargo, concurrent invocations are serialized by cargo's own
    // target-directory lock, and a repeat build is an incremental
    // no-op — correct, bounded, and honestly redundant.
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
            .unwrap_or_else(|error| panic!("cargo build -p {crate_name} did not spawn: {error}"));
        assert!(
            status.success(),
            "cargo build -p {crate_name} --features bin-serve failed"
        );
    }
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

#[cfg(test)]
mod resolution_tests {
    use super::*;

    /// The three arms of the rule: unset falls back to the workspace's
    /// own target/, absolute is honored as-is, relative is REFUSED
    /// with instructions — never resolved by guess (the round-4 fix:
    /// the old code resolved against the workspace root while cargo
    /// resolves against the invocation cwd, so the lookup and the
    /// build could disagree).
    #[test]
    fn relative_cargo_target_dir_is_refused_not_guessed() {
        let manifest = "/repo/crates/some-crate";
        assert_eq!(
            debug_dir_from(None, manifest).expect("unset resolves"),
            PathBuf::from("/repo/target/debug")
        );
        assert_eq!(
            debug_dir_from(Some(std::ffi::OsStr::new("/abs/target")), manifest)
                .expect("absolute resolves"),
            PathBuf::from("/abs/target/debug")
        );
        let refusal = debug_dir_from(Some(std::ffi::OsStr::new("rel-target")), manifest)
            .expect_err("relative must refuse");
        assert!(
            refusal.contains("`rel-target` is relative")
                && refusal.contains("set an absolute CARGO_TARGET_DIR"),
            "the refusal names the value and the fix: {refusal}"
        );
    }
}

//! Repo-anchored paths. The harness runs from the repo root (`cargo run -p
//! rdlt-bench`) or any directory containing `benches/`.

use std::path::PathBuf;

use crate::{BenchError, Result};

#[derive(Debug, Clone)]
pub struct Paths {
    pub repo: PathBuf,
    pub benches: PathBuf,
    pub cells_dir: PathBuf,
    pub fixtures_toml: PathBuf,
    pub bars_toml: PathBuf,
    pub results: PathBuf,
    pub cli: PathBuf,
    /// Where the connector BINARIES land (`<target>/release`) — the
    /// `{{bins}}` substitution the cells' pipeline templates
    /// point their `connector: path:` overrides at, and the directory
    /// the runner prepends to the measured CLI's PATH so rich-spelling
    /// specs resolve the same bins. Release
    /// UNCONDITIONALLY, the harness convention `cli` already follows: a
    /// measured cell must spawn the shipped shape, and an absent
    /// release bin fails LOUD at spawn — a debug (or prefer-what's-
    /// present) fallback would measure an unoptimized connector
    /// silently and taint the recorded ratios.
    pub bins: PathBuf,
}

impl Paths {
    pub fn resolve() -> Result<Self> {
        let mut dir = std::env::current_dir()?;
        loop {
            if dir.join("benches").is_dir() && dir.join("Cargo.toml").is_file() {
                break;
            }
            if !dir.pop() {
                return Err(BenchError(
                    "not inside the rdlt repo (no benches/ + Cargo.toml above cwd)".into(),
                ));
            }
        }
        let benches = dir.join("benches");
        // Honour CARGO_TARGET_DIR: a contributor who redirects cargo's
        // output (a shared target dir, a faster disk) otherwise gets a
        // "CLI missing" failure straight after a successful `make release`.
        // An absolute override is used as-is; a relative one resolves
        // against the repo root, exactly as cargo itself treats it.
        let target = match std::env::var_os("CARGO_TARGET_DIR") {
            Some(target) => dir.join(target),
            None => dir.join("target"),
        };
        Ok(Self {
            cells_dir: benches.join("cells"),
            fixtures_toml: benches.join("fixtures/fixtures.toml"),
            bars_toml: benches.join("bars.toml"),
            results: benches.join("results"),
            cli: target.join("release/rdlt"),
            bins: target.join("release"),
            repo: dir,
            benches,
        })
    }
}

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
        Ok(Self {
            cells_dir: benches.join("cells"),
            fixtures_toml: benches.join("fixtures/fixtures.toml"),
            bars_toml: benches.join("bars.toml"),
            results: benches.join("results"),
            // Honour CARGO_TARGET_DIR: a contributor who redirects cargo's
            // output (a shared target dir, a faster disk) otherwise gets a
            // "CLI missing" failure straight after a successful `make release`.
            // An absolute override is used as-is; a relative one resolves
            // against the repo root, exactly as cargo itself treats it.
            cli: match std::env::var_os("CARGO_TARGET_DIR") {
                Some(target) => dir.join(target).join("release/rdlt"),
                None => dir.join("target/release/rdlt"),
            },
            repo: dir,
            benches,
        })
    }
}

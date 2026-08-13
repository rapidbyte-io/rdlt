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
    /// The live ledger: where `run` writes fresh artifacts. Empty since
    /// the 045 history reset until a recorded session re-mints baselines.
    pub results: PathBuf,
    /// The RECORDED artifacts the bars bind against — what `gate` reads,
    /// and what `report` renders the matrix from (the two must agree: a
    /// report reading the emptied live ledger would splice emptiness
    /// over the recorded tables the bars still cite). Since the 045
    /// history reset this is the dated archive of the pre-split
    /// recordings; `run` writes to `results`, never here, so a live run
    /// cannot overwrite an archived recording. The next recorded session
    /// (046) re-points this at `results` when it mints post-split
    /// baselines under 004 governance.
    pub recorded_results: PathBuf,
    /// The RECORDED history feed `report`'s Trends table renders —
    /// archived beside [`Self::recorded_results`]'s artifacts; the live
    /// feed (`benches/history.jsonl`, appended by `run`) starts empty.
    /// 046 re-points this at the live feed together with
    /// `recorded_results`.
    pub recorded_history: PathBuf,
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
        let recorded_results = benches.join("records/archive-2026-08-13");
        // KEEP TOGETHER: 046's re-point moves this pair to the live
        // ledger (`results` / `benches/history.jsonl`) in one edit —
        // gate and report read the same recorded truth either way.
        Ok(Self {
            cells_dir: benches.join("harness/cells"),
            fixtures_toml: benches.join("harness/fixtures/fixtures.toml"),
            bars_toml: benches.join("bars.toml"),
            results: benches.join("results"),
            recorded_history: recorded_results.join("history.jsonl"),
            recorded_results,
            cli: target.join("release/rdlt"),
            bins: target.join("release"),
            repo: dir,
            benches,
        })
    }
}

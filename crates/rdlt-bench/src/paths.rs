//! Repo-anchored paths. The harness runs from the repo root (`cargo run -p
//! rdlt-bench`) or any directory below it, and every command reads and
//! writes the ONE ledger under `benches/`.

use std::path::PathBuf;

use crate::cell;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct Paths {
    pub repo: PathBuf,
    pub benches: PathBuf,
    pub cells_dir: PathBuf,
    pub fixtures_toml: PathBuf,
    pub bars_toml: PathBuf,
    /// The `benches/competitors/*/variants.toml` modules.
    pub competitors_dir: PathBuf,
    /// The ledger: `run` writes fresh artifacts here, `gate` binds bars
    /// against them and `report` renders the matrix from them. Against a
    /// casual `run` overwriting a recording, git is the guard — the
    /// artifacts are tracked files, so an unrecorded run shows as a dirty
    /// tree and committing is the act of recording. Selftest output lands
    /// under `raw/` beneath it, which the recorded-artifact check ignores.
    pub results: PathBuf,
    /// The append-only history feed the Trends table renders.
    pub history: PathBuf,
    pub results_md: PathBuf,
    /// The release CLI every pipeline cell measures.
    pub cli: PathBuf,
    /// Where the connector binaries land (`<target>/release`): the
    /// `{{bins}}` substitution the pipeline templates point their
    /// `connector: path:` overrides at, and the directory prepended to the
    /// measured CLI's PATH so rich-spelling specs resolve the same bins.
    /// Release unconditionally: an absent release bin fails loud at spawn,
    /// where a debug fallback would measure an unoptimized connector
    /// silently.
    pub bins: PathBuf,
}

impl Paths {
    /// Walk up from the cwd to the repo root (the directory holding both
    /// `benches/` and `Cargo.toml`), honouring `CARGO_TARGET_DIR` for the
    /// release bins: an absolute override is used as-is, a relative one
    /// resolves against the repo root, exactly as cargo treats it.
    pub fn resolve() -> Result<Self> {
        let mut dir = std::env::current_dir()?;
        loop {
            if dir.join("benches").is_dir() && dir.join("Cargo.toml").is_file() {
                break;
            }
            if !dir.pop() {
                return Err(Error(
                    "not inside the rdlt repo (no benches/ + Cargo.toml above cwd)".into(),
                ));
            }
        }
        let target = match std::env::var_os("CARGO_TARGET_DIR") {
            Some(target) => dir.join(target),
            None => dir.join("target"),
        };
        Ok(Self::rooted(dir, target))
    }

    /// The layout under a repo root and a cargo target directory.
    pub fn rooted(repo: PathBuf, target: PathBuf) -> Self {
        let benches = repo.join("benches");
        Self {
            cells_dir: benches.join("harness/cells"),
            fixtures_toml: benches.join("harness/fixtures/fixtures.toml"),
            bars_toml: benches.join("bars.toml"),
            competitors_dir: benches.join("competitors"),
            results: benches.join("results"),
            history: benches.join("history.jsonl"),
            results_md: benches.join("RESULTS.md"),
            cli: target.join("release/rdlt"),
            bins: target.join("release"),
            repo,
            benches,
        }
    }

    /// `gate` binds bars against recorded artifacts, so a checkout without
    /// any gets ONE refusal with instructions — never an all-bars-fail
    /// drizzle of per-cell noise. Selftest output does not count: a ledger
    /// holding only that debris is as empty as a missing one. Only real
    /// files with a `.json` extension count — a stray directory is not an
    /// artifact.
    pub fn require_recorded_artifacts(&self) -> Result<()> {
        let has_recorded = std::fs::read_dir(&self.results)
            .map(|entries| {
                entries.flatten().any(|e| {
                    let path = e.path();
                    path.is_file()
                        && path.extension().is_some_and(|ext| ext == "json")
                        && path
                            .file_stem()
                            .and_then(|stem| stem.to_str())
                            .is_some_and(|stem| !cell::is_selftest(stem))
                })
            })
            .unwrap_or(false);
        if !has_recorded {
            return Err(Error(format!(
                "recorded results dir {} is missing or has no recorded artifacts (harness selftest output does not count) — gate and report bind the recorded ledger; likely cause: no recorded bench session yet (record one on a quiet machine and commit its artifacts) or a checkout without the committed recordings",
                self.results.display()
            )));
        }
        Ok(())
    }

    /// What `report` demands: the artifacts plus a non-empty history feed,
    /// because its Trends table renders the feed and a missing OR empty one
    /// would splice empty trends over the recorded table silently. `gate`
    /// demands only [`Self::require_recorded_artifacts`]: it never reads
    /// history, and the mid-reset state (artifacts committed, history
    /// rotated away) must still gate.
    pub fn require_recorded_ledger(&self) -> Result<()> {
        self.require_recorded_artifacts()?;
        let history_empty = std::fs::read_to_string(&self.history)
            .map(|feed| feed.trim().is_empty())
            .unwrap_or(true);
        if history_empty {
            return Err(Error(format!(
                "recorded history {} is missing or empty — the report's Trends table renders the recorded feed; likely cause: no recorded bench session yet (record one on a quiet machine and commit its history) or a checkout without the committed recordings",
                self.history.display()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths_at(root: &std::path::Path) -> Paths {
        Paths::rooted(root.to_path_buf(), root.join("target"))
    }

    fn results_refusal(paths: &Paths) -> String {
        format!(
            "recorded results dir {} is missing or has no recorded artifacts (harness selftest output does not count) — gate and report bind the recorded ledger; likely cause: no recorded bench session yet (record one on a quiet machine and commit its artifacts) or a checkout without the committed recordings",
            paths.results.display()
        )
    }

    fn history_refusal(paths: &Paths) -> String {
        format!(
            "recorded history {} is missing or empty — the report's Trends table renders the recorded feed; likely cause: no recorded bench session yet (record one on a quiet machine and commit its history) or a checkout without the committed recordings",
            paths.history.display()
        )
    }

    #[test]
    fn the_layout_hangs_off_the_repo_root_and_the_target_dir() {
        let paths = Paths::rooted("/r".into(), "/t".into());
        assert_eq!(paths.cells_dir, PathBuf::from("/r/benches/harness/cells"));
        assert_eq!(paths.results, PathBuf::from("/r/benches/results"));
        assert_eq!(paths.history, PathBuf::from("/r/benches/history.jsonl"));
        assert_eq!(paths.results_md, PathBuf::from("/r/benches/RESULTS.md"));
        assert_eq!(
            paths.competitors_dir,
            PathBuf::from("/r/benches/competitors")
        );
        assert_eq!(paths.cli, PathBuf::from("/t/release/rdlt"));
        assert_eq!(paths.bins, PathBuf::from("/t/release"));
    }

    #[test]
    fn a_missing_results_dir_refuses_with_instructions() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        let err = paths.require_recorded_artifacts().unwrap_err();
        assert_eq!(err.to_string(), results_refusal(&paths));
    }

    /// A ledger holding ONLY harness selftest output plus stray non-artifact
    /// files is a no-recorded-session checkout, and it must refuse exactly
    /// like a missing dir — counting the debris would let that state pass.
    #[test]
    fn a_ledger_holding_only_selftest_debris_refuses_the_same_way() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.results).unwrap();
        std::fs::write(paths.results.join("selftest-protocol.json"), "{}").unwrap();
        std::fs::write(paths.results.join("raw.txt"), "x").unwrap();
        // A directory with an artifact-shaped name is not an artifact.
        std::fs::create_dir(paths.results.join("stray.json")).unwrap();
        let err = paths.require_recorded_artifacts().unwrap_err();
        assert_eq!(err.to_string(), results_refusal(&paths));
    }

    #[test]
    fn a_missing_history_refuses_naming_the_feed() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.results).unwrap();
        std::fs::write(paths.results.join("cell.json"), "{}").unwrap();
        let err = paths.require_recorded_ledger().unwrap_err();
        assert_eq!(err.to_string(), history_refusal(&paths));
    }

    /// An existing-but-empty (or whitespace-only) feed is a truncated-history
    /// state and refuses like a missing one — blessing it would splice
    /// empty trends silently.
    #[test]
    fn an_empty_history_refuses_like_a_missing_one() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.results).unwrap();
        std::fs::write(paths.results.join("cell.json"), "{}").unwrap();
        std::fs::write(&paths.history, "\n  \n").unwrap();
        let err = paths.require_recorded_ledger().unwrap_err();
        assert_eq!(err.to_string(), history_refusal(&paths));
    }

    /// Gate demands only the artifacts half (it never reads history), so the
    /// mid-reset state — artifacts committed, history rotated away — gates
    /// fine while the report still refuses.
    #[test]
    fn gate_needs_only_artifacts_report_needs_the_history_too() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.results).unwrap();
        std::fs::write(paths.results.join("cell.json"), "{}").unwrap();
        paths.require_recorded_artifacts().unwrap();
        paths.require_recorded_ledger().unwrap_err();
    }

    #[test]
    fn a_populated_recorded_ledger_passes() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.results).unwrap();
        std::fs::write(paths.results.join("cell.json"), "{}").unwrap();
        std::fs::write(&paths.history, "{\"ts\":\"2026-08-13\"}\n").unwrap();
        paths.require_recorded_ledger().unwrap();
    }
}

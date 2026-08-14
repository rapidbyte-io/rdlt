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
    /// The live ledger: where `run` writes fresh artifacts.
    pub results: PathBuf,
    /// The RECORDED artifacts the bars bind against — what `gate` reads,
    /// and what `report` renders the matrix from (the two must agree: a
    /// report reading a different ledger than the gate would splice
    /// tables the bars do not cite). Since the 046 re-point this IS the
    /// live ledger ([`Self::results`]): the recorded truth is the
    /// COMMITTED content of `benches/results/`, re-minted by the
    /// 2026-08-13/14 post-split recorded session under 004 governance.
    /// The pre-split recordings are dated history, byte-identical under
    /// `benches/records/archive-2026-08-13/`, read by no command. An
    /// empty ledger is a refusal, not an empty table — see
    /// [`Self::require_recorded_ledger`]. Against a casual `run`
    /// overwriting a recording, GIT IS THE GUARD: the recorded artifacts
    /// and history feed are tracked files, so an unrecorded run shows as
    /// a dirty tree and committing is the act of recording (selftest
    /// output never lands here at all — `run` routes it to scratch).
    pub recorded_results: PathBuf,
    /// The RECORDED history feed `report`'s Trends table renders — since
    /// the 046 re-point, the live feed (`benches/history.jsonl`, appended
    /// by `run`), which restarted with the post-split session; the
    /// pre-split feed is archived beside the archive's artifacts.
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
        // The recorded ledger and the live ledger COINCIDE: gate and
        // report bind the committed content of the same paths `run`
        // writes to. The fields stay separate because they answer
        // different questions (where fresh artifacts land vs what the
        // bars cite), and a future history reset re-splits them here in
        // one edit — the 045 reset did exactly that, pointing the
        // recorded pair at `records/archive-2026-08-13/` until 046's
        // recorded session re-minted the live ledger.
        Ok(Self {
            cells_dir: benches.join("harness/cells"),
            fixtures_toml: benches.join("harness/fixtures/fixtures.toml"),
            bars_toml: benches.join("bars.toml"),
            results: benches.join("results"),
            recorded_history: benches.join("history.jsonl"),
            recorded_results: benches.join("results"),
            cli: target.join("release/rdlt"),
            bins: target.join("release"),
            repo: dir,
            benches,
        })
    }

    /// LOUD EMPTINESS, the artifacts half: `gate` binds bars against
    /// recorded artifacts, so a checkout without any gets a refusal with
    /// instructions — never an all-bars-fail drizzle of per-cell noise.
    /// "Recorded" excludes harness selftest output by the shared naming
    /// rule ([`crate::is_selftest`]): a ledger holding only selftest
    /// debris (gitignored, present on any machine that ran the selftest
    /// cell) is as empty as a missing one, and counting it would let
    /// exactly the no-recorded-session state this refusal exists for pass
    /// silently. Only real files with a `.json` extension count — a
    /// stray directory or unreadable name is not an artifact.
    pub fn require_recorded_artifacts(&self) -> Result<()> {
        let has_recorded = std::fs::read_dir(&self.recorded_results)
            .map(|entries| {
                entries.flatten().any(|e| {
                    let path = e.path();
                    path.is_file()
                        && path.extension().is_some_and(|ext| ext == "json")
                        && path
                            .file_stem()
                            .and_then(|stem| stem.to_str())
                            .is_some_and(|stem| !crate::is_selftest(stem))
                })
            })
            .unwrap_or(false);
        if !has_recorded {
            return Err(BenchError(format!(
                "recorded results dir {} is missing or has no recorded artifacts (harness selftest output does not count) — gate and report bind the recorded ledger; likely cause: no recorded bench session yet (record one on a quiet machine and commit its artifacts under 004 governance) or a checkout without the committed recordings",
                self.recorded_results.display()
            )));
        }
        Ok(())
    }

    /// The FULL recorded-ledger requirement — what `report` demands: the
    /// artifacts half plus a non-empty recorded history feed, because its
    /// Trends table renders the feed and a missing OR empty one would
    /// splice empty trends over the recorded table silently. `gate`
    /// deliberately demands only [`Self::require_recorded_artifacts`]:
    /// it never reads history, and the mid-reset state (artifacts
    /// committed, history rotated away) must still gate.
    pub fn require_recorded_ledger(&self) -> Result<()> {
        self.require_recorded_artifacts()?;
        let history_empty = std::fs::read_to_string(&self.recorded_history)
            .map(|feed| feed.trim().is_empty())
            .unwrap_or(true);
        if history_empty {
            return Err(BenchError(format!(
                "recorded history {} is missing or empty — the report's Trends table renders the recorded feed; likely cause: no recorded bench session yet (record one on a quiet machine and commit its history under 004 governance) or a checkout without the committed recordings",
                self.recorded_history.display()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths_at(root: &std::path::Path) -> Paths {
        Paths {
            repo: root.to_path_buf(),
            benches: root.join("benches"),
            cells_dir: root.join("benches/harness/cells"),
            fixtures_toml: root.join("benches/harness/fixtures/fixtures.toml"),
            bars_toml: root.join("benches/bars.toml"),
            results: root.join("benches/results"),
            recorded_results: root.join("benches/results"),
            recorded_history: root.join("benches/history.jsonl"),
            cli: root.join("target/release/rdlt"),
            bins: root.join("target/release"),
        }
    }

    fn results_refusal(paths: &Paths) -> String {
        format!(
            "recorded results dir {} is missing or has no recorded artifacts (harness selftest output does not count) — gate and report bind the recorded ledger; likely cause: no recorded bench session yet (record one on a quiet machine and commit its artifacts under 004 governance) or a checkout without the committed recordings",
            paths.recorded_results.display()
        )
    }

    fn history_refusal(paths: &Paths) -> String {
        format!(
            "recorded history {} is missing or empty — the report's Trends table renders the recorded feed; likely cause: no recorded bench session yet (record one on a quiet machine and commit its history under 004 governance) or a checkout without the committed recordings",
            paths.recorded_history.display()
        )
    }

    #[test]
    fn a_missing_recorded_results_dir_refuses_with_instructions() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        let err = paths.require_recorded_artifacts().unwrap_err();
        assert_eq!(err.to_string(), results_refusal(&paths));
    }

    /// The debris red: a ledger holding ONLY harness selftest output (the
    /// gitignored state any machine that ran the selftest cell is in) plus
    /// stray non-artifact files is a no-recorded-session checkout, and it
    /// must refuse exactly like a missing dir — counting the debris would
    /// resurrect the silent/noisy outcomes the refusal replaced.
    #[test]
    fn a_ledger_holding_only_selftest_debris_refuses_the_same_way() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.recorded_results).unwrap();
        std::fs::write(paths.recorded_results.join("selftest-protocol.json"), "{}").unwrap();
        std::fs::write(paths.recorded_results.join("raw.txt"), "x").unwrap();
        // A directory with an artifact-shaped name is not an artifact.
        std::fs::create_dir(paths.recorded_results.join("stray.json")).unwrap();
        let err = paths.require_recorded_artifacts().unwrap_err();
        assert_eq!(err.to_string(), results_refusal(&paths));
    }

    #[test]
    fn a_missing_recorded_history_refuses_naming_the_feed() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.recorded_results).unwrap();
        std::fs::write(paths.recorded_results.join("cell.json"), "{}").unwrap();
        let err = paths.require_recorded_ledger().unwrap_err();
        assert_eq!(err.to_string(), history_refusal(&paths));
    }

    /// The empty red: an existing-but-empty (or whitespace-only) feed is a
    /// truncated-history state and refuses like a missing one — blessing
    /// it would splice empty trends silently.
    #[test]
    fn an_empty_recorded_history_refuses_like_a_missing_one() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.recorded_results).unwrap();
        std::fs::write(paths.recorded_results.join("cell.json"), "{}").unwrap();
        std::fs::write(&paths.recorded_history, "\n  \n").unwrap();
        let err = paths.require_recorded_ledger().unwrap_err();
        assert_eq!(err.to_string(), history_refusal(&paths));
    }

    /// The split: gate demands only the artifacts half (it never reads
    /// history), so the mid-reset state — artifacts committed, history
    /// rotated away — gates fine while the report still refuses.
    #[test]
    fn gate_needs_only_artifacts_report_needs_the_history_too() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.recorded_results).unwrap();
        std::fs::write(paths.recorded_results.join("cell.json"), "{}").unwrap();
        paths.require_recorded_artifacts().unwrap();
        paths.require_recorded_ledger().unwrap_err();
    }

    #[test]
    fn a_populated_recorded_ledger_passes() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths_at(root.path());
        std::fs::create_dir_all(&paths.recorded_results).unwrap();
        std::fs::write(paths.recorded_results.join("cell.json"), "{}").unwrap();
        std::fs::write(&paths.recorded_history, "{\"ts\":\"2026-08-13\"}\n").unwrap();
        paths.require_recorded_ledger().unwrap();
    }
}

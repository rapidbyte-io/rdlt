//! Cell + bar models and loaders.
//!
//! Everything here is load-time validation: unknown fields, duplicate ids, and
//! bars naming an unknown cell die with the offender named before any container
//! is touched.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{BenchError, Result};

/// Where a run's measured statistic comes from. Every measured cell uses the
/// harness wall clock around the release-CLI child; the self-timed last-line
/// JSON convention lives on the competitor side (`protocol::last_json_field`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Timing {
    /// Harness wall clock around the child (the only measured timing).
    #[default]
    Wall,
    /// The child prints its measurement in ms as the last stdout line.
    StdoutMs,
    /// The child self-times and prints `{"seconds": …}` JSON (same convention
    /// as the dlt baselines).
    SelfJsonSeconds,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verify {
    pub table: String,
    pub expected_rows: u64,
}

/// One competitor invocation for this cell: which variant, and the argv it
/// runs in-container (with `{{conn}}`-style substitutions).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompetitorRef {
    pub variant: String,
    pub args: Vec<String>,
    /// Container mounts, `host:container[:opts]` form (substituted).
    #[serde(default)]
    pub mounts: Vec<String>,
    /// e.g. "host" for cells that reach a host-published Postgres port.
    #[serde(default)]
    pub network: Option<String>,
    /// Fewer runs than the rdlt side where the baseline is slow (recorded in
    /// the artifact); default: the cell's `runs`.
    #[serde(default)]
    pub runs: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    pub id: String,
    /// Fixtures this cell brings up (all started before the cell runs, all
    /// reset before every run). The FIRST is the primary — it supplies the
    /// `{{conn}}`/`{{data}}`/`{{port}}` substitutions and its data dir is the
    /// cell's working data. A cross-store cell (e.g. postgres source → s3
    /// destination) lists both; the non-primary endpoints are addressed by
    /// their fixed fixture ports in the pipeline spec.
    pub fixtures: Vec<String>,
    /// Pipeline-spec YAML template (relative to benches/); `{{conn}}`,
    /// `{{data}}`, `{{workdir}}` substituted by the runner.
    #[serde(default)]
    pub pipeline: Option<PathBuf>,
    /// Override argv template (selftest, hyperfine cells). Default: the
    /// release CLI running `pipeline`.
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// Shell line run before every run (hyperfine `--prepare`, subprocess
    /// pre-run cleanup/setup) — `{{spec}}` is available: templates render
    /// BEFORE prepare so untimed setup loads can run the same pipeline.
    #[serde(default)]
    pub prepare_sh: Option<String>,
    #[serde(default)]
    pub timing: Timing,
    /// Free-form knobs recorded verbatim into the artifact.
    #[serde(default)]
    pub workload: BTreeMap<String, toml::Value>,
    #[serde(default = "default_warmups")]
    pub warmups: u32,
    #[serde(default = "default_runs")]
    pub runs: u32,
    #[serde(default, rename = "competitor")]
    pub competitors: Vec<CompetitorRef>,
    #[serde(default)]
    pub verify: Option<Verify>,
    /// The cell's claim — one sentence, rendered as the matrix-row caption
    /// (FR-014). Carries any regime caveats (e.g. the dedup cell's
    /// full-redelivery note).
    #[serde(default)]
    pub note: Option<String>,
}

fn default_warmups() -> u32 {
    1
}
fn default_runs() -> u32 {
    5
}

impl Cell {
    /// A cell must say HOW to run: a pipeline template, an explicit command,
    /// or both (command referencing the rendered spec).
    fn check(&self, file: &Path) -> Result<()> {
        if self.pipeline.is_none() && self.command.is_none() {
            return Err(BenchError(format!(
                "{}: cell `{}` has neither `pipeline` nor `command`",
                file.display(),
                self.id
            )));
        }
        if self.fixtures.is_empty() {
            return Err(BenchError(format!(
                "{}: cell `{}` names no fixtures",
                file.display(),
                self.id
            )));
        }
        Ok(())
    }

    /// The primary fixture — first in the list; supplies the cell's
    /// `{{conn}}`/`{{data}}`/`{{port}}` substitutions.
    pub fn primary_fixture(&self) -> &str {
        // `check` guarantees the list is non-empty before any cell runs.
        &self.fixtures[0]
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CellFile {
    #[serde(default, rename = "cell")]
    cells: Vec<Cell>,
}

/// Load every `*.toml` under `cells_dir`. Duplicate ids are a typed error
/// naming both files.
pub fn load_cells(cells_dir: &Path) -> Result<Vec<Cell>> {
    // Unreadable directory entries are an error naming the directory, never
    // silently skipped: a cell file the OS refused to stat would otherwise
    // vanish from the matrix without a trace.
    let mut entries: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(cells_dir)
        .map_err(|e| BenchError(format!("reading {}: {e}", cells_dir.display())))?
    {
        let path = entry
            .map_err(|e| BenchError(format!("reading an entry of {}: {e}", cells_dir.display())))?
            .path();
        if path.extension().is_some_and(|e| e == "toml") {
            entries.push(path);
        }
    }
    entries.sort();

    let mut cells: Vec<Cell> = Vec::new();
    let mut seen: BTreeMap<String, PathBuf> = BTreeMap::new();
    for path in entries {
        let file: CellFile = crate::load_toml(&path)?;
        for cell in file.cells {
            cell.check(&path)?;
            if let Some(first) = seen.get(&cell.id) {
                return Err(BenchError(format!(
                    "duplicate cell id `{}`: declared in {} and {}",
                    cell.id,
                    first.display(),
                    path.display()
                )));
            }
            seen.insert(cell.id.clone(), path.clone());
            cells.push(cell);
        }
    }
    Ok(cells)
}

// ---------------------------------------------------------------------------
// Bars
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BarKind {
    /// rdlt must be at least `min_ratio`× faster than the named competitor's
    /// wall median.
    RatioVs { competitor: String, min_ratio: f64 },
    /// Absolute wall-median bound on reference hardware (the 004 cold-start
    /// form — competitor releases can never flip it).
    AbsoluteMs { max_ms: f64 },
    /// rdlt peak RSS must be at most `max_rss_ratio` of the competitor's.
    RssRatioVs {
        competitor: String,
        max_rss_ratio: f64,
    },
}

// No `deny_unknown_fields` here: serde cannot combine it with `flatten` (the
// tag+payload fields would register as unknown). The flattened `BarKind` enum
// still rejects unknown fields inside each variant.
#[derive(Debug, Clone, Deserialize)]
pub struct Bar {
    pub cell: String,
    #[serde(flatten)]
    pub kind: BarKind,
    /// Jitter allowance (percent) before a violation is declared.
    #[serde(default)]
    pub tolerance_pct: f64,
    /// Informational-only: a pointer to the evidence/version-policy record
    /// that set this bar. Required in bars.toml so every bar cites its
    /// provenance, but the gate never reads it — it documents, it does not
    /// gate.
    pub policy: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BarsFile {
    #[serde(default, rename = "bar")]
    bars: Vec<Bar>,
}

pub fn load_bars(path: &Path) -> Result<Vec<Bar>> {
    let file: BarsFile = crate::load_toml(path)?;
    Ok(file.bars)
}

/// Cross-validation: every bar references an existing cell. Enforcement is
/// measurement-first (constitution v1.1.0) — a bar is added only after a
/// recorded session, so the only structural rule left is that its cell exists.
pub fn cross_validate(cells: &[Cell], bars: &[Bar]) -> Result<()> {
    for bar in bars {
        if !cells.iter().any(|c| c.id == bar.cell) {
            return Err(BenchError(format!(
                "bars.toml names unknown cell `{}`",
                bar.cell
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir_with(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, body) in files {
            std::fs::write(dir.path().join(name), body).unwrap();
        }
        dir
    }

    const GOOD: &str = r#"
[[cell]]
id = "a-cell"
fixtures = ["none"]
pipeline = "cells/pipelines/a.yaml"
warmups = 0
runs = 3
[cell.workload]
rows = 1000
"#;

    #[test]
    fn happy_path_parses_with_defaults() {
        let dir = dir_with(&[("e2e.toml", GOOD)]);
        let cells = load_cells(dir.path()).unwrap();
        assert_eq!(cells.len(), 1);
        let c = &cells[0];
        assert_eq!(c.id, "a-cell");
        assert_eq!(c.runs, 3);
        assert_eq!(c.workload["rows"], toml::Value::Integer(1000));
        // defaults where unstated
        assert!(c.competitors.is_empty());
        assert!(c.verify.is_none());
    }

    #[test]
    fn unknown_field_is_rejected_naming_the_file() {
        let dir = dir_with(&[(
            "e2e.toml",
            "[[cell]]\nid='x'\nfixtures=['f']\npipeline='p'\nbogus=1\n",
        )]);
        let err = load_cells(dir.path()).unwrap_err().to_string();
        assert!(err.contains("e2e.toml"), "{err}");
        assert!(err.contains("bogus"), "{err}");
    }

    #[test]
    fn retired_taxonomy_keys_are_rejected_as_unknown() {
        // class/mode are gone from the schema — a stray key is a load error,
        // not a silently ignored field.
        let dir = dir_with(&[(
            "e2e.toml",
            "[[cell]]\nid='x'\nfixtures=['f']\npipeline='p'\nclass='gated'\n",
        )]);
        let err = load_cells(dir.path()).unwrap_err().to_string();
        assert!(err.contains("class"), "{err}");
    }

    #[test]
    fn duplicate_id_names_both_files() {
        let one = "[[cell]]\nid='dup'\nfixtures=['f']\npipeline='p'\n";
        let dir = dir_with(&[("a.toml", one), ("b.toml", one)]);
        let err = load_cells(dir.path()).unwrap_err().to_string();
        assert!(err.contains("duplicate cell id `dup`"), "{err}");
        assert!(err.contains("a.toml") && err.contains("b.toml"), "{err}");
    }

    #[test]
    fn cell_with_no_fixtures_is_rejected() {
        let dir = dir_with(&[("a.toml", "[[cell]]\nid='x'\nfixtures=[]\npipeline='p'\n")]);
        let err = load_cells(dir.path()).unwrap_err().to_string();
        assert!(err.contains("names no fixtures"), "{err}");
        assert!(err.contains('x'), "{err}");
    }

    #[test]
    fn cell_without_pipeline_or_command_is_rejected() {
        let dir = dir_with(&[("a.toml", "[[cell]]\nid='x'\nfixtures=['f']\n")]);
        let err = load_cells(dir.path()).unwrap_err().to_string();
        assert!(err.contains("neither `pipeline` nor `command`"), "{err}");
        assert!(err.contains('x'), "{err}");
    }

    fn bars_file(dir: &tempfile::TempDir, body: &str) -> PathBuf {
        let p = dir.path().join("bars.toml");
        std::fs::write(&p, body).unwrap();
        p
    }

    const BAR_RATIO: &str = r#"
[[bar]]
cell = "a-cell"
kind = "ratio_vs"
competitor = "dlt-pyarrow"
min_ratio = 10.0
tolerance_pct = 3.0
policy = "specs/012-bench-harness/plan.md"
"#;

    #[test]
    fn bar_kinds_parse() {
        let dir = tempfile::tempdir().unwrap();
        let p = bars_file(
            &dir,
            &format!(
                "{BAR_RATIO}\n[[bar]]\ncell='b'\nkind='absolute_ms'\nmax_ms=40.0\npolicy='x'\n\n[[bar]]\ncell='c'\nkind='rss_ratio_vs'\ncompetitor='dlt-pyarrow'\nmax_rss_ratio=0.2\npolicy='y'\n"
            ),
        );
        let bars = load_bars(&p).unwrap();
        assert_eq!(bars.len(), 3);
        assert!(
            matches!(bars[0].kind, BarKind::RatioVs { ref competitor, min_ratio } if competitor == "dlt-pyarrow" && min_ratio == 10.0)
        );
        assert!(matches!(bars[1].kind, BarKind::AbsoluteMs { max_ms } if max_ms == 40.0));
        assert!(
            matches!(bars[2].kind, BarKind::RssRatioVs { max_rss_ratio, .. } if max_rss_ratio == 0.2)
        );
    }

    #[test]
    fn every_bar_must_reference_an_existing_cell() {
        let dir = dir_with(&[("a.toml", GOOD)]);
        let cells = load_cells(dir.path()).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        // `a-cell` exists → a bar over it validates (measurement-first: the
        // only structural rule left is that the cell exists).
        let bars = load_bars(&bars_file(&tmp, BAR_RATIO)).unwrap();
        cross_validate(&cells, &bars).unwrap();
        // A bar over an unknown cell is a loud load-time error.
        let mut ghost = bars.clone();
        ghost[0].cell = "ghost".into();
        let err = cross_validate(&cells, &ghost).unwrap_err().to_string();
        assert!(err.contains("unknown cell `ghost`"), "{err}");
    }
}

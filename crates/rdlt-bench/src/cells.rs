//! Cell + bar models and loaders (data-model.md §1/§5, contract BH1).
//!
//! Everything here is load-time validation: unknown fields, duplicate ids, and
//! gated-cell↔bar mismatches die with the offender named before any container
//! is touched.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{BenchError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    Gated,
    Scoreboard,
}

impl std::fmt::Display for Class {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Class::Gated => "gated",
            Class::Scoreboard => "scoreboard",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Release-CLI child process; the GATED measurement configuration (FR-011).
    Subprocess,
    /// In-process via the `rdlt` crate: RunReport + events attribution.
    Library,
    /// Cold-start only: shells the recorded hyperfine protocol (R8).
    Hyperfine,
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
    pub class: Class,
    pub mode: Mode,
    pub fixture: String,
    /// Pipeline-spec YAML template (relative to benches/); `{{conn}}`,
    /// `{{data}}`, `{{workdir}}` substituted by the runner.
    #[serde(default)]
    pub pipeline: Option<PathBuf>,
    /// Override argv template (selftest, hyperfine cells). Default: the
    /// release CLI running `pipeline`.
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// Shell line run before every run (hyperfine `--prepare`, subprocess
    /// pre-run cleanup) — e.g. removing the cold-start workdir.
    #[serde(default)]
    pub prepare_sh: Option<String>,
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
    /// Suite = the cell file's stem (e2e, pg, …); filled by the loader.
    #[serde(skip)]
    pub suite: String,
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
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CellFile {
    #[serde(default, rename = "cell")]
    cells: Vec<Cell>,
}

/// Load every `*.toml` under `cells_dir`. Duplicate ids are a typed error
/// naming both files (BH1).
pub fn load_cells(cells_dir: &Path) -> Result<Vec<Cell>> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(cells_dir)
        .map_err(|e| BenchError(format!("reading {}: {e}", cells_dir.display())))?
        .filter_map(|d| d.ok().map(|d| d.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "toml"))
        .collect();
    entries.sort();

    let mut cells: Vec<Cell> = Vec::new();
    let mut seen: BTreeMap<String, PathBuf> = BTreeMap::new();
    for path in entries {
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| BenchError(format!("reading {}: {e}", path.display())))?;
        let file: CellFile = toml::from_str(&raw)
            .map_err(|e| BenchError(format!("parsing {}: {e}", path.display())))?;
        let suite = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        for mut cell in file.cells {
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
            cell.suite = suite.clone();
            cells.push(cell);
        }
    }
    Ok(cells)
}

// ---------------------------------------------------------------------------
// Bars (data-model.md §5)
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
    RssRatioVs { competitor: String, max_rss_ratio: f64 },
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
    /// Pointer to the evidence/version-policy record that set this bar.
    pub policy: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BarsFile {
    #[serde(default, rename = "bar")]
    bars: Vec<Bar>,
}

pub fn load_bars(path: &Path) -> Result<Vec<Bar>> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| BenchError(format!("reading {}: {e}", path.display())))?;
    let file: BarsFile =
        toml::from_str(&raw).map_err(|e| BenchError(format!("parsing {}: {e}", path.display())))?;
    Ok(file.bars)
}

/// BH1 cross-validation: gated ↔ bar is bijective over gated cells (a gated
/// cell without a bar and a bar over an unknown or scoreboard cell are both
/// load-time errors).
pub fn cross_validate(cells: &[Cell], bars: &[Bar]) -> Result<()> {
    for cell in cells.iter().filter(|c| c.class == Class::Gated) {
        if !bars.iter().any(|b| b.cell == cell.id) {
            return Err(BenchError(format!(
                "gated cell `{}` has no bar in bars.toml",
                cell.id
            )));
        }
    }
    for bar in bars {
        match cells.iter().find(|c| c.id == bar.cell) {
            None => {
                return Err(BenchError(format!(
                    "bars.toml names unknown cell `{}`",
                    bar.cell
                )));
            }
            Some(c) if c.class != Class::Gated => {
                return Err(BenchError(format!(
                    "bars.toml names cell `{}` but it is {}, not gated — scoreboard cells never gate",
                    bar.cell, c.class
                )));
            }
            Some(_) => {}
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
class = "scoreboard"
mode = "subprocess"
fixture = "none"
pipeline = "cells/pipelines/a.yaml"
warmups = 0
runs = 3
[cell.workload]
rows = 1000
"#;

    #[test]
    fn happy_path_parses_with_defaults_and_suite() {
        let dir = dir_with(&[("e2e.toml", GOOD)]);
        let cells = load_cells(dir.path()).unwrap();
        assert_eq!(cells.len(), 1);
        let c = &cells[0];
        assert_eq!(c.id, "a-cell");
        assert_eq!(c.suite, "e2e");
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
            "[[cell]]\nid='x'\nclass='gated'\nmode='subprocess'\nfixture='f'\npipeline='p'\nbogus=1\n",
        )]);
        let err = load_cells(dir.path()).unwrap_err().to_string();
        assert!(err.contains("e2e.toml"), "{err}");
        assert!(err.contains("bogus"), "{err}");
    }

    #[test]
    fn duplicate_id_names_both_files() {
        let one = "[[cell]]\nid='dup'\nclass='scoreboard'\nmode='subprocess'\nfixture='f'\npipeline='p'\n";
        let dir = dir_with(&[("a.toml", one), ("b.toml", one)]);
        let err = load_cells(dir.path()).unwrap_err().to_string();
        assert!(err.contains("duplicate cell id `dup`"), "{err}");
        assert!(err.contains("a.toml") && err.contains("b.toml"), "{err}");
    }

    #[test]
    fn cell_without_pipeline_or_command_is_rejected() {
        let dir = dir_with(&[(
            "a.toml",
            "[[cell]]\nid='x'\nclass='scoreboard'\nmode='subprocess'\nfixture='f'\n",
        )]);
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
        assert!(matches!(bars[0].kind, BarKind::RatioVs { ref competitor, min_ratio } if competitor == "dlt-pyarrow" && min_ratio == 10.0));
        assert!(matches!(bars[1].kind, BarKind::AbsoluteMs { max_ms } if max_ms == 40.0));
        assert!(matches!(bars[2].kind, BarKind::RssRatioVs { max_rss_ratio, .. } if max_rss_ratio == 0.2));
    }

    #[test]
    fn gated_cell_without_bar_is_rejected() {
        let dir = dir_with(&[(
            "a.toml",
            "[[cell]]\nid='g'\nclass='gated'\nmode='subprocess'\nfixture='f'\npipeline='p'\n",
        )]);
        let cells = load_cells(dir.path()).unwrap();
        let err = cross_validate(&cells, &[]).unwrap_err().to_string();
        assert!(err.contains("gated cell `g` has no bar"), "{err}");
    }

    #[test]
    fn bar_over_unknown_or_scoreboard_cell_is_rejected() {
        let dir = dir_with(&[("a.toml", GOOD)]);
        let cells = load_cells(dir.path()).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let bars = load_bars(&bars_file(&tmp, BAR_RATIO)).unwrap();
        // `a-cell` exists but is scoreboard:
        let err = cross_validate(&cells, &bars).unwrap_err().to_string();
        assert!(err.contains("scoreboard cells never gate"), "{err}");
        // unknown cell:
        let mut bars2 = bars.clone();
        bars2[0].cell = "ghost".into();
        let err2 = cross_validate(&cells, &bars2).unwrap_err().to_string();
        assert!(err2.contains("unknown cell `ghost`"), "{err2}");
    }
}

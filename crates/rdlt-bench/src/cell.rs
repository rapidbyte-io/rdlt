//! Cells: the declarative matrix rows under `benches/harness/cells/*.toml`.
//! Everything here is load-time validation — unknown fields, duplicate ids,
//! a cell with no way to run — refused with the offender named before any
//! container is touched. `Selection` is the CLI's `<cell>` / `--filter`
//! pair.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::artifact;
use crate::error::{Error, Result};

/// Where a run's measured statistic comes from: the harness wall clock
/// around the release-CLI child. Competitor arms self-time instead (their
/// summary line), which is why the enum exists at all.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Timing {
    #[default]
    Wall,
}

/// One competitor invocation for this cell: which variant, and the argv it
/// runs (with `{{conn}}`-style substitutions).
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
    /// Shell line printing this arm's output size in bytes, run AFTER the
    /// timed runs with the same substitutions as `prepare_sh`. Per arm
    /// because each arm writes to its own place.
    #[serde(default)]
    pub artifact_bytes_sh: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    pub id: String,
    /// Fixtures this cell brings up (all started before the cell runs, all
    /// reset before every run). The FIRST is the primary — it supplies the
    /// `{{conn}}`/`{{data}}`/`{{port}}` substitutions and its data dir is
    /// the cell's working data; a cross-store cell lists both stores and
    /// addresses the non-primary one by its fixed port in the pipeline spec.
    pub fixtures: Vec<String>,
    /// Pipeline-spec YAML template (relative to benches/), rendered with the
    /// product's substitution keys.
    #[serde(default)]
    pub pipeline: Option<PathBuf>,
    /// Override argv template (the selftest cell). Default: the release CLI
    /// running `pipeline`.
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// Shell line run before every run — `{{spec}}` is available: templates
    /// render BEFORE prepare so untimed setup loads can run the same
    /// pipeline.
    #[serde(default)]
    pub prepare_sh: Option<String>,
    /// Shell line printing rdlt's output size in bytes, run AFTER the timed
    /// runs with the same substitutions as `prepare_sh`.
    #[serde(default)]
    pub artifact_bytes_sh: Option<String>,
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
    /// The cell's declared destination shape: every table it expects with
    /// the row count each must hold. The map IS the claim — a run that lands
    /// a table absent from it moved rows the cell never said it would.
    #[serde(default)]
    pub verify: Option<artifact::Verify>,
    /// The cell's claim in one sentence, rendered as the matrix-row caption;
    /// carries any regime caveats.
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
    /// A cell must say HOW to run (a pipeline template, an explicit command,
    /// or both), name at least one fixture, and — for a pipeline — declare
    /// what its destination should hold, or the run has nothing to check
    /// its delivered tables against and extra streams pass unnoticed.
    fn check(&self, file: &Path) -> Result<()> {
        if self.pipeline.is_none() && self.command.is_none() {
            return Err(Error(format!(
                "{}: cell `{}` has neither `pipeline` nor `command`",
                file.display(),
                self.id
            )));
        }
        if self.fixtures.is_empty() {
            return Err(Error(format!(
                "{}: cell `{}` names no fixtures",
                file.display(),
                self.id
            )));
        }
        if self.pipeline.is_some() && self.verify.as_ref().is_none_or(artifact::Verify::is_empty) {
            return Err(Error(format!(
                "{}: cell `{}` runs a pipeline but declares no `[cell.verify]` entries — \
                 add one `<table> = <rows>` line per table the run must land",
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
/// naming both files; an unreadable directory entry is an error naming the
/// directory, never a silently vanished cell.
pub fn load(cells_dir: &Path) -> Result<Vec<Cell>> {
    let mut entries: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(cells_dir)
        .map_err(|e| Error(format!("reading {}: {e}", cells_dir.display())))?
    {
        let path = entry
            .map_err(|e| Error(format!("reading an entry of {}: {e}", cells_dir.display())))?
            .path();
        if path.extension().is_some_and(|e| e == "toml") {
            entries.push(path);
        }
    }
    entries.sort();

    let mut cells: Vec<Cell> = Vec::new();
    let mut seen: BTreeMap<String, PathBuf> = BTreeMap::new();
    for path in entries {
        let file: CellFile = crate::error::load_toml(&path)?;
        for cell in file.cells {
            cell.check(&path)?;
            if let Some(first) = seen.get(&cell.id) {
                return Err(Error(format!(
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

/// Cells whose output is harness machinery, never a product recording —
/// the ONE naming rule shared by the RESULTS.md matrix, the Trends table,
/// the recorded-ledger requirement, and `run`'s output routing.
pub fn is_selftest(id: &str) -> bool {
    id.starts_with("selftest")
}

/// The CLI's cell selection: an exact id and/or a `*`-glob over ids.
#[derive(Debug, clap::Args)]
pub struct Selection {
    /// A single cell id
    pub cell: Option<String>,
    /// Glob over cell ids (`*` wildcards, e.g. 'pg-*')
    #[arg(long)]
    pub filter: Option<String>,
}

impl Selection {
    pub fn selects(&self, cell: &Cell) -> bool {
        self.cell.as_deref().is_none_or(|id| id == cell.id)
            && self
                .filter
                .as_deref()
                .is_none_or(|g| glob_match(g, &cell.id))
    }
}

/// `*`-only glob — the ids are kebab-case; anything fancier is unneeded.
fn glob_match(pattern: &str, id: &str) -> bool {
    fn inner(p: &[u8], s: &[u8]) -> bool {
        match (p.first(), s.first()) {
            (None, None) => true,
            (Some(b'*'), _) => inner(&p[1..], s) || (!s.is_empty() && inner(p, &s[1..])),
            (Some(c), Some(d)) if c == d => inner(&p[1..], &s[1..]),
            _ => false,
        }
    }
    inner(pattern.as_bytes(), id.as_bytes())
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
[cell.verify]
events = 1000
"#;

    #[test]
    fn happy_path_parses_with_defaults() {
        let dir = dir_with(&[("e2e.toml", GOOD)]);
        let cells = load(dir.path()).unwrap();
        assert_eq!(cells.len(), 1);
        let c = &cells[0];
        assert_eq!(c.id, "a-cell");
        assert_eq!(c.runs, 3);
        assert_eq!(c.workload["rows"], toml::Value::Integer(1000));
        // defaults where unstated
        assert!(c.competitors.is_empty());
        assert_eq!(c.timing, Timing::Wall);
        assert_eq!(c.verify.as_ref().unwrap()["events"], 1000);
    }

    #[test]
    fn unknown_field_is_rejected_naming_the_file() {
        let dir = dir_with(&[(
            "e2e.toml",
            "[[cell]]\nid='x'\nfixtures=['f']\npipeline='p'\nbogus=1\n",
        )]);
        let err = load(dir.path()).unwrap_err().to_string();
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
        let err = load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("class"), "{err}");
    }

    /// Only the wall clock exists as a timing mode; a cell asking for any
    /// other is refused at load, naming the value.
    #[test]
    fn a_non_wall_timing_mode_is_rejected_as_unknown() {
        let dir = dir_with(&[(
            "e2e.toml",
            "[[cell]]\nid='x'\nfixtures=['f']\ncommand=['true']\ntiming='self_timed'\n",
        )]);
        let err = load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("self_timed"), "{err}");
    }

    #[test]
    fn duplicate_id_names_both_files() {
        let one = "[[cell]]\nid='dup'\nfixtures=['f']\npipeline='p'\n[cell.verify]\nt=1\n";
        let dir = dir_with(&[("a.toml", one), ("b.toml", one)]);
        let err = load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("duplicate cell id `dup`"), "{err}");
        assert!(err.contains("a.toml") && err.contains("b.toml"), "{err}");
    }

    #[test]
    fn cell_with_no_fixtures_is_rejected() {
        let dir = dir_with(&[("a.toml", "[[cell]]\nid='x'\nfixtures=[]\npipeline='p'\n")]);
        let err = load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("names no fixtures"), "{err}");
        assert!(err.contains('x'), "{err}");
    }

    #[test]
    fn cell_without_pipeline_or_command_is_rejected() {
        let dir = dir_with(&[("a.toml", "[[cell]]\nid='x'\nfixtures=['f']\n")]);
        let err = load(dir.path()).unwrap_err().to_string();
        assert!(err.contains("neither `pipeline` nor `command`"), "{err}");
        assert!(err.contains('x'), "{err}");
    }

    #[test]
    fn selection_matches_by_id_and_by_star_glob() {
        let cells = load(dir_with(&[("e2e.toml", GOOD)]).path()).unwrap();
        let cell = &cells[0];
        let pick = |id: Option<&str>, filter: Option<&str>| Selection {
            cell: id.map(str::to_owned),
            filter: filter.map(str::to_owned),
        };
        assert!(pick(None, None).selects(cell));
        assert!(pick(Some("a-cell"), None).selects(cell));
        assert!(!pick(Some("b-cell"), None).selects(cell));
        assert!(pick(None, Some("a-*")).selects(cell));
        assert!(pick(None, Some("*cell")).selects(cell));
        assert!(!pick(None, Some("pg-*")).selects(cell));
        assert!(!pick(Some("a-cell"), Some("pg-*")).selects(cell));
    }
}

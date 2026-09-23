//! `cargo xtask coverage-gate`: fails when llvm-cov line or branch coverage is below a threshold.

#[cfg(test)]
mod tests;

use std::fs;
use std::path::Path;
use std::process::ExitCode;

use anyhow::Context as _;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Export {
    data: Vec<ExportData>,
}

#[derive(Debug, Deserialize)]
struct ExportData {
    totals: Totals,
}

#[derive(Debug, Deserialize)]
struct Totals {
    lines: Summary,
    branches: Summary,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct Summary {
    count: u64,
    covered: u64,
}

impl Summary {
    /// Percentage covered; a metric with nothing to cover counts as fully covered.
    fn percent(self) -> f64 {
        if self.count == 0 {
            return 100.0;
        }
        #[expect(clippy::cast_precision_loss, reason = "counts are far below 2^52")]
        let percent = self.covered as f64 * 100.0 / self.count as f64;
        percent
    }
}

/// A metric below its required percentage.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Shortfall {
    pub(crate) metric: &'static str,
    pub(crate) actual: f64,
    pub(crate) required: f64,
}

/// The metrics in an llvm-cov JSON export that fall below `min_lines` or `min_branches`.
pub(crate) fn shortfalls(
    json: &str,
    min_lines: f64,
    min_branches: f64,
) -> anyhow::Result<Vec<Shortfall>> {
    let export: Export = serde_json::from_str(json).context("parsing the llvm-cov JSON export")?;
    let totals = &export
        .data
        .first()
        .context("the llvm-cov export has no data")?
        .totals;
    let mut found = Vec::new();
    for (metric, summary, required) in [
        ("lines", totals.lines, min_lines),
        ("branches", totals.branches, min_branches),
    ] {
        let actual = summary.percent();
        if actual < required {
            found.push(Shortfall {
                metric,
                actual,
                required,
            });
        }
    }
    Ok(found)
}

/// Reads the export at `path`, prints any shortfall and fails when there is one.
#[expect(clippy::print_stdout, reason = "shortfalls are the command's output")]
pub(crate) fn run(path: &Path, min_lines: f64, min_branches: f64) -> anyhow::Result<ExitCode> {
    let json = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let found = shortfalls(&json, min_lines, min_branches)?;
    for Shortfall {
        metric,
        actual,
        required,
    } in &found
    {
        println!("error: {metric} coverage {actual:.2}% is below {required:.2}%");
    }
    Ok(if found.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

//! `cargo xtask sim-rate`: fails when a simulation sweep ran fewer seeds a second than a floor.

#[cfg(test)]
mod tests;

use std::fs;
use std::path::Path;
use std::process::ExitCode;

use anyhow::Context as _;
use serde::Deserialize;

/// A sweep's summary, the last line of its timing file.
#[derive(Debug, Deserialize)]
struct Summary {
    seeds: u64,
    failed: u64,
    seeds_per_s: f64,
}

/// The seeds a second of the sweep whose timing lines are `timings`, which ran a seed and in
/// which none failed.
pub(crate) fn rate(timings: &str) -> anyhow::Result<f64> {
    let last = timings
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .context("the timing file is empty")?;
    let summary: Summary =
        serde_json::from_str(last).context("the timing file's last line is no summary")?;
    anyhow::ensure!(summary.seeds > 0, "the sweep ran no seeds");
    anyhow::ensure!(
        summary.failed == 0,
        "{} of the sweep's seeds failed",
        summary.failed
    );
    Ok(summary.seeds_per_s)
}

/// Reads the timing file at `path`, prints its rate, and fails when its sweep ran fewer than
/// `floor` seeds a second.
#[expect(clippy::print_stdout, reason = "the rate is the command's output")]
pub(crate) fn run(path: &Path, floor: f64) -> anyhow::Result<ExitCode> {
    let timings =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let rate = rate(&timings)?;
    println!("{rate:.2} seeds/s against a floor of {floor:.2}");
    Ok(if rate >= floor {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

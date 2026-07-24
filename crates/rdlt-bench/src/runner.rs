//! The one protocol that runs every cell: fixtures up, competitors FIRST
//! (baseline-first discipline — the competitor runs before the rdlt side on
//! the same quiet machine), then rdlt — warmups, N runs, stats, artifact.
//! Every measured number comes from the release-CLI subprocess.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::artifact::{Artifact, CpuStats, RdltSide, RssStats, VerifyOutcome};
use crate::cells::{Cell, Timing};
use crate::paths::Paths;
use crate::protocol::{self, Sample};
use crate::sample::{ResourceUsage, Sampler};
use crate::template::substitute;
use crate::{BenchError, Result};

/// Attach the path an io failure concerns — a bare `?` on `std::fs` yields
/// only "io: {e}" (the `From<io::Error>` shape) with no offender named.
fn at(path: &Path) -> impl Fn(std::io::Error) -> BenchError + '_ {
    move |e| BenchError(format!("{}: {e}", path.display()))
}

/// Rows/bytes totals a RunReport JSON attributes to its tables.
fn report_totals(report: &serde_json::Value) -> (u64, u64) {
    let mut rows = 0;
    let mut bytes = 0;
    if let Some(tables) = report.get("tables").and_then(|t| t.as_object()) {
        for table in tables.values() {
            rows += table.get("rows").and_then(|v| v.as_u64()).unwrap_or(0);
            bytes += table.get("bytes").and_then(|v| v.as_u64()).unwrap_or(0);
        }
    }
    (rows, bytes)
}

fn report_table_rows(report: &serde_json::Value, table: &str) -> u64 {
    report
        .get("tables")
        .and_then(|t| t.get(table))
        .and_then(|t| t.get("rows"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

/// Detail attached to each counted run.
#[derive(Debug, Default)]
pub struct RunDetail {
    pub report: Option<serde_json::Value>,
    pub usage: Option<ResourceUsage>,
    /// Harness wall clock around the child — the window the sampler ran
    /// over. For self-timed cells this differs from `wall_ms` (the reported
    /// measurement), and CPU utilization must divide by THIS (finding 7).
    pub clock_ms: f64,
}

/// Render the cell's pipeline template into `run_dir/pipeline.yaml` and expose
/// it to later substitutions as `{{spec}}`. Runs BEFORE prepare_sh so untimed
/// setup (snapshot loads, seed refreshes) can drive the very pipeline. `None`
/// when the cell has no `pipeline` (a custom `command` cell).
fn render_spec(
    cell: &Cell,
    subs: &mut BTreeMap<String, String>,
    paths: &Paths,
    run_dir: &Path,
) -> Result<Option<PathBuf>> {
    let Some(template) = &cell.pipeline else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(paths.benches.join(template)).map_err(|e| {
        BenchError(format!(
            "cell `{}`: reading template {}: {e}",
            cell.id,
            template.display()
        ))
    })?;
    let spec = run_dir.join("pipeline.yaml");
    std::fs::write(&spec, substitute(&raw, subs)).map_err(at(&spec))?;
    subs.insert("spec".into(), spec.display().to_string());
    Ok(Some(spec))
}

/// Run the cell's untimed `prepare_sh` (seed refresh, state wipe) if declared.
fn run_prepare(cell: &Cell, subs: &BTreeMap<String, String>) -> Result<()> {
    let Some(prepare) = &cell.prepare_sh else {
        return Ok(());
    };
    let script = substitute(prepare, subs);
    let status = std::process::Command::new("sh")
        .args(["-c", &script])
        .status()?;
    if !status.success() {
        return Err(BenchError(format!("cell `{}`: prepare_sh failed", cell.id)));
    }
    Ok(())
}

/// The measured argv: a custom `command` (substituted), else the release CLI
/// running the rendered spec with a `--report` sink.
fn measured_argv(
    cell: &Cell,
    subs: &BTreeMap<String, String>,
    paths: &Paths,
    spec_path: Option<&PathBuf>,
    report_path: &Path,
) -> Vec<String> {
    match &cell.command {
        Some(custom) => custom.iter().map(|a| substitute(a, subs)).collect(),
        None => {
            let spec = spec_path.expect("checked at load: pipeline or command");
            vec![
                paths.cli.display().to_string(),
                "run".into(),
                spec.display().to_string(),
                "--report".into(),
                report_path.display().to_string(),
            ]
        }
    }
}

/// The reported measurement for one run, per the cell's timing mode: harness
/// wall clock, a numeric stdout line, or a `seconds` field in self-reported
/// JSON (the latter two let a self-timing command exclude its own setup).
fn measured_wall_ms(cell: &Cell, output: &std::process::Output, clock_ms: f64) -> Result<f64> {
    match cell.timing {
        Timing::Wall => Ok(clock_ms),
        Timing::StdoutMs => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout
                .lines()
                .rev()
                .find_map(|l| l.trim().parse::<f64>().ok())
                .ok_or_else(|| {
                    BenchError(format!(
                        "cell `{}`: timing=stdout_ms but no numeric line on stdout: {stdout}",
                        cell.id
                    ))
                })
        }
        Timing::SelfJsonSeconds => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            protocol::last_json_field(&stdout, "seconds")
                .and_then(|v| v.as_f64())
                .map(|s| s * 1000.0)
                .ok_or_else(|| {
                    BenchError(format!(
                        "cell `{}`: timing=self_json_seconds but no `seconds` JSON on stdout: {stdout}",
                        cell.id
                    ))
                })
        }
    }
}

fn run_once_subprocess(
    cell: &Cell,
    subs: &BTreeMap<String, String>,
    paths: &Paths,
    run_dir: &Path,
    seq: u32,
    counted: bool,
) -> Result<Sample<RunDetail>> {
    let mut subs = subs.clone();
    subs.insert("workdir".into(), run_dir.display().to_string());
    // Per-run sequence for prepare scripts that need run-unique mutations
    // (the merge-strategy 50%-changed regime, finding 1).
    subs.insert("run".into(), seq.to_string());

    let report_path = run_dir.join("report.json");
    let spec_path = render_spec(cell, &mut subs, paths, run_dir)?;
    run_prepare(cell, &subs)?;
    let argv = measured_argv(cell, &subs, paths, spec_path.as_ref(), &report_path);

    let capture_stdout = cell.timing != Timing::Wall;
    let started = Instant::now();
    let child = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .current_dir(run_dir)
        .stdout(if capture_stdout {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        })
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| BenchError(format!("cell `{}`: spawning {}: {e}", cell.id, argv[0])))?;
    let sampler = counted.then(|| Sampler::spawn(child.id()));
    let output = child.wait_with_output()?;
    let clock_ms = started.elapsed().as_secs_f64() * 1000.0;
    let usage = sampler.map(Sampler::stop);

    if !output.status.success() {
        return Err(BenchError(format!(
            "cell `{}`: run failed ({}): {}",
            cell.id,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let wall_ms = measured_wall_ms(cell, &output, clock_ms)?;

    let report = report_path
        .exists()
        .then(|| std::fs::read_to_string(&report_path).ok())
        .flatten()
        .and_then(|raw| serde_json::from_str(&raw).ok());
    Ok(Sample {
        wall_ms,
        detail: RunDetail {
            report,
            usage,
            clock_ms,
        },
    })
}

/// CPU stats from the (last counted run's) sampler series. The denominator
/// is the SAMPLER window (process wall clock), never a self-reported
/// measurement window (finding 7).
fn cpu_stats(usage: Option<&ResourceUsage>, sampled_window_ms: f64) -> CpuStats {
    let Some(usage) = usage else {
        return CpuStats {
            note: Some("no sampler ran".into()),
            ..CpuStats::default()
        };
    };
    let Some(cpu_ms) = usage.cpu_ms else {
        return CpuStats {
            note: usage.note.clone(),
            ..CpuStats::default()
        };
    };
    let mean = (cpu_ms as f64 / sampled_window_ms).max(0.0);
    let peak = usage
        .series
        .windows(2)
        .filter_map(|w| {
            let dt = w[1].at_ms.saturating_sub(w[0].at_ms);
            let dc = w[1].cpu_ms.saturating_sub(w[0].cpu_ms);
            (dt > 0).then(|| dc as f64 / dt as f64)
        })
        .fold(None::<f64>, |acc, u| Some(acc.map_or(u, |a| a.max(u))));
    CpuStats {
        mean_util: Some(mean),
        peak_util: peak.or(Some(mean)),
        user_sys_ms: Some(cpu_ms),
        note: usage.note.clone(),
    }
}

fn rss_stats(usage: Option<&ResourceUsage>) -> RssStats {
    match usage {
        Some(u) => RssStats {
            peak_bytes: u.peak_rss_bytes,
            note: u.note.clone(),
        },
        None => RssStats {
            peak_bytes: None,
            note: Some("no sampler ran".into()),
        },
    }
}

/// Assemble the rdlt side from counted samples (+ the last run's detail).
pub(crate) fn rdlt_side(samples: &[Sample<RunDetail>]) -> RdltSide {
    let runs_ms: Vec<f64> = samples.iter().map(|s| s.wall_ms).collect();
    let median_ms = protocol::median(&runs_ms);
    let p95_ms = protocol::p95(&runs_ms);
    let last = samples.last().expect("protocol guarantees >= 1 run");
    let (rows, bytes) = last
        .detail
        .report
        .as_ref()
        .map(report_totals)
        .map_or((None, None), |(r, b)| (Some(r), Some(b)));
    let secs = median_ms / 1000.0;
    RdltSide {
        median_ms,
        p95_ms,
        rows,
        bytes,
        rows_per_s: rows.map(|r| r as f64 / secs),
        mb_per_s: bytes.map(|b| b as f64 / (1024.0 * 1024.0) / secs),
        cpu: cpu_stats(last.detail.usage.as_ref(), last.detail.clock_ms),
        rss: rss_stats(last.detail.usage.as_ref()),
        streams: vec![],
        runs_ms,
    }
}

fn verify_outcome(cell: &Cell, samples: &[Sample<RunDetail>]) -> Result<Option<VerifyOutcome>> {
    let Some(verify) = &cell.verify else {
        return Ok(None);
    };
    let report = samples
        .last()
        .and_then(|s| s.detail.report.as_ref())
        .ok_or_else(|| {
            BenchError(format!(
                "cell `{}`: verify declared but no RunReport captured",
                cell.id
            ))
        })?;
    let actual = report_table_rows(report, &verify.table);
    if actual != verify.expected_rows {
        return Err(BenchError(format!(
            "cell `{}`: verify FAILED — table `{}` has {actual} rows, expected {}",
            cell.id, verify.table, verify.expected_rows
        )));
    }
    Ok(Some(VerifyOutcome {
        table: verify.table.clone(),
        expected_rows: verify.expected_rows,
        actual_rows: actual,
    }))
}

/// Run one cell end to end and return its artifact (not yet written).
pub fn run_cell(
    cell: &Cell,
    paths: &Paths,
    fixture: &crate::fixtures::Started,
    competitor_pins: BTreeMap<String, String>,
    competitors: BTreeMap<String, crate::artifact::CompetitorSide>,
    // Quiet-guard verdict, obtained by the CALLER before any competitor ran
    // (finding 2: the baseline side must be guarded too).
    quiet_note: Option<String>,
    // Whether the quiet guard was overridden (`RDLT_BENCH_FORCE=1`) — stamped
    // into the artifact so a forced number is never mistaken for evidence.
    forced: bool,
) -> Result<Artifact> {
    let mut subs: BTreeMap<String, String> = BTreeMap::new();
    subs.insert("repo".into(), paths.repo.display().to_string());
    subs.insert("benches".into(), paths.benches.display().to_string());
    subs.insert("cli".into(), paths.cli.display().to_string());
    subs.insert("data".into(), fixture.data_dir.path().display().to_string());
    if let Some(conn) = fixture.conn() {
        subs.insert("conn".into(), conn.to_owned());
    }
    if let Some(port) = fixture.def.port {
        subs.insert("port".into(), port.to_string());
    }

    let invocation = tempfile::tempdir().map_err(|e| BenchError(format!("tempdir: {e}")))?;
    let mut run_seq = 0u32;

    // Every cell runs the same way: the release-CLI subprocess, warmups then N
    // counted runs. A `command` cell (selftest) needs no CLI; a pipeline cell
    // does, so refuse early with the build hint rather than fail mid-protocol.
    if cell.command.is_none() && !paths.cli.is_file() {
        return Err(BenchError(format!(
            "release CLI missing at {} — run `make release` first",
            paths.cli.display()
        )));
    }
    let samples = protocol::run_protocol(cell.warmups, cell.runs, |counted| {
        fixture.reset()?;
        let run_dir = invocation.path().join(format!("run-{run_seq}"));
        run_seq += 1;
        std::fs::create_dir_all(&run_dir).map_err(at(&run_dir))?;
        let seq = run_seq - 1;
        run_once_subprocess(cell, &subs, paths, &run_dir, seq, counted)
    })?;
    let rdlt_side = rdlt_side(&samples);
    let verify = verify_outcome(cell, &samples)?;

    let mut artifact = Artifact {
        format_version: crate::artifact::ARTIFACT_FORMAT_VERSION,
        cell_id: cell.id.clone(),
        recorded_at: crate::artifact::recorded_at(),
        fingerprint: crate::artifact::fingerprint(
            fixture.hashes.clone(),
            competitor_pins,
            quiet_note,
        ),
        workload: cell.workload.clone(),
        rdlt: rdlt_side,
        competitors,
        verify,
        forced,
        extra: serde_json::Map::new(),
    };

    // Fill competitor→rdlt ratios now that the rdlt median exists.
    let rdlt_median = artifact.rdlt.median_ms;
    for side in artifact.competitors.values_mut() {
        if let crate::artifact::CompetitorSide::Ok {
            median_ms,
            ratio_vs_rdlt,
            ..
        } = side
        {
            *ratio_vs_rdlt = Some(*median_ms / rdlt_median);
        }
    }
    Ok(artifact)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_totals_sum_across_tables() {
        let report: serde_json::Value = serde_json::json!({
            "tables": {
                "events": {"rows": 10, "bytes": 100},
                "events__tags": {"rows": 20, "bytes": 50},
            }
        });
        assert_eq!(report_totals(&report), (30, 150));
        assert_eq!(report_table_rows(&report, "events"), 10);
        assert_eq!(report_table_rows(&report, "ghost"), 0);
    }
}

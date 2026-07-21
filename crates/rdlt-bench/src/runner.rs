//! The one protocol that runs every cell (BH2): fixtures up, competitors
//! FIRST (the R12 baseline-first discipline), then rdlt — warmups, N runs,
//! stats, artifact. Gated numbers come from the release-CLI subprocess
//! (FR-011); library mode adds attribution detail for scoreboards.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::artifact::{
    Artifact, CpuStats, RdltSide, RssStats, VerifyOutcome,
};
use crate::cells::{Cell, Mode};
use crate::protocol::{self, QuietVerdict, Sample};
use crate::sample::{ResourceUsage, Sampler};
use crate::{BenchError, Result};

/// Repo-anchored paths. The harness runs from the repo root (`cargo run -p
/// rdlt-bench`) or any directory containing `benches/`.
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
            cli: dir.join("target/release/rdlt"),
            repo: dir,
            benches,
        })
    }
}

/// `{{key}}` substitution — unknown keys are left intact so a typo surfaces
/// verbatim in the failing command rather than vanishing.
pub fn substitute(template: &str, subs: &BTreeMap<String, String>) -> String {
    let mut out = template.to_owned();
    for (key, value) in subs {
        out = out.replace(&format!("{{{{{key}}}}}"), value);
    }
    out
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
}

fn run_once_subprocess(
    cell: &Cell,
    subs: &BTreeMap<String, String>,
    paths: &Paths,
    run_dir: &Path,
    counted: bool,
) -> Result<Sample<RunDetail>> {
    let mut subs = subs.clone();
    subs.insert("workdir".into(), run_dir.display().to_string());

    if let Some(prepare) = &cell.prepare_sh {
        let script = substitute(prepare, &subs);
        let status = std::process::Command::new("sh").args(["-c", &script]).status()?;
        if !status.success() {
            return Err(BenchError(format!("cell `{}`: prepare_sh failed", cell.id)));
        }
    }

    let report_path = run_dir.join("report.json");
    let spec_path = match &cell.pipeline {
        Some(template) => {
            let raw = std::fs::read_to_string(paths.benches.join(template)).map_err(|e| {
                BenchError(format!("cell `{}`: reading template {}: {e}", cell.id, template.display()))
            })?;
            let spec = run_dir.join("pipeline.yaml");
            std::fs::write(&spec, substitute(&raw, &subs))?;
            Some(spec)
        }
        None => None,
    };
    if let Some(spec) = &spec_path {
        subs.insert("spec".into(), spec.display().to_string());
    }

    let argv: Vec<String> = match &cell.command {
        Some(custom) => custom.iter().map(|a| substitute(a, &subs)).collect(),
        None => {
            let spec = spec_path.as_ref().expect("checked at load: pipeline or command");
            vec![
                paths.cli.display().to_string(),
                "run".into(),
                spec.display().to_string(),
                "--report".into(),
                report_path.display().to_string(),
            ]
        }
    };

    let started = Instant::now();
    let child = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .current_dir(run_dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| BenchError(format!("cell `{}`: spawning {}: {e}", cell.id, argv[0])))?;
    let sampler = counted.then(|| Sampler::spawn(child.id()));
    let output = child.wait_with_output()?;
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    let usage = sampler.map(Sampler::stop);

    if !output.status.success() {
        return Err(BenchError(format!(
            "cell `{}`: run failed ({}): {}",
            cell.id,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let report = report_path
        .exists()
        .then(|| std::fs::read_to_string(&report_path).ok())
        .flatten()
        .and_then(|raw| serde_json::from_str(&raw).ok());
    Ok(Sample { wall_ms, detail: RunDetail { report, usage } })
}

/// CPU stats from the (last counted run's) sampler series + wall time.
fn cpu_stats(usage: Option<&ResourceUsage>, wall_ms: f64) -> CpuStats {
    let Some(usage) = usage else {
        return CpuStats { note: Some("no sampler ran".into()), ..CpuStats::default() };
    };
    let Some(cpu_ms) = usage.cpu_ms else {
        return CpuStats { note: usage.note.clone(), ..CpuStats::default() };
    };
    let mean = (cpu_ms as f64 / wall_ms).max(0.0);
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
        Some(u) => RssStats { peak_bytes: u.peak_rss_bytes, note: u.note.clone() },
        None => RssStats { peak_bytes: None, note: Some("no sampler ran".into()) },
    }
}

/// Assemble the rdlt side from counted samples (+ the last run's detail).
pub fn rdlt_side(samples: &[Sample<RunDetail>]) -> RdltSide {
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
        cpu: cpu_stats(last.detail.usage.as_ref(), last.wall_ms),
        rss: rss_stats(last.detail.usage.as_ref()),
        streams: vec![],
        runs_ms,
    }
}

fn verify_outcome(cell: &Cell, samples: &[Sample<RunDetail>]) -> Result<Option<VerifyOutcome>> {
    let Some(verify) = &cell.verify else { return Ok(None) };
    let report = samples
        .last()
        .and_then(|s| s.detail.report.as_ref())
        .ok_or_else(|| {
            BenchError(format!("cell `{}`: verify declared but no RunReport captured", cell.id))
        })?;
    let actual = report_table_rows(report, &verify.table);
    let ok = actual == verify.expected_rows;
    if !ok {
        return Err(BenchError(format!(
            "cell `{}`: verify FAILED — table `{}` has {actual} rows, expected {}",
            cell.id, verify.table, verify.expected_rows
        )));
    }
    Ok(Some(VerifyOutcome {
        table: verify.table.clone(),
        expected_rows: verify.expected_rows,
        actual_rows: actual,
        ok,
    }))
}

/// Hyperfine mode (R8): shell the recorded protocol, parse its JSON export.
fn run_hyperfine(cell: &Cell, subs: &BTreeMap<String, String>, run_dir: &Path) -> Result<Vec<f64>> {
    let mut subs = subs.clone();
    subs.insert("workdir".into(), run_dir.display().to_string());
    let command = cell
        .command
        .as_ref()
        .ok_or_else(|| BenchError(format!("hyperfine cell `{}` needs `command`", cell.id)))?
        .iter()
        .map(|a| substitute(a, &subs))
        .collect::<Vec<_>>()
        .join(" ");
    let export = run_dir.join("hyperfine.json");
    let mut argv: Vec<String> = vec![
        "hyperfine".into(),
        "-N".into(),
        "--warmup".into(),
        cell.warmups.to_string(),
        "--runs".into(),
        cell.runs.to_string(),
        "--export-json".into(),
        export.display().to_string(),
    ];
    if let Some(prepare) = &cell.prepare_sh {
        argv.push("--prepare".into());
        argv.push(substitute(prepare, &subs));
    }
    argv.push(command);
    let status = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .current_dir(run_dir)
        .status()
        .map_err(|e| BenchError(format!("cell `{}`: hyperfine: {e} (is it installed?)", cell.id)))?;
    if !status.success() {
        return Err(BenchError(format!("cell `{}`: hyperfine failed ({status})", cell.id)));
    }
    let raw = std::fs::read_to_string(&export)?;
    let parsed: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| BenchError(format!("hyperfine export: {e}")))?;
    parsed["results"][0]["times"]
        .as_array()
        .map(|times| times.iter().filter_map(|t| t.as_f64()).map(|s| s * 1000.0).collect())
        .ok_or_else(|| BenchError("hyperfine export missing results[0].times".into()))
}

/// Run one cell end to end and return its artifact (not yet written).
pub fn run_cell(
    cell: &Cell,
    paths: &Paths,
    fixture: &crate::fixtures::Started,
    competitor_pin: Option<String>,
    competitors: BTreeMap<String, crate::artifact::CompetitorSide>,
) -> Result<Artifact> {
    let quiet_note = match protocol::quiet_guard_now(cell.class)? {
        QuietVerdict::Quiet => None,
        QuietVerdict::Annotated(note) => {
            eprintln!("[{}] {note}", cell.id);
            Some(note)
        }
    };

    let mut subs: BTreeMap<String, String> = BTreeMap::new();
    subs.insert("repo".into(), paths.repo.display().to_string());
    subs.insert("benches".into(), paths.benches.display().to_string());
    subs.insert("cli".into(), paths.cli.display().to_string());
    subs.insert("data".into(), fixture.data.path().display().to_string());
    if let Some(conn) = fixture.conn() {
        subs.insert("conn".into(), conn.to_owned());
    }
    if let Some(port) = fixture.def.port {
        subs.insert("port".into(), port.to_string());
    }

    let invocation = tempfile::tempdir().map_err(|e| BenchError(format!("tempdir: {e}")))?;
    let mut run_seq = 0u32;

    let mut rdlt = match cell.mode {
        Mode::Subprocess => {
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
                std::fs::create_dir_all(&run_dir)?;
                run_once_subprocess(cell, &subs, paths, &run_dir, counted)
            })?;
            let mut side = rdlt_side(&samples);
            let verify = verify_outcome(cell, &samples)?;
            return_side(cell, paths, fixture, competitor_pin, competitors, quiet_note, {
                side.streams = vec![];
                (side, verify)
            })
        }
        Mode::Library => {
            let samples = protocol::run_protocol(cell.warmups, cell.runs, |_counted| {
                fixture.reset()?;
                let run_dir = invocation.path().join(format!("run-{run_seq}"));
                run_seq += 1;
                std::fs::create_dir_all(&run_dir)?;
                crate::library_mode::run_once(cell, &subs, paths, &run_dir)
            })?;
            let side = crate::library_mode::side_from(&samples);
            let verify = crate::library_mode::verify_from(cell, &samples)?;
            return_side(cell, paths, fixture, competitor_pin, competitors, quiet_note, (side, verify))
        }
        Mode::Hyperfine => {
            fixture.reset()?;
            let run_dir = invocation.path().join("hyperfine");
            std::fs::create_dir_all(&run_dir)?;
            // Render the pipeline template once — hyperfine reuses it, the
            // prepare_sh wipes per-run state.
            let mut hsubs = subs.clone();
            hsubs.insert("workdir".into(), run_dir.display().to_string());
            if let Some(template) = &cell.pipeline {
                let raw = std::fs::read_to_string(paths.benches.join(template))?;
                let spec = run_dir.join("pipeline.yaml");
                std::fs::write(&spec, substitute(&raw, &hsubs))?;
                subs.insert("spec".into(), spec.display().to_string());
            }
            let runs_ms = run_hyperfine(cell, &subs, &run_dir)?;
            let side = RdltSide {
                median_ms: protocol::median(&runs_ms),
                p95_ms: protocol::p95(&runs_ms),
                rows: None,
                bytes: None,
                rows_per_s: None,
                mb_per_s: None,
                cpu: CpuStats {
                    note: Some("hyperfine protocol — wall only (R8)".into()),
                    ..CpuStats::default()
                },
                rss: RssStats { peak_bytes: None, note: Some("hyperfine protocol".into()) },
                streams: vec![],
                runs_ms,
            };
            return_side(cell, paths, fixture, competitor_pin, competitors, quiet_note, (side, None))
        }
    }?;

    // Fill competitor→rdlt ratios now that the rdlt median exists.
    let rdlt_median = rdlt.rdlt.median_ms;
    for side in rdlt.competitors.values_mut() {
        if let crate::artifact::CompetitorSide::Ok { median_ms, ratio_vs_rdlt, .. } = side {
            *ratio_vs_rdlt = Some(*median_ms / rdlt_median);
        }
    }
    Ok(rdlt)
}

#[allow(clippy::too_many_arguments)]
fn return_side(
    cell: &Cell,
    _paths: &Paths,
    fixture: &crate::fixtures::Started,
    competitor_pin: Option<String>,
    competitors: BTreeMap<String, crate::artifact::CompetitorSide>,
    quiet_note: Option<String>,
    (rdlt, verify): (RdltSide, Option<VerifyOutcome>),
) -> Result<Artifact> {
    Ok(Artifact {
        format_version: crate::artifact::ARTIFACT_FORMAT_VERSION,
        cell_id: cell.id.clone(),
        class: cell.class,
        mode: cell.mode,
        suite: cell.suite.clone(),
        recorded_at: crate::artifact::recorded_at(),
        fingerprint: crate::artifact::fingerprint(
            fixture.hashes.clone(),
            competitor_pin,
            quiet_note,
        ),
        workload: cell.workload.clone(),
        rdlt,
        competitors,
        verify,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitution_replaces_known_and_keeps_unknown() {
        let mut subs = BTreeMap::new();
        subs.insert("conn".to_owned(), "pg://x".to_owned());
        assert_eq!(substitute("a {{conn}} b {{typo}}", &subs), "a pg://x b {{typo}}");
    }

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

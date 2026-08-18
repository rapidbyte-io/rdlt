//! The product arm: how the harness measures rdlt for one cell. The render
//! vocabulary and its substitutions, the connector-binary preflight, one
//! measured run (release-CLI subprocess under the resource sampler), and the
//! fold of N counted runs into the artifact's rdlt side plus the verified
//! table set. Every measured number comes from the release-CLI subprocess.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::artifact::{self, CpuStats, RdltSide, RssStats};
use crate::cell::Cell;
use crate::error::{Error, Result};
use crate::fixture;
use crate::measure::{self, Measured};
use crate::paths::Paths;
use crate::sample::{ResourceUsage, Sampler};
use crate::template::substitute;

/// Every `{{key}}` a cell's PIPELINE template may reference — the ONE place
/// the render vocabulary is written down. The pipeline suite renders the
/// cell templates from THIS slice rather than a second hand-written list,
/// and `put` refuses any key not named here, so the two cannot drift in
/// either direction: a key renamed here without a stand-in in the suite
/// fails the suite, and a substitution added without naming it here fails
/// at the first cell that runs.
///
/// `spec` is deliberately ABSENT: it is inserted by `render_spec` AFTER the
/// pipeline template has been rendered, so only a `command` line can see it.
pub const SUBSTITUTION_KEYS: &[&str] = &[
    "benches", "bins", "cli", "conn", "data", "port", "repo", "run", "workdir",
];

/// Insert one render substitution, refusing a key [`SUBSTITUTION_KEYS`] does
/// not name.
fn put(subs: &mut BTreeMap<String, String>, key: &str, value: String) {
    assert!(
        SUBSTITUTION_KEYS.contains(&key),
        "`{key}` is not in product::SUBSTITUTION_KEYS — add it there \
         (the pipeline suite renders templates from that slice, and a key \
         missing from it is a key no test can see)"
    );
    subs.insert(key.to_owned(), value);
}

/// The cell's substitution map: the repo layout, the release CLI, the
/// connector-binary directory, and the primary fixture's `data`/`conn`/
/// `port`. Shared by the product side and every competitor arm of the cell.
pub fn substitutions(paths: &Paths, primary: &fixture::Live) -> BTreeMap<String, String> {
    let mut subs = BTreeMap::new();
    put(&mut subs, "repo", paths.repo.display().to_string());
    put(&mut subs, "benches", paths.benches.display().to_string());
    put(&mut subs, "cli", paths.cli.display().to_string());
    // Release unconditionally, like `cli`: an absent release bin fails LOUD
    // at spawn, whereas a debug fallback would measure an unoptimized
    // connector silently.
    put(&mut subs, "bins", paths.bins.display().to_string());
    put(
        &mut subs,
        "data",
        primary.data_dir.path().display().to_string(),
    );
    if let Some(conn) = primary.conn() {
        put(&mut subs, "conn", conn.to_owned());
    }
    if let Some(port) = primary.def.port {
        put(&mut subs, "port", port.to_string());
    }
    subs
}

/// Rows/bytes totals a run-report JSON attributes to its tables.
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

/// Detail attached to each counted run: the CLI's run report (absent for a
/// `command` cell) and the sampler's reading.
#[derive(Debug, Default)]
pub(crate) struct Detail {
    pub(crate) report: Option<serde_json::Value>,
    pub(crate) usage: Option<ResourceUsage>,
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
        Error(format!(
            "cell `{}`: reading template {}: {e}",
            cell.id,
            template.display()
        ))
    })?;
    let spec = run_dir.join("pipeline.yaml");
    std::fs::write(&spec, substitute(&raw, subs)).map_err(|e| Error::io(&spec, e))?;
    subs.insert("spec".into(), spec.display().to_string());
    Ok(Some(spec))
}

/// The connector binaries a pipeline template names through literal
/// `{{bins}}/<name>` paths, read straight from the unrendered text. A
/// template that never mentions `{{bins}}` (an id-only `connector:` arm
/// resolves its bin off PATH instead) yields an empty set, and so does a
/// bare `{{bins}}` with no `/<name>` after it: there is no binary to name.
fn required_connector_bins(template: &str) -> BTreeSet<&str> {
    let mut names = BTreeSet::new();
    for tail in template.split("{{bins}}/").skip(1) {
        // A path component ends at whitespace or a YAML quote.
        let name = tail
            .split(|c: char| c.is_whitespace() || c == '"' || c == '\'')
            .next()
            .unwrap_or("");
        if !name.is_empty() {
            names.insert(name);
        }
    }
    names
}

/// The conventional connector binaries named by the pipeline's source and
/// destination `connector:` arms — the last segment of each `id`, for the
/// arms that carry no `path:` override.
fn declared_connector_bins(template: &str) -> Result<BTreeSet<String>> {
    let mut rendered = template.to_owned();
    for key in SUBSTITUTION_KEYS {
        rendered = rendered.replace(&format!("{{{{{key}}}}}"), "rdlt-placeholder");
    }
    let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(&rendered)
        .map_err(|error| Error(format!("parsing pipeline template: {error}")))?;
    let mut bins = BTreeSet::new();
    for role in ["source", "destination"] {
        let Some(declaration) = document
            .get(role)
            .and_then(serde_yaml_ng::Value::as_mapping)
        else {
            continue;
        };
        let connector = serde_yaml_ng::Value::String("connector".to_owned());
        let Some(explicit) = declaration
            .get(&connector)
            .and_then(serde_yaml_ng::Value::as_mapping)
        else {
            continue;
        };
        // An arm carrying `path:` names its OWN binary — possibly one no
        // conventional build produces — so demanding the conventional
        // `{{bins}}` name would refuse a cell no provisioning step can
        // satisfy; the spawn diagnoses that path itself. The override is
        // any STRING, exactly as the document model reads it: a null
        // `path:` parses as None there (PATH discovery — provision), while
        // `path: ""` is a real override the runtime will try to exec.
        let path = serde_yaml_ng::Value::String("path".to_owned());
        if explicit
            .get(&path)
            .and_then(serde_yaml_ng::Value::as_str)
            .is_some()
        {
            continue;
        }
        let id = serde_yaml_ng::Value::String("id".to_owned());
        let segment = explicit
            .get(&id)
            .and_then(serde_yaml_ng::Value::as_str)
            .and_then(|value| value.rsplit('.').next());
        if let Some(segment) = segment.filter(|segment| !segment.is_empty()) {
            bins.insert(format!("rdlt-connector-{segment}"));
        }
    }
    Ok(bins)
}

/// Everything a cell needs on disk before anything is built, seeded or
/// measured: the release CLI and every connector binary its pipeline names
/// through a literal path or an id-only `connector:` arm. Depends only
/// on the cell and paths, so the matrix calls it before any fixture comes
/// up or competitor baseline runs; `run` calls it again because that entry
/// point is also used directly, at the cost of a few metadata checks.
pub(crate) fn preconditions(cell: &Cell, paths: &Paths) -> Result<()> {
    // A `command` cell (selftest) needs no CLI; a pipeline cell does, so
    // refuse with the build hint rather than fail mid-protocol.
    if cell.command.is_none() && !paths.cli.is_file() {
        return Err(Error(format!(
            "release CLI missing at {} — run `make release` first",
            paths.cli.display()
        )));
    }
    // The literal token check preserves path-specific diagnostics; the
    // declaration check covers id-only arms before PATH resolution can
    // reach a host-installed binary.
    if let Some(template) = &cell.pipeline {
        let path = paths.benches.join(template);
        let raw = std::fs::read_to_string(&path).map_err(|e| {
            Error(format!(
                "cell `{}`: reading template {}: {e}",
                cell.id,
                path.display()
            ))
        })?;
        for name in required_connector_bins(&raw) {
            let bin = paths.bins.join(name);
            if !bin.is_file() {
                return Err(Error(format!(
                    "cell `{}`: connector binary missing at {} — the template names it \
                     as `{{{{bins}}}}/{name}`; the first-party connector bins are built \
                     in the sibling rdlt-connectors repository (`make connector-bins` \
                     there builds them release — a debug fallback would measure an \
                     unoptimized connector); copy or link the bin into this \
                     workspace's target/release",
                    cell.id,
                    bin.display()
                )));
            }
        }
        let declared = declared_connector_bins(&raw).map_err(|error| {
            Error(format!(
                "cell `{}`: reading connector declarations from {}: {error}",
                cell.id,
                path.display()
            ))
        })?;
        for name in declared {
            let bin = paths.bins.join(&name);
            if !bin.is_file() {
                return Err(Error(format!(
                    "cell `{}`: connector binary missing at {} — pipeline {} requires \
                     `{name}`; build it in the sibling rdlt-connectors repository \
                     (`make connector-bins` there) and copy or link it into this \
                     workspace's target/release before benchmarking",
                    cell.id,
                    bin.display(),
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

/// The PATH the measured CLI (and any prepare-run CLI) sees: the
/// connector-binary directory prepended to the harness's own PATH. An
/// id-only `connector:` arm carries no `path:` override — the CLI resolves
/// its id by a PATH walk for `rdlt-connector-<segment>` — so the harness
/// points that walk at the same `<target>/release` directory `{{bins}}`
/// names, instead of at whatever connectors the operator has installed.
fn path_with_bins(paths: &Paths) -> std::ffi::OsString {
    let mut entries = vec![paths.bins.clone()];
    if let Some(path) = std::env::var_os("PATH") {
        entries.extend(std::env::split_paths(&path));
    }
    std::env::join_paths(entries).expect("paths.bins joins the process's own PATH entries")
}

/// Run the cell's untimed `prepare_sh` (seed refresh, state wipe) if declared.
fn run_prepare(cell: &Cell, subs: &BTreeMap<String, String>, paths: &Paths) -> Result<()> {
    let Some(prepare) = &cell.prepare_sh else {
        return Ok(());
    };
    let script = substitute(prepare, subs);
    let status = std::process::Command::new("sh")
        .args(["-c", &script])
        .env("PATH", path_with_bins(paths))
        .status()?;
    if !status.success() {
        return Err(Error(format!("cell `{}`: prepare_sh failed", cell.id)));
    }
    Ok(())
}

/// Measure what an arm left behind, by running its declared shell line and
/// reading a byte count off stdout. Deliberately untimed and AFTER the runs:
/// it must not appear in any timing, and it reads the output of the LAST
/// run, which is the one still on disk. A failure is recorded as absent —
/// never as zero and never as a run failure: the size is context for a
/// comparison, not part of it.
pub(crate) fn measure_artifact_bytes(
    label: &str,
    script: Option<&String>,
    subs: &BTreeMap<String, String>,
) -> Option<u64> {
    let script = substitute(script?, subs);
    let output = match std::process::Command::new("sh")
        .args(["-c", &script])
        .output()
    {
        Ok(output) => output,
        Err(e) => {
            // An unspawnable `sh` and a cell that declared no sizer both
            // produce an absent size; without this line they are
            // indistinguishable in the artifact.
            eprintln!("  {label}: artifact_bytes_sh could not run ({e}) — recording absent");
            return None;
        }
    };
    if !output.status.success() {
        eprintln!(
            "  {label}: artifact_bytes_sh failed ({}) — recording absent, not zero",
            output.status
        );
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    match text.trim().parse::<u64>() {
        Ok(bytes) => Some(bytes),
        Err(_) => {
            eprintln!(
                "  {label}: artifact_bytes_sh printed {:?}, which is not a byte count — \
                 recording absent",
                text.trim()
            );
            None
        }
    }
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

/// One run of the measured subprocess in its own `run_dir`: render the spec,
/// run prepare, spawn the argv under the sampler (counted runs only), time it
/// on the harness wall clock, and read the run report it left behind.
fn run_once(
    cell: &Cell,
    subs: &BTreeMap<String, String>,
    paths: &Paths,
    run_dir: &Path,
    seq: u32,
    counted: bool,
) -> Result<Measured<Detail>> {
    let mut subs = subs.clone();
    put(&mut subs, "workdir", run_dir.display().to_string());
    // Per-run sequence for prepare scripts that need run-unique mutations
    // (the merge-strategy 50%-changed regime).
    put(&mut subs, "run", seq.to_string());

    let report_path = run_dir.join("report.json");
    let spec_path = render_spec(cell, &mut subs, paths, run_dir)?;
    run_prepare(cell, &subs, paths)?;
    let argv = measured_argv(cell, &subs, paths, spec_path.as_ref(), &report_path);

    let started = Instant::now();
    let child = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .env("PATH", path_with_bins(paths))
        .current_dir(run_dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| Error(format!("cell `{}`: spawning {}: {e}", cell.id, argv[0])))?;
    let sampler = counted.then(|| Sampler::spawn(child.id()));
    let output = child.wait_with_output()?;
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    let usage = sampler.map(Sampler::stop);

    if !output.status.success() {
        return Err(Error(format!(
            "cell `{}`: run failed ({}): {}",
            cell.id,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    // "Absent" must mean genuinely absent: a corrupt or unreadable report is
    // the real failure, not a cell that produced no report at all.
    let report = if report_path.exists() {
        let raw = std::fs::read_to_string(&report_path).map_err(|e| {
            Error(format!(
                "cell `{}`: reading the run report at {}: {e}",
                cell.id,
                report_path.display()
            ))
        })?;
        Some(serde_json::from_str(&raw).map_err(|e| {
            Error(format!(
                "cell `{}`: parsing the run report at {}: {e}",
                cell.id,
                report_path.display()
            ))
        })?)
    } else {
        None
    };
    Ok(Measured {
        wall_ms,
        detail: Detail { report, usage },
    })
}

/// CPU stats from the (last counted run's) sampler series. The denominator
/// is the sampler window — the run's wall clock.
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

/// Fold the counted runs (+ the last run's detail) into the artifact's rdlt
/// side.
fn fold(samples: &[Measured<Detail>]) -> RdltSide {
    let runs_ms: Vec<f64> = samples.iter().map(|s| s.wall_ms).collect();
    let median_ms = measure::median(&runs_ms);
    let p95_ms = measure::p95(&runs_ms);
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
        artifact_bytes: None, // filled by `run`, which knows the cell's sizer
        rows_per_s: rows.map(|r| r as f64 / secs),
        mb_per_s: bytes.map(|b| b as f64 / (1024.0 * 1024.0) / secs),
        cpu: cpu_stats(last.detail.usage.as_ref(), last.wall_ms),
        rss: rss_stats(last.detail.usage.as_ref()),
        runs_ms,
    }
}

/// The delivered set must MATCH the declared set, not merely contain it:
/// checking only the declared tables' row counts leaves a run free to move
/// extra streams the cell never claimed, which makes its timing incomparable
/// with a competitor arm that moved only the declared set while every row
/// count still passes.
fn verify(cell: &Cell, samples: &[Measured<Detail>]) -> Result<Option<artifact::Verify>> {
    let Some(verify) = &cell.verify else {
        return Ok(None);
    };
    let report = samples
        .last()
        .and_then(|s| s.detail.report.as_ref())
        .ok_or_else(|| {
            Error(format!(
                "cell `{}`: verify declared but no run report captured",
                cell.id
            ))
        })?;
    let delivered: BTreeSet<&str> = report
        .get("tables")
        .and_then(serde_json::Value::as_object)
        .map(|t| t.keys().map(String::as_str).collect())
        .unwrap_or_default();
    let declared: BTreeSet<&str> = verify.keys().map(String::as_str).collect();
    let surplus: Vec<&str> = delivered.difference(&declared).copied().collect();
    let missing: Vec<&str> = declared.difference(&delivered).copied().collect();
    if !surplus.is_empty() || !missing.is_empty() {
        let mut detail = Vec::new();
        if !surplus.is_empty() {
            detail.push(format!(
                "delivered but not declared: {}",
                surplus.join(", ")
            ));
        }
        if !missing.is_empty() {
            detail.push(format!(
                "declared but not delivered: {}",
                missing.join(", ")
            ));
        }
        return Err(Error(format!(
            "cell `{}`: verify FAILED — the run's tables do not match the cell's \
             declared set ({}); a cell that moves undeclared tables is not comparable \
             with its competitor arms",
            cell.id,
            detail.join("; ")
        )));
    }
    let mut outcome = artifact::Verify::new();
    for (table, expected) in verify {
        let actual = report_table_rows(report, table);
        if actual != *expected {
            return Err(Error(format!(
                "cell `{}`: verify FAILED — table `{table}` has {actual} rows, expected {expected}",
                cell.id
            )));
        }
        outcome.insert(table.clone(), actual);
    }
    Ok(Some(outcome))
}

/// The product side of one cell: the rdlt record and the verified table set.
#[derive(Debug)]
pub struct Measurement {
    pub rdlt: RdltSide,
    pub verify: Option<artifact::Verify>,
}

/// Measure the product for one cell: preconditions, then warmups and N
/// counted runs — every fixture reset before each, primary first — folded
/// into the rdlt side, its output sized, its delivered tables verified.
pub fn run(
    cell: &Cell,
    paths: &Paths,
    fixtures: &[&fixture::Live],
    subs: &BTreeMap<String, String>,
) -> Result<Measurement> {
    let invocation = tempfile::tempdir().map_err(|e| Error(format!("tempdir: {e}")))?;
    let mut run_seq = 0u32;

    preconditions(cell, paths)?;
    let samples = measure::run(cell.warmups, cell.runs, |counted| {
        for fixture in fixtures {
            fixture.reset()?;
        }
        let run_dir = invocation.path().join(format!("run-{run_seq}"));
        run_seq += 1;
        std::fs::create_dir_all(&run_dir).map_err(|e| Error::io(&run_dir, e))?;
        let seq = run_seq - 1;
        run_once(cell, subs, paths, &run_dir, seq, counted)
    })?;
    let mut rdlt = fold(&samples);
    rdlt.artifact_bytes = measure_artifact_bytes("rdlt", cell.artifact_bytes_sh.as_ref(), subs);
    let verify = verify(cell, &samples)?;
    Ok(Measurement { rdlt, verify })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Literal paths and pipeline declarations independently identify
    /// the connector binaries a cell requires.
    #[test]
    fn connector_bins_are_read_from_paths_and_declarations() {
        let remote = "source:\n  connector:\n    path: {{bins}}/rdlt-connector-postgres\n\
                      destination:\n  connector:\n    path: \"{{bins}}/rdlt-connector-file\"\n";
        assert_eq!(
            required_connector_bins(remote)
                .into_iter()
                .collect::<Vec<_>>(),
            ["rdlt-connector-file", "rdlt-connector-postgres"]
        );
        // Repeats collapse; the same bin on both sides is one check.
        let twice = "a {{bins}}/rdlt-connector-postgres b {{bins}}/rdlt-connector-postgres";
        assert_eq!(required_connector_bins(twice).len(), 1);
        // An ordinary cell's template, and a bare `{{bins}}` with no
        // name after it, both demand nothing.
        assert!(
            required_connector_bins(
                "source:\n  connector:\n    id: io.rapidbyte.postgres\n    config: {conn: {{conn}}}\n"
            )
            .is_empty()
        );
        assert!(required_connector_bins("dir: {{bins}}\n").is_empty());

        // Only the `connector:` arm is read: an arm in any other shape
        // declares nothing here (the CLI refuses it at parse anyway).
        let other = "source:\n  oracle: {{data}}/oracle.yaml\n\
                     destination:\n  postgres:\n    conn: '{{conn}}'\n";
        assert!(
            declared_connector_bins(other)
                .expect("the template is well-formed YAML")
                .is_empty()
        );
        let explicit = "source:\n  connector:\n    id: io.rapidbyte.file\n\
                        destination:\n  connector:\n    id: io.rapidbyte.postgres\n";
        assert_eq!(
            declared_connector_bins(explicit).expect("the connector template parses"),
            BTreeSet::from([
                "rdlt-connector-file".to_owned(),
                "rdlt-connector-postgres".to_owned(),
            ])
        );
        // A `path:` override names its OWN binary — the conventional
        // workspace name is NOT demanded for it (an out-of-tree bin
        // would otherwise make the cell permanently unrunnable), while
        // the other side's id-only declaration still is.
        let overridden = "source:\n  connector:\n    id: com.example.foo\n    \
                          path: /opt/foo/bin/foo-connector\n\
                          destination:\n  connector:\n    id: io.rapidbyte.postgres\n";
        assert_eq!(
            declared_connector_bins(overridden).expect("the overridden template parses"),
            BTreeSet::from(["rdlt-connector-postgres".to_owned()])
        );
        // A NULL `path:` is not an override: the document model reads it
        // as None and falls back to PATH discovery, so preflight must
        // still provision the conventional bin — while `path: ""` IS
        // an override there (a string the runtime will try to exec),
        // so preflight must not demand a bin the run would never use.
        // Both templates are parsed through the REAL document model below,
        // pinning the agreement itself rather than one reader's half.
        let null_path = "source:\n  connector:\n    id: io.rapidbyte.postgres\n    path:\n    \
                         config: {}\n\
                         destination:\n  connector:\n    id: io.rapidbyte.file\n    config: {}\n";
        assert_eq!(
            declared_connector_bins(null_path).expect("the null-path template parses"),
            BTreeSet::from([
                "rdlt-connector-file".to_owned(),
                "rdlt-connector-postgres".to_owned(),
            ])
        );
        let empty_path = null_path.replace("path:\n", "path: \"\"\n");
        assert_eq!(
            declared_connector_bins(&empty_path).expect("the empty-path template parses"),
            BTreeSet::from(["rdlt-connector-file".to_owned()])
        );
        for (template, expected_path) in [(null_path, None), (empty_path.as_str(), Some(""))] {
            let spec: rdlt::document::Document =
                serde_yaml_ng::from_str(&format!("pipeline: p\n{template}"))
                    .expect("the template parses through the real document model");
            assert_eq!(
                spec.source.path.as_deref(),
                expected_path.map(std::path::Path::new),
                "the document model's reading this rule mirrors"
            );
        }
    }

    /// An id-only `connector:` pipeline refuses a missing release
    /// connector before the CLI can resolve a host-installed binary from
    /// PATH.
    #[test]
    fn id_only_pipeline_refuses_a_missing_release_connector() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = Paths::rooted(root.path().to_owned(), root.path().join("target"));
        let pipeline = paths.benches.join("cells/pipelines/id-only.yaml");
        std::fs::create_dir_all(pipeline.parent().expect("pipeline parent"))
            .expect("the pipeline directory creates");
        std::fs::create_dir_all(&paths.bins).expect("the release directory creates");
        std::fs::write(
            &pipeline,
            "source:\n  connector: {id: io.rapidbyte.oracle, config: config.yaml}\n\
             destination:\n  connector: {id: io.rapidbyte.postgres, config: {}}\n",
        )
        .expect("the id-only pipeline writes");
        std::fs::write(&paths.cli, "").expect("the release CLI marker writes");
        let file: toml::Value = toml::from_str(
            "[[cell]]\nid='id-only'\nfixtures=['oracle']\npipeline='cells/pipelines/id-only.yaml'\n\
             [cell.verify]\nevents=1\n",
        )
        .expect("the cell TOML parses");
        let cell: Cell = file["cell"][0]
            .clone()
            .try_into()
            .expect("the cell deserializes");

        let error = preconditions(&cell, &paths)
            .expect_err("a missing id-only connector must refuse")
            .to_string();
        assert!(
            error.contains(
                paths
                    .bins
                    .join("rdlt-connector-oracle")
                    .to_str()
                    .expect("utf-8 path")
            ),
            "the refusal names the missing binary: {error}"
        );
        assert!(
            error.contains("rdlt-connectors") && error.contains("make connector-bins"),
            "the refusal points at the connectors repository's build verb: {error}"
        );
    }

    /// The `put` guard is live, not decorative: a key outside
    /// [`SUBSTITUTION_KEYS`] refuses. Without this the whole
    /// single-source-of-truth could rot into a slice nothing consults.
    #[test]
    #[should_panic(expected = "SUBSTITUTION_KEYS")]
    fn put_refuses_a_key_the_shared_slice_does_not_name() {
        let mut subs = BTreeMap::new();
        put(&mut subs, "binaries", "/t/release".into());
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

    /// Build a cell whose only interesting property is its declared table set,
    /// paired with one sample carrying a run report of the delivered set.
    fn cell_and_sample(
        declared: &[(&str, u64)],
        delivered: &[(&str, u64)],
    ) -> (Cell, Vec<Measured<Detail>>) {
        let toml = format!(
            "[[cell]]\nid='c'\nfixtures=['f']\npipeline='p'\n[cell.verify]\n{}\n",
            declared
                .iter()
                .map(|(t, r)| format!("{t} = {r}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        let file: toml::Value = toml::from_str(&toml).unwrap();
        let cell: Cell = file["cell"][0].clone().try_into().unwrap();
        let tables: serde_json::Map<String, serde_json::Value> = delivered
            .iter()
            .map(|(t, r)| ((*t).to_owned(), serde_json::json!({"rows": r, "bytes": 1})))
            .collect();
        let sample = Measured {
            wall_ms: 1.0,
            detail: Detail {
                report: Some(serde_json::json!({ "tables": tables })),
                usage: None,
            },
        };
        (cell, vec![sample])
    }

    /// The defect this check exists for: a cell verified one table's row
    /// count while the run also delivered two others, so its timing covered
    /// three times the rows its competitor arm moved and every recorded
    /// count still passed.
    #[test]
    fn a_run_delivering_undeclared_tables_fails_naming_them() {
        let (cell, samples) = cell_and_sample(
            &[("events_merged", 1_000_000)],
            &[
                ("events_merged", 1_000_000),
                ("events", 1_000_000),
                ("events_v2", 1_000_000),
            ],
        );
        let err = verify(&cell, &samples)
            .expect_err("undeclared tables make the arm incomparable")
            .to_string();
        assert!(err.contains("delivered but not declared"), "{err}");
        assert!(err.contains("events") && err.contains("events_v2"), "{err}");
    }

    #[test]
    fn a_declared_table_the_run_never_landed_fails_too() {
        let (cell, samples) = cell_and_sample(
            &[("events", 200_000), ("events__tags", 400_000)],
            &[("events", 200_000)],
        );
        let err = verify(&cell, &samples).unwrap_err().to_string();
        assert!(err.contains("declared but not delivered"), "{err}");
        assert!(err.contains("events__tags"), "{err}");
    }

    #[test]
    fn a_matching_set_records_every_declared_table() {
        let (cell, samples) = cell_and_sample(
            &[("events", 200_000), ("events__tags", 400_000)],
            &[("events", 200_000), ("events__tags", 400_000)],
        );
        let outcome = verify(&cell, &samples).unwrap().unwrap();
        assert_eq!(outcome["events"], 200_000);
        assert_eq!(outcome["events__tags"], 400_000);
    }

    #[test]
    fn a_matching_set_with_a_wrong_count_still_fails() {
        let (cell, samples) = cell_and_sample(&[("events", 200_000)], &[("events", 199_999)]);
        let err = verify(&cell, &samples).unwrap_err().to_string();
        assert!(err.contains("199999") && err.contains("200000"), "{err}");
    }
}

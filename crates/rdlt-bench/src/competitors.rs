//! Competitors as a first-class module. dlt is the
//! first: a pinned container image, a variant registry, and same-metric
//! reporting. Wall time stays the baseline's in-process SELF-timing —
//! continuity with every recorded multiple; CPU/peak-RSS come from the
//! container's cgroup v2, polled while it runs. Missing baselines are LOUD.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::artifact::{CompetitorSide, CpuStats, RssStats};
use crate::cells::CompetitorRef;
use crate::protocol;
use crate::{BenchError, Result};

/// How a variant's arms execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VariantKind {
    /// A pinned container image whose entrypoint self-times and prints the
    /// summary JSON line (the dlt shape).
    #[default]
    SelfTimedContainer,
    /// A host-side `driver.py` in the variant's module directory: it drives an
    /// external system (e.g. an Airbyte cluster), times the work itself, and
    /// prints the SAME summary JSON line — zero artifact divergence.
    Driver,
}

/// A resolved competitor variant: the pin every artifact fingerprint records,
/// plus how to execute it. `pin`/`image` may come from the file's `[defaults]`
/// table (all dlt variants share one pinned image), a per-variant value
/// overriding it.
#[derive(Debug, Clone)]
pub struct Variant {
    pub id: String,
    /// e.g. "dlt 1.29.0" — recorded in every artifact fingerprint.
    pub pin: String,
    pub kind: VariantKind,
    /// Container image (self_timed_container kind only).
    pub image: Option<String>,
    /// Driver script path, resolved relative to the module directory
    /// (driver kind only).
    pub driver: Option<std::path::PathBuf>,
    /// Machine-prerequisite probe, run with `sh -c` in the module directory
    /// before any driver run; non-zero exit ⇒ the arm records
    /// `Missing{reason}` (loud skip, never an error).
    pub prerequisite_sh: Option<String>,
    /// Per-variant run-count override (a cell's competitor entry still wins).
    pub runs: Option<u32>,
    /// The `benches/competitors/<module>/` directory the variant came from —
    /// drivers execute with this as their working directory.
    pub module_dir: std::path::PathBuf,
}

/// Shared defaults every variant inherits unless it states its own.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct VariantDefaults {
    pin: Option<String>,
    image: Option<String>,
}

/// One `[[variant]]` as written: `pin`/`image` are optional here and fall back
/// to `[defaults]`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVariant {
    id: String,
    pin: Option<String>,
    image: Option<String>,
    #[serde(default)]
    kind: VariantKind,
    driver: Option<String>,
    prerequisite_sh: Option<String>,
    runs: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VariantsFile {
    #[serde(default)]
    defaults: VariantDefaults,
    #[serde(default, rename = "variant")]
    variants: Vec<RawVariant>,
}

pub fn load_variants(path: &Path) -> Result<Vec<Variant>> {
    fn resolve(
        value: Option<String>,
        default: &Option<String>,
        id: &str,
        name: &str,
    ) -> Result<String> {
        value.or_else(|| default.clone()).ok_or_else(|| {
            BenchError(format!(
                "variant `{id}`: no `{name}` and none in [defaults]"
            ))
        })
    }
    let module_dir = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| ".".into());
    let file: VariantsFile = crate::load_toml(path)?;
    file.variants
        .into_iter()
        .map(|raw| {
            let pin = resolve(raw.pin, &file.defaults.pin, &raw.id, "pin")?;
            let (image, driver) = match raw.kind {
                VariantKind::SelfTimedContainer => (
                    Some(resolve(raw.image, &file.defaults.image, &raw.id, "image")?),
                    None,
                ),
                VariantKind::Driver => {
                    let driver = raw.driver.ok_or_else(|| {
                        BenchError(format!("variant `{}`: kind=driver needs `driver`", raw.id))
                    })?;
                    (None, Some(module_dir.join(driver)))
                }
            };
            Ok(Variant {
                pin,
                kind: raw.kind,
                image,
                driver,
                prerequisite_sh: raw.prerequisite_sh,
                runs: raw.runs,
                module_dir: module_dir.clone(),
                id: raw.id,
            })
        })
        .collect()
}

/// Discover every module's variants into ONE flat namespace:
/// `benches/competitors/*/variants.toml`, deterministic (sorted) module
/// order, duplicate variant id = load-time error naming both files.
pub fn discover_variants(competitors_dir: &Path) -> Result<Vec<Variant>> {
    let mut files: Vec<std::path::PathBuf> = match std::fs::read_dir(competitors_dir) {
        Ok(entries) => entries
            .filter_map(|e| Some(e.ok()?.path().join("variants.toml")))
            .filter(|p| p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    let mut all: Vec<Variant> = Vec::new();
    let mut sources: BTreeMap<String, std::path::PathBuf> = BTreeMap::new();
    for file in files {
        for variant in load_variants(&file)? {
            if let Some(first) = sources.get(&variant.id) {
                return Err(BenchError(format!(
                    "duplicate variant id `{}`: declared in both {} and {} (variant ids are one flat namespace)",
                    variant.id,
                    first.display(),
                    file.display(),
                )));
            }
            sources.insert(variant.id.clone(), file.clone());
            all.push(variant);
        }
    }
    Ok(all)
}

struct DriverRun {
    seconds: f64,
    peak_rss: Option<u64>,
    extra: Option<serde_json::Value>,
}

/// The venv convention: a module ships `.venv/` next to its driver (created
/// by its setup instructions); without one the driver runs on the system
/// `python3`.
fn driver_python(module_dir: &Path) -> std::path::PathBuf {
    let venv = module_dir.join(".venv/bin/python");
    if venv.is_file() {
        venv
    } else {
        "python3".into()
    }
}

fn run_driver_once(
    variant: &Variant,
    reference: &CompetitorRef,
    subs: &BTreeMap<String, String>,
) -> Result<DriverRun> {
    let driver = variant.driver.as_ref().expect("driver kind has a driver");
    let mut cmd = Command::new(driver_python(&variant.module_dir));
    cmd.arg(driver).current_dir(&variant.module_dir);
    cmd.args(
        reference
            .args
            .iter()
            .map(|a| crate::template::substitute(a, subs)),
    );
    let out = cmd.output()?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        return Err(BenchError(format!(
            "driver {} ({}) failed: {}{}",
            variant.id,
            driver.display(),
            stdout,
            String::from_utf8_lossy(&out.stderr)
        )));
    }
    let summary = protocol::last_json(&stdout).ok_or_else(|| {
        BenchError(format!(
            "driver {}: no summary JSON line on stdout: {stdout}",
            variant.id
        ))
    })?;
    let seconds = summary
        .get("seconds")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| {
            BenchError(format!(
                "driver {}: summary line has no `seconds`: {summary}",
                variant.id
            ))
        })?;
    Ok(DriverRun {
        seconds,
        peak_rss: summary
            .get("peak_rss_kb")
            .and_then(|v| v.as_u64())
            .map(|kb| kb * 1024),
        extra: summary.get("extra").cloned(),
    })
}

fn image_exists(engine: &str, image: &str) -> bool {
    Command::new(engine)
        .args(["image", "exists", image])
        .status()
        .is_ok_and(|s| s.success())
        // docker has no `image exists`; inspect works on both as fallback
        || Command::new(engine)
            .args(["image", "inspect", image])
            .output()
            .is_ok_and(|o| o.status.success())
}

#[derive(Debug, Default, Clone)]
struct CgroupReading {
    memory_peak: Option<u64>,
    cpu_usec: Option<u64>,
}

/// Read the container's cgroup v2 accounting from INSIDE it (`podman exec`):
/// with a private cgroup namespace the container sees its own controllers at
/// `/sys/fs/cgroup`, which works regardless of where the harness itself runs
/// (host paths are unreachable from e.g. a distrobox shell).
///
/// ONLY `memory.peak` (a kernel high-water mark) is accepted as a peak;
/// there is deliberately NO `memory.current` fallback — an instantaneous
/// reading labeled "peak" would be a fabricated metric.
fn read_cgroup_via_exec(engine: &str, name: &str) -> CgroupReading {
    let out = Command::new(engine)
        .args([
            "exec",
            name,
            "sh",
            "-c",
            "cat /sys/fs/cgroup/memory.peak 2>/dev/null; cat /sys/fs/cgroup/cpu.stat",
        ])
        .output();
    let Ok(out) = out else {
        return CgroupReading::default();
    };
    if !out.status.success() {
        return CgroupReading::default();
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut reading = CgroupReading::default();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        match (parts.next(), parts.next()) {
            (Some("usage_usec"), Some(v)) => reading.cpu_usec = v.parse().ok(),
            (Some(first), None) if reading.memory_peak.is_none() => {
                reading.memory_peak = first.parse().ok();
            }
            _ => {}
        }
    }
    reading
}

/// The PRIMARY RSS statistic: the baseline scripts self-report `peak_rss_kb`
/// (getrusage ru_maxrss) on the same JSON line — the statistic every recorded
/// dlt RSS multiple and the 1/5 bar derivation used. cgroup `memory.peak` is
/// the fallback only, and is labeled as the different statistic it is (it
/// additionally charges page cache, so it is not comparable to ru_maxrss).
fn self_reported_rss(stdout: &str) -> Option<u64> {
    protocol::last_json_field(stdout, "peak_rss_kb")
        .and_then(|v| v.as_u64())
        .map(|kb| kb * 1024)
}

/// The baseline scripts' convention: one JSON line on stdout whose `seconds`
/// field is the in-process self-timed measurement.
fn self_timed_seconds(stdout: &str) -> Option<f64> {
    protocol::last_json_field(stdout, "seconds").and_then(|v| v.as_f64())
}

struct ContainerRun {
    seconds: f64,
    peak_rss: Option<u64>,
    rss_source: &'static str,
    cpu_ms: Option<u64>,
    wall_ms: f64,
}

fn run_container_once(
    engine: &str,
    variant: &Variant,
    reference: &CompetitorRef,
    subs: &BTreeMap<String, String>,
    seq: u32,
) -> Result<ContainerRun> {
    let name = format!("rdlt-bench-comp-{}-{seq}", variant.id);
    let _ = Command::new(engine).args(["rm", "-f", &name]).output();

    let mut argv: Vec<String> = vec!["run".into(), "-d".into(), "--name".into(), name.clone()];
    if let Some(network) = &reference.network {
        argv.push(format!("--network={network}"));
    }
    for mount in &reference.mounts {
        argv.push("-v".into());
        argv.push(crate::template::substitute(mount, subs));
    }
    argv.push(variant.image.clone().expect("container kind has an image"));
    argv.extend(
        reference
            .args
            .iter()
            .map(|a| crate::template::substitute(a, subs)),
    );

    let started = Instant::now();
    let out = Command::new(engine).args(&argv).output()?;
    if !out.status.success() {
        return Err(BenchError(format!(
            "starting competitor {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        )));
    }

    // Poll the container's cgroup while it runs; last reading ≈ final usage
    // (memory.peak is monotonic; cpu counters only grow). Sparse polling is
    // fine for exactly that reason.
    let mut last = CgroupReading::default();
    loop {
        let reading = read_cgroup_via_exec(engine, &name);
        if reading.memory_peak.is_some() || reading.cpu_usec.is_some() {
            last = reading;
        }
        let running = Command::new(engine)
            .args(["inspect", "--format", "{{.State.Running}}", &name])
            .output()
            .is_ok_and(|o| String::from_utf8_lossy(&o.stdout).trim() == "true");
        if !running {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;

    let exit_code = Command::new(engine)
        .args(["inspect", "--format", "{{.State.ExitCode}}", &name])
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .parse::<i64>()
                .ok()
        })
        .unwrap_or(-1);
    let logs = Command::new(engine).args(["logs", &name]).output()?;
    let _ = Command::new(engine).args(["rm", "-f", &name]).output();

    let stdout = String::from_utf8_lossy(&logs.stdout);
    if exit_code != 0 {
        return Err(BenchError(format!(
            "competitor {name} exited {exit_code}: {}{}",
            stdout,
            String::from_utf8_lossy(&logs.stderr)
        )));
    }
    let seconds = self_timed_seconds(&stdout).ok_or_else(|| {
        BenchError(format!(
            "competitor {name}: no self-timed `seconds` JSON on stdout: {stdout}"
        ))
    })?;
    // ru_maxrss FIRST — it is the statistic the recorded multiples and the
    // gated 1/5 bar were derived from; memory.peak also counts page cache
    // and would silently change what the bar enforces.
    let (peak_rss, rss_source) = match self_reported_rss(&stdout) {
        Some(peak) => (
            Some(peak),
            "self-reported ru_maxrss — the recorded statistic (bar derivation)",
        ),
        None => (
            last.memory_peak,
            "cgroup v2 memory.peak (in-container read) — NOTE: includes page cache, a different statistic than the recorded ru_maxrss",
        ),
    };
    Ok(ContainerRun {
        seconds,
        peak_rss,
        rss_source,
        cpu_ms: last.cpu_usec.map(|u| u / 1000),
        wall_ms,
    })
}

/// Run one competitor reference for a cell: warmup-free (the baselines have
/// always been measured cold-process, warm-cache — continuity), N runs,
/// medians. Returns `Missing` (never an error) when the image isn't built or
/// a driver's machine prerequisite fails — the rdlt side must still run.
pub fn run_competitor(
    variant: &Variant,
    reference: &CompetitorRef,
    runs: u32,
    subs: &BTreeMap<String, String>,
    fixtures: &[&crate::fixtures::Started],
) -> CompetitorSide {
    // Run-count precedence: the cell's competitor entry > the variant's own
    // override > the cell default.
    let runs = reference.runs.or(variant.runs).unwrap_or(runs).max(1);
    match variant.kind {
        VariantKind::SelfTimedContainer => {
            run_container_competitor(variant, reference, runs, subs, fixtures)
        }
        VariantKind::Driver => run_driver_competitor(variant, reference, runs, subs, fixtures),
    }
}

fn run_container_competitor(
    variant: &Variant,
    reference: &CompetitorRef,
    runs: u32,
    subs: &BTreeMap<String, String>,
    fixtures: &[&crate::fixtures::Started],
) -> CompetitorSide {
    let engine = match crate::fixtures::container_engine() {
        Ok(e) => e,
        Err(e) => {
            return CompetitorSide::Missing {
                reason: e.to_string(),
            };
        }
    };
    let image = variant
        .image
        .as_deref()
        .expect("container kind has an image");
    if !image_exists(&engine, image) {
        return CompetitorSide::Missing {
            reason: format!(
                "image `{image}` not built (build it from {})",
                variant.module_dir.display()
            ),
        };
    }

    let mut self_timed_ms = Vec::with_capacity(runs as usize);
    let mut last: Option<ContainerRun> = None;
    for seq in 0..runs {
        // Same discipline as the rdlt side: destination state reset between
        // runs (the shell harnesses dropped dest schemas per baseline run).
        // Every store the cell uses is reset, source and destination alike.
        for fixture in fixtures {
            if let Err(e) = fixture.reset() {
                return CompetitorSide::Missing {
                    reason: format!("fixture reset failed: {e}"),
                };
            }
        }
        match run_container_once(&engine, variant, reference, subs, seq) {
            Ok(run) => {
                self_timed_ms.push(run.seconds * 1000.0);
                last = Some(run);
            }
            Err(e) => {
                return CompetitorSide::Missing {
                    reason: e.to_string(),
                };
            }
        }
    }
    let median_ms = protocol::median(&self_timed_ms);
    let last = last.expect("runs >= 1");
    let cpu = CpuStats {
        mean_util: last.cpu_ms.map(|c| c as f64 / last.wall_ms),
        peak_util: None,
        user_sys_ms: last.cpu_ms,
        note: Some("cgroup v2 cpu.stat, last poll before exit".into()),
    };
    let rss = RssStats {
        peak_bytes: last.peak_rss,
        note: Some(if last.peak_rss.is_some() {
            last.rss_source.into()
        } else {
            "no RSS reading (cgroup unreachable, nothing self-reported) — null, not fabricated"
                .into()
        }),
    };
    CompetitorSide::Ok {
        runs_ms: self_timed_ms,
        median_ms,
        self_timed: true,
        cpu,
        rss,
        ratio_vs_rdlt: None, // filled once the rdlt median exists
        artifact_bytes: None, // filled by the caller, which knows the arm's `artifact_bytes_sh`
        extra: None,
    }
}

fn run_driver_competitor(
    variant: &Variant,
    reference: &CompetitorRef,
    runs: u32,
    subs: &BTreeMap<String, String>,
    fixtures: &[&crate::fixtures::Started],
) -> CompetitorSide {
    // Machine prerequisite ONCE before any run: an external system the driver
    // depends on (e.g. the Airbyte cluster) that this machine may simply not
    // have. Failure is a loud Missing{reason}, never an error.
    if let Some(probe) = &variant.prerequisite_sh {
        let out = Command::new("sh")
            .args(["-c", probe])
            .current_dir(&variant.module_dir)
            .output();
        let failed = match &out {
            Ok(o) => !o.status.success(),
            Err(_) => true,
        };
        if failed {
            let detail = out
                .map(|o| {
                    let mut text = String::from_utf8_lossy(&o.stdout).trim().to_string();
                    let err = String::from_utf8_lossy(&o.stderr).trim().to_string();
                    if !err.is_empty() {
                        if !text.is_empty() {
                            text.push_str("; ");
                        }
                        text.push_str(&err);
                    }
                    text
                })
                .unwrap_or_else(|e| e.to_string());
            return CompetitorSide::Missing {
                reason: format!("prerequisite failed for `{}`: {detail}", variant.id),
            };
        }
    }

    let mut self_timed_ms = Vec::with_capacity(runs as usize);
    let mut last: Option<DriverRun> = None;
    for _ in 0..runs {
        for fixture in fixtures {
            if let Err(e) = fixture.reset() {
                return CompetitorSide::Missing {
                    reason: format!("fixture reset failed: {e}"),
                };
            }
        }
        match run_driver_once(variant, reference, subs) {
            Ok(run) => {
                self_timed_ms.push(run.seconds * 1000.0);
                last = Some(run);
            }
            Err(e) => {
                return CompetitorSide::Missing {
                    reason: e.to_string(),
                };
            }
        }
    }
    let median_ms = protocol::median(&self_timed_ms);
    let last = last.expect("runs >= 1");
    CompetitorSide::Ok {
        runs_ms: self_timed_ms,
        median_ms,
        self_timed: true,
        cpu: CpuStats {
            mean_util: None,
            peak_util: None,
            user_sys_ms: None,
            note: Some("driver-run external system — no per-process CPU accounting".into()),
        },
        rss: RssStats {
            peak_bytes: last.peak_rss,
            note: Some(if last.peak_rss.is_some() {
                "driver-reported (the module states which system statistic this is)".into()
            } else {
                "no RSS reading (nothing driver-reported) — null, not fabricated".into()
            }),
        },
        ratio_vs_rdlt: None,
        artifact_bytes: None, // filled by the caller, which knows the arm
        extra: last.extra,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_toml_parses_and_rejects_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("variants.toml");
        std::fs::write(
            &p,
            "[[variant]]\nid='dlt-pyarrow'\npin='dlt 1.29.0'\nimage='rdlt-baseline'\n",
        )
        .unwrap();
        let variants = load_variants(&p).unwrap();
        assert_eq!(variants[0].pin, "dlt 1.29.0");
        assert_eq!(variants[0].image.as_deref(), Some("rdlt-baseline"));

        std::fs::write(&p, "[[variant]]\nid='x'\npin='p'\nimage='i'\nnope=1\n").unwrap();
        let err = load_variants(&p).unwrap_err().to_string();
        assert!(err.contains("nope"), "{err}");
    }

    #[test]
    fn variant_defaults_fill_pin_and_image() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("variants.toml");
        std::fs::write(
            &p,
            "[defaults]\npin='dlt 1.29.0'\nimage='rdlt-baseline'\n\n[[variant]]\nid='dlt'\n\n[[variant]]\nid='other'\nimage='custom'\n",
        )
        .unwrap();
        let variants = load_variants(&p).unwrap();
        assert_eq!(variants[0].pin, "dlt 1.29.0");
        assert_eq!(variants[0].image.as_deref(), Some("rdlt-baseline"));
        // per-variant override wins over the default
        assert_eq!(variants[1].image.as_deref(), Some("custom"));

        // no value and no default → a loud error naming the variant + field
        std::fs::write(&p, "[[variant]]\nid='bare'\n").unwrap();
        let err = load_variants(&p).unwrap_err().to_string();
        assert!(err.contains("bare") && err.contains("pin"), "{err}");
    }

    #[test]
    fn self_timed_seconds_takes_the_last_json_line() {
        let stdout =
            "noise\n{\"rows\": 10, \"seconds\": 1.5}\n{\"seconds\": 2.5, \"rows_per_s\": 4}\n";
        assert_eq!(self_timed_seconds(stdout), Some(2.5));
        assert_eq!(self_timed_seconds("no json here"), None);
    }

    fn container_variant(id: &str, image: &str) -> Variant {
        Variant {
            id: id.into(),
            pin: "dlt 0.0.0".into(),
            kind: VariantKind::SelfTimedContainer,
            image: Some(image.into()),
            driver: None,
            prerequisite_sh: None,
            runs: None,
            module_dir: ".".into(),
        }
    }

    #[test]
    fn missing_image_is_loud_not_silent() {
        let variant = container_variant("ghost", "rdlt-bench-definitely-not-built");
        let reference = CompetitorRef {
            artifact_bytes_sh: None,
            variant: "ghost".into(),
            args: vec!["x.py".into()],
            mounts: vec![],
            network: None,
            runs: None,
        };
        // With no engine OR no image, both paths must produce Missing{reason}.
        let fixture = crate::fixtures::start(
            &crate::fixtures::FixtureDef {
                id: "none".into(),
                kind: crate::fixtures::FixtureKind::None,
                generate_sh: None,
                container_args: vec![],
                hash_files: vec![],
                image: None,
                port: None,
                seed_sql: None,
                reset_sql: None,
                conn: None,
                service_sh: None,
                run_args: Vec::new(),
                container_port: None,
                ready_port: None,
                reset_sh: None,
                teardown_sh: None,
            },
            &BTreeMap::new(),
        )
        .unwrap();
        let side = run_competitor(&variant, &reference, 1, &BTreeMap::new(), &[&fixture]);
        match side {
            CompetitorSide::Missing { reason } => {
                assert!(!reason.is_empty());
            }
            CompetitorSide::Ok { .. } => panic!("ghost image cannot run"),
        }
    }

    #[test]
    fn self_reported_rss_reads_the_baseline_convention() {
        let stdout = "{\"rows\": 10, \"seconds\": 1.5, \"peak_rss_kb\": 2048}\n";
        assert_eq!(self_reported_rss(stdout), Some(2048 * 1024));
        assert_eq!(self_reported_rss("{\"seconds\": 1.0}"), None);
    }

    #[test]
    fn discovery_is_flat_and_duplicate_ids_name_both_files() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("alpha");
        let b = dir.path().join("beta");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        std::fs::write(
            a.join("variants.toml"),
            "[[variant]]\nid='dlt'\npin='dlt 1.29.0'\nimage='rdlt-baseline'\n",
        )
        .unwrap();
        std::fs::write(
            b.join("variants.toml"),
            "[[variant]]\nid='airbyte'\npin='airbyte 2.1.1'\nkind='driver'\ndriver='driver.py'\n",
        )
        .unwrap();
        let variants = discover_variants(dir.path()).unwrap();
        let ids: Vec<&str> = variants.iter().map(|v| v.id.as_str()).collect();
        assert_eq!(ids, ["dlt", "airbyte"]); // sorted module order (alpha, beta)

        std::fs::write(
            b.join("variants.toml"),
            "[[variant]]\nid='dlt'\npin='p'\nimage='i'\n",
        )
        .unwrap();
        let err = discover_variants(dir.path()).unwrap_err().to_string();
        assert!(
            err.contains("duplicate variant id `dlt`")
                && err.contains("alpha")
                && err.contains("beta"),
            "{err}"
        );
    }

    #[test]
    fn driver_variant_parses_and_requires_driver() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("variants.toml");
        std::fs::write(
            &p,
            "[[variant]]\nid='airbyte'\npin='airbyte 2.1.1'\nkind='driver'\ndriver='driver.py'\nprerequisite_sh='true'\nruns=3\n",
        )
        .unwrap();
        let v = &load_variants(&p).unwrap()[0];
        assert_eq!(v.kind, VariantKind::Driver);
        assert_eq!(v.runs, Some(3));
        assert_eq!(v.driver.as_ref().unwrap(), &dir.path().join("driver.py"));
        assert!(v.image.is_none());

        std::fs::write(&p, "[[variant]]\nid='x'\npin='p'\nkind='driver'\n").unwrap();
        let err = load_variants(&p).unwrap_err().to_string();
        assert!(err.contains("needs `driver`"), "{err}");
    }

    #[test]
    fn failed_prerequisite_is_missing_with_the_probe_output_as_reason() {
        let dir = tempfile::tempdir().unwrap();
        let variant = Variant {
            id: "airbyte".into(),
            pin: "airbyte 2.1.1".into(),
            kind: VariantKind::Driver,
            image: None,
            driver: Some(dir.path().join("driver.py")),
            prerequisite_sh: Some("echo 'abctl cluster not running'; exit 1".into()),
            runs: None,
            module_dir: dir.path().to_path_buf(),
        };
        let reference = CompetitorRef {
            artifact_bytes_sh: None,
            variant: "airbyte".into(),
            args: vec![],
            mounts: vec![],
            network: None,
            runs: None,
        };
        let side = run_competitor(&variant, &reference, 1, &BTreeMap::new(), &[]);
        match side {
            CompetitorSide::Missing { reason } => {
                assert!(
                    reason.contains("prerequisite failed") && reason.contains("abctl cluster"),
                    "{reason}"
                );
            }
            CompetitorSide::Ok { .. } => panic!("failed prerequisite cannot measure"),
        }
    }

    #[test]
    fn driver_summary_line_yields_seconds_rss_and_extra_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let driver = dir.path().join("driver.py");
        std::fs::write(
            &driver,
            "import json\nprint('noise')\nprint(json.dumps({'seconds': 42.5, 'rows': 5, 'peak_rss_kb': 1024, 'extra': {'sync_s': 40.0}}))\n",
        )
        .unwrap();
        let variant = Variant {
            id: "airbyte".into(),
            pin: "airbyte 2.1.1".into(),
            kind: VariantKind::Driver,
            image: None,
            driver: Some(driver),
            prerequisite_sh: None,
            runs: None,
            module_dir: dir.path().to_path_buf(),
        };
        let reference = CompetitorRef {
            artifact_bytes_sh: None,
            variant: "airbyte".into(),
            args: vec![],
            mounts: vec![],
            network: None,
            runs: Some(1),
        };
        let side = run_competitor(&variant, &reference, 5, &BTreeMap::new(), &[]);
        match side {
            CompetitorSide::Ok {
                runs_ms,
                median_ms,
                rss,
                extra,
                ..
            } => {
                assert_eq!(runs_ms.len(), 1); // the reference's runs override won
                assert_eq!(median_ms, 42500.0);
                assert_eq!(rss.peak_bytes, Some(1024 * 1024));
                assert_eq!(extra.unwrap()["sync_s"], 40.0);
            }
            CompetitorSide::Missing { reason } => panic!("driver run failed: {reason}"),
        }
    }
}

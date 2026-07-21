//! Competitors as a first-class module (research R4, contract BH4). dlt is the
//! first: a pinned container image, a variant registry, and same-metric
//! reporting. Wall time stays the baseline's in-process SELF-timing —
//! continuity with every recorded multiple; CPU/peak-RSS come from the
//! container's cgroup v2, polled while it runs. Missing baselines are LOUD.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::artifact::{CompetitorSide, CpuStats, RssStats};
use crate::cells::CompetitorRef;
use crate::protocol;
use crate::{BenchError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Feeds gated ratio bars.
    Baseline,
    /// Scoreboard context only.
    Context,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Variant {
    pub id: String,
    /// e.g. "dlt 1.29.0" — recorded in every artifact fingerprint.
    pub pin: String,
    pub image: String,
    pub role: Role,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VariantsFile {
    #[serde(default, rename = "variant")]
    variants: Vec<Variant>,
}

pub fn load_variants(path: &Path) -> Result<Vec<Variant>> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| BenchError(format!("reading {}: {e}", path.display())))?;
    let file: VariantsFile =
        toml::from_str(&raw).map_err(|e| BenchError(format!("parsing {}: {e}", path.display())))?;
    Ok(file.variants)
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

/// The container's cgroup v2 dir, resolved through its init PID's
/// `/proc/<pid>/cgroup` (`0::<path>` line) — works for rootless podman.
fn cgroup_dir(engine: &str, name: &str) -> Option<PathBuf> {
    let out = Command::new(engine)
        .args(["inspect", "--format", "{{.State.Pid}}", name])
        .output()
        .ok()?;
    let pid: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    let cgroup = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).ok()?;
    let path = cgroup.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
    Some(PathBuf::from(format!("/sys/fs/cgroup{path}")))
}

#[derive(Debug, Default, Clone)]
struct CgroupReading {
    memory_peak: Option<u64>,
    cpu_usec: Option<u64>,
    user_usec: Option<u64>,
    system_usec: Option<u64>,
}

fn read_cgroup(dir: &Path) -> CgroupReading {
    let memory_peak = std::fs::read_to_string(dir.join("memory.peak"))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .or_else(|| {
            // memory.peak needs kernel 6.5+; fall back to current (sampled).
            std::fs::read_to_string(dir.join("memory.current"))
                .ok()
                .and_then(|s| s.trim().parse().ok())
        });
    let mut reading = CgroupReading { memory_peak, ..CgroupReading::default() };
    if let Ok(stat) = std::fs::read_to_string(dir.join("cpu.stat")) {
        for line in stat.lines() {
            let mut parts = line.split_whitespace();
            match (parts.next(), parts.next().and_then(|v| v.parse::<u64>().ok())) {
                (Some("usage_usec"), Some(v)) => reading.cpu_usec = Some(v),
                (Some("user_usec"), Some(v)) => reading.user_usec = Some(v),
                (Some("system_usec"), Some(v)) => reading.system_usec = Some(v),
                _ => {}
            }
        }
    }
    reading
}

/// The baseline scripts' convention: one JSON line on stdout whose `seconds`
/// field is the in-process self-timed measurement.
fn self_timed_seconds(stdout: &str) -> Option<f64> {
    stdout.lines().rev().find_map(|line| {
        serde_json::from_str::<serde_json::Value>(line.trim())
            .ok()
            .and_then(|v| v.get("seconds").and_then(|s| s.as_f64()))
    })
}

struct ContainerRun {
    seconds: f64,
    peak_rss: Option<u64>,
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
        argv.push(crate::runner::substitute(mount, subs));
    }
    argv.push(variant.image.clone());
    argv.extend(reference.args.iter().map(|a| crate::runner::substitute(a, subs)));

    let started = Instant::now();
    let out = Command::new(engine).args(&argv).output()?;
    if !out.status.success() {
        return Err(BenchError(format!(
            "starting competitor {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        )));
    }

    // Poll the container's cgroup while it runs; last reading ≈ final usage
    // (memory.peak is monotonic; cpu counters only grow).
    let cgroup = cgroup_dir(engine, &name);
    let mut last = CgroupReading::default();
    loop {
        if let Some(dir) = &cgroup {
            let reading = read_cgroup(dir);
            if reading.memory_peak.is_some() || reading.cpu_usec.is_some() {
                last = reading;
            }
        }
        let running = Command::new(engine)
            .args(["inspect", "--format", "{{.State.Running}}", &name])
            .output()
            .is_ok_and(|o| String::from_utf8_lossy(&o.stdout).trim() == "true");
        if !running {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let wall_ms = started.elapsed().as_secs_f64() * 1000.0;

    let exit_code = Command::new(engine)
        .args(["inspect", "--format", "{{.State.ExitCode}}", &name])
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<i64>().ok())
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
        BenchError(format!("competitor {name}: no self-timed `seconds` JSON on stdout: {stdout}"))
    })?;
    Ok(ContainerRun {
        seconds,
        peak_rss: last.memory_peak,
        cpu_ms: last.cpu_usec.map(|u| u / 1000),
        wall_ms,
    })
}

/// Run one competitor reference for a cell: warmup-free (the baselines have
/// always been measured cold-process, warm-cache — continuity), N runs,
/// medians. Returns `Missing` (never an error) when the image isn't built —
/// the rdlt side must still run (BH4).
pub fn run_competitor(
    variant: &Variant,
    reference: &CompetitorRef,
    runs: u32,
    subs: &BTreeMap<String, String>,
) -> CompetitorSide {
    let engine = match crate::fixtures::container_engine() {
        Ok(e) => e,
        Err(e) => return CompetitorSide::Missing { reason: e.to_string() },
    };
    if !image_exists(&engine, &variant.image) {
        return CompetitorSide::Missing {
            reason: format!(
                "image `{}` not built (build it from benches/competitors/dlt/)",
                variant.image
            ),
        };
    }

    let runs = reference.runs.unwrap_or(runs).max(1);
    let mut self_timed_ms = Vec::with_capacity(runs as usize);
    let mut last: Option<ContainerRun> = None;
    for seq in 0..runs {
        match run_container_once(&engine, variant, reference, subs, seq) {
            Ok(run) => {
                self_timed_ms.push(run.seconds * 1000.0);
                last = Some(run);
            }
            Err(e) => return CompetitorSide::Missing { reason: e.to_string() },
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
        note: last.peak_rss.map_or_else(
            || Some("cgroup unreadable — reported null, not fabricated".into()),
            |_| Some("cgroup v2 memory.peak".into()),
        ),
    };
    CompetitorSide::Ok {
        runs_ms: self_timed_ms,
        median_ms,
        self_timed: true,
        cpu,
        rss,
        ratio_vs_rdlt: None, // filled once the rdlt median exists
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
            "[[variant]]\nid='dlt-pyarrow'\npin='dlt 1.29.0'\nimage='rdlt-baseline'\nrole='baseline'\n",
        )
        .unwrap();
        let variants = load_variants(&p).unwrap();
        assert_eq!(variants[0].role, Role::Baseline);

        std::fs::write(&p, "[[variant]]\nid='x'\npin='p'\nimage='i'\nrole='baseline'\nnope=1\n")
            .unwrap();
        let err = load_variants(&p).unwrap_err().to_string();
        assert!(err.contains("nope"), "{err}");
    }

    #[test]
    fn self_timed_seconds_takes_the_last_json_line() {
        let stdout = "noise\n{\"rows\": 10, \"seconds\": 1.5}\n{\"seconds\": 2.5, \"rows_per_s\": 4}\n";
        assert_eq!(self_timed_seconds(stdout), Some(2.5));
        assert_eq!(self_timed_seconds("no json here"), None);
    }

    #[test]
    fn missing_image_is_loud_not_silent() {
        let variant = Variant {
            id: "ghost".into(),
            pin: "dlt 0.0.0".into(),
            image: "rdlt-bench-definitely-not-built".into(),
            role: Role::Baseline,
        };
        let reference = CompetitorRef {
            variant: "ghost".into(),
            args: vec!["x.py".into()],
            mounts: vec![],
            network: None,
            runs: None,
        };
        // With no engine OR no image, both paths must produce Missing{reason}.
        let side = run_competitor(&variant, &reference, 1, &BTreeMap::new());
        match side {
            CompetitorSide::Missing { reason } => {
                assert!(!reason.is_empty());
            }
            CompetitorSide::Ok { .. } => panic!("ghost image cannot run"),
        }
    }

    #[test]
    fn cgroup_parser_reads_cpu_stat_shape() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("memory.peak"), "123456\n").unwrap();
        std::fs::write(
            dir.path().join("cpu.stat"),
            "usage_usec 5000000\nuser_usec 4000000\nsystem_usec 1000000\n",
        )
        .unwrap();
        let r = read_cgroup(dir.path());
        assert_eq!(r.memory_peak, Some(123456));
        assert_eq!(r.cpu_usec, Some(5_000_000));
        assert_eq!(r.system_usec, Some(1_000_000));
    }
}

//! The container kind: a pinned image whose entrypoint self-times and prints
//! the summary line. Wall time is the baseline's in-process self-timing;
//! CPU and peak RSS come from the container's cgroup v2, polled while it
//! runs, unless the summary line self-reports RSS.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{Duration, Instant};

use crate::artifact::{CpuStats, RssStats};
use crate::cell::CompetitorRef;
use crate::competitor::{Run, summary, variant::Variant};
use crate::error::{Error, Result};
use crate::{fixture, template};

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

/// The engine to run under, once per arm; a missing engine or an unbuilt
/// image is the arm's `Missing` reason.
pub(super) fn preflight(variant: &Variant) -> std::result::Result<String, String> {
    let engine = fixture::container_engine().map_err(|e| e.to_string())?;
    let image = variant
        .image
        .as_deref()
        .expect("container kind has an image");
    if !image_exists(&engine, image) {
        return Err(format!(
            "image `{image}` not built (build it from {})",
            variant.module_dir.display()
        ));
    }
    Ok(engine)
}

#[derive(Debug, Default, Clone)]
struct CgroupReading {
    memory_peak: Option<u64>,
    cpu_usec: Option<u64>,
}

/// Read the container's cgroup v2 accounting from INSIDE it (`podman exec`):
/// with a private cgroup namespace the container sees its own controllers at
/// `/sys/fs/cgroup`, which works regardless of where the harness itself runs
/// (host paths are unreachable from e.g. a toolbox shell). ONLY `memory.peak`
/// (a kernel high-water mark) is accepted as a peak — an instantaneous
/// `memory.current` labeled "peak" would be a fabricated metric.
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

/// One detached container run: start, poll its cgroup until it exits, then
/// read exit code and logs, remove it, and parse the summary line.
pub(super) fn run_once(
    engine: &str,
    variant: &Variant,
    reference: &CompetitorRef,
    subs: &BTreeMap<String, String>,
    seq: u32,
) -> Result<Run> {
    let name = format!("rdlt-bench-comp-{}-{seq}", variant.id);
    let _ = Command::new(engine).args(["rm", "-f", &name]).output();

    let mut argv: Vec<String> = vec!["run".into(), "-d".into(), "--name".into(), name.clone()];
    if let Some(network) = &reference.network {
        argv.push(format!("--network={network}"));
    }
    for mount in &reference.mounts {
        argv.push("-v".into());
        argv.push(template::substitute(mount, subs));
    }
    argv.push(variant.image.clone().expect("container kind has an image"));
    argv.extend(reference.args.iter().map(|a| template::substitute(a, subs)));

    let started = Instant::now();
    let out = Command::new(engine).args(&argv).output()?;
    if !out.status.success() {
        return Err(Error(format!(
            "starting competitor {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        )));
    }

    // Poll the container's cgroup while it runs; last reading ≈ final usage
    // (memory.peak is monotonic; cpu counters only grow), so sparse polling
    // is fine.
    let mut last = CgroupReading::default();
    loop {
        let reading = read_cgroup_via_exec(engine, &name);
        if reading.memory_peak.is_some() || reading.cpu_usec.is_some() {
            last = reading;
        }
        // A failed inspect is not evidence the container stopped — it is
        // evidence we could not ask. Treating the two alike ends the wait early
        // and reports a job that may still be running as finished.
        match Command::new(engine)
            .args(["inspect", "--format", "{{.State.Running}}", &name])
            .output()
        {
            Ok(o) if String::from_utf8_lossy(&o.stdout).trim() == "true" => {}
            Ok(_) => break,
            Err(e) => {
                return Err(Error(format!(
                    "polling container `{name}`: cannot run `{engine} inspect`: {e}"
                )));
            }
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
        return Err(Error(format!(
            "competitor {name} exited {exit_code}: {}{}",
            stdout,
            String::from_utf8_lossy(&logs.stderr)
        )));
    }
    let summary = summary::parse(&stdout).ok_or_else(|| {
        Error(format!(
            "competitor {name}: no self-timed `seconds` JSON on stdout: {stdout}"
        ))
    })?;
    // Self-reported ru_maxrss FIRST — it is the statistic the recorded
    // multiples and RSS bars were derived from; cgroup memory.peak also counts
    // page cache and would silently change what a bar enforces.
    let (peak_bytes, rss_note) = match summary.peak_rss_bytes() {
        Some(peak) => (
            Some(peak),
            "self-reported ru_maxrss — the recorded statistic (bar derivation)",
        ),
        None => (
            last.memory_peak,
            "cgroup v2 memory.peak (in-container read) — NOTE: includes page cache, a different statistic than the recorded ru_maxrss",
        ),
    };
    let cpu_ms = last.cpu_usec.map(|u| u / 1000);
    Ok(Run {
        seconds: summary.seconds,
        cpu: CpuStats {
            mean_util: cpu_ms.map(|c| c as f64 / wall_ms),
            peak_util: None,
            user_sys_ms: cpu_ms,
            note: Some("cgroup v2 cpu.stat, last poll before exit".into()),
        },
        rss: RssStats {
            peak_bytes,
            note: Some(if peak_bytes.is_some() {
                rss_note.into()
            } else {
                "no RSS reading (cgroup unreachable, nothing self-reported) — null, not fabricated"
                    .into()
            }),
        },
        extra: None,
    })
}

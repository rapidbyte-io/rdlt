//! The measurement protocol as executable code: quiet-machine guard, warmups,
//! N runs, medians/percentiles. Prose rules become refusals.

use crate::{BenchError, Result};

/// A machine is "quiet" when 1-minute loadavg is below this fraction of the
/// core count — background compile jobs and browsers blow straight past it.
const QUIET_LOAD_PER_CORE: f64 = 0.25;

/// Env var to run on a loaded machine anyway (the run is then loudly annotated
/// in the artifact — `forced: true` — instead of refused).
pub const FORCE_ENV: &str = "RDLT_BENCH_FORCE";

#[derive(Debug, Clone, PartialEq)]
pub enum QuietVerdict {
    Quiet,
    /// Machine is loaded; a forced run proceeds with this annotation recorded
    /// in the artifact.
    Annotated(String),
}

pub fn loadavg_1min() -> Result<f64> {
    let raw = std::fs::read_to_string("/proc/loadavg")?;
    raw.split_whitespace()
        .next()
        .and_then(|f| f.parse().ok())
        .ok_or_else(|| BenchError(format!("unparseable /proc/loadavg: {raw}")))
}

fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// Whether the quiet guard is being overridden this invocation (`RDLT_BENCH_FORCE=1`).
/// Recorded as `forced` in every artifact written under the override.
pub fn forced() -> bool {
    std::env::var(FORCE_ENV).is_ok_and(|v| v == "1")
}

/// One classless rule: every measured run REFUSES on a loaded machine unless
/// forced — then it proceeds loudly annotated. There is no measurement
/// configuration that is exempt.
pub fn quiet_guard(load1: f64, ncores: usize, forced: bool) -> Result<QuietVerdict> {
    let threshold = QUIET_LOAD_PER_CORE * ncores as f64;
    if load1 <= threshold {
        return Ok(QuietVerdict::Quiet);
    }
    let note = format!(
        "MACHINE NOT QUIET: loadavg {load1:.2} > {threshold:.2} ({ncores} cores) — number is context, not evidence"
    );
    if forced {
        Ok(QuietVerdict::Annotated(note))
    } else {
        Err(BenchError(format!(
            "refusing run: {note} (set {FORCE_ENV}=1 to run annotated)"
        )))
    }
}

/// Guard using the live machine state. A measured run WAITS for the machine to
/// settle first (container spin-up and fixture builds from earlier cells linger
/// in the 1-minute loadavg) — refusal is for load that never decays, i.e.
/// something ELSE is running. A forced run skips the wait and annotates.
pub fn quiet_guard_now() -> Result<QuietVerdict> {
    let forced = forced();
    let ncores = cores();
    if forced {
        return quiet_guard(loadavg_1min()?, ncores, forced);
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        let load1 = loadavg_1min()?;
        if load1 <= QUIET_LOAD_PER_CORE * ncores as f64 {
            return Ok(QuietVerdict::Quiet);
        }
        if std::time::Instant::now() >= deadline {
            return quiet_guard(load1, ncores, forced);
        }
        eprintln!("   waiting for quiet machine (loadavg {load1:.2}) ...");
        std::thread::sleep(std::time::Duration::from_secs(15));
    }
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Median of the samples (mean-of-middle-two for even N). Panics on empty —
/// the protocol never records zero runs.
pub fn median(samples: &[f64]) -> f64 {
    assert!(!samples.is_empty(), "median of zero runs");
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// The last stdout line that parses as a JSON object carrying `field`, and
/// that field's value. The self-timing convention the dlt baselines, the
/// shred-only example, and the subprocess `self_json_seconds` timing all
/// share: a run may print noise, then one JSON summary line last.
pub(crate) fn last_json_field(stdout: &str, field: &str) -> Option<serde_json::Value> {
    stdout.lines().rev().find_map(|line| {
        serde_json::from_str::<serde_json::Value>(line.trim())
            .ok()
            .and_then(|v| v.get(field).cloned())
    })
}

/// Nearest-rank p95 (the sample at ceil(0.95·N), 1-indexed).
pub fn p95(samples: &[f64]) -> f64 {
    assert!(!samples.is_empty(), "p95 of zero runs");
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let rank = ((0.95 * sorted.len() as f64).ceil() as usize).max(1);
    sorted[rank - 1]
}

/// One measured sample: wall time plus whatever the mode's collector attached.
#[derive(Debug, Clone)]
pub struct Sample<T> {
    pub wall_ms: f64,
    pub detail: T,
}

/// The uniform loop: `warmups` uncounted runs, then `runs` counted ones.
/// The closure receives `counted` so collectors can skip warmup sampling.
pub fn run_protocol<T>(
    warmups: u32,
    runs: u32,
    mut one_run: impl FnMut(bool) -> Result<Sample<T>>,
) -> Result<Vec<Sample<T>>> {
    if runs == 0 {
        return Err(BenchError("protocol requires runs >= 1".into()));
    }
    for _ in 0..warmups {
        one_run(false)?;
    }
    let mut samples = Vec::with_capacity(runs as usize);
    for _ in 0..runs {
        samples.push(one_run(true)?);
    }
    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_odd_even_and_p95_index() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), 2.5);
        assert_eq!(p95(&[1.0]), 1.0);
        // 20 samples: rank ceil(19) = 19 → the 19th smallest.
        let v: Vec<f64> = (1..=20).map(f64::from).collect();
        assert_eq!(p95(&v), 19.0);
    }

    #[test]
    fn loaded_machine_refuses_unless_forced() {
        let err = quiet_guard(6.0, 8, false).unwrap_err().to_string();
        assert!(err.contains("refusing run"), "{err}");
        assert!(err.contains(FORCE_ENV), "{err}");
        let forced = quiet_guard(6.0, 8, true).unwrap();
        assert!(matches!(forced, QuietVerdict::Annotated(ref n) if n.contains("NOT QUIET")));
    }

    #[test]
    fn quiet_machine_passes_regardless_of_force() {
        assert_eq!(quiet_guard(0.5, 8, false).unwrap(), QuietVerdict::Quiet);
        assert_eq!(quiet_guard(0.5, 8, true).unwrap(), QuietVerdict::Quiet);
    }

    #[test]
    fn protocol_counts_warmups_separately() {
        let mut calls = Vec::new();
        let samples = run_protocol(2, 3, |counted| {
            calls.push(counted);
            Ok(Sample {
                wall_ms: 1.0,
                detail: (),
            })
        })
        .unwrap();
        assert_eq!(samples.len(), 3);
        assert_eq!(calls, vec![false, false, true, true, true]);
        assert!(
            run_protocol(0, 0, |_| Ok(Sample {
                wall_ms: 0.0,
                detail: ()
            }))
            .is_err()
        );
    }
}

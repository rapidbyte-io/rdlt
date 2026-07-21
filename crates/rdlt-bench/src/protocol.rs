//! The 004 measurement protocol as executable code (contract BH2): quiet-machine
//! guard, warmups, N runs, medians/percentiles. Prose rules become refusals.

use crate::cells::Class;
use crate::{BenchError, Result};

/// A machine is "quiet" when 1-minute loadavg is below this fraction of the
/// core count — background compile jobs and browsers blow straight past it.
const QUIET_LOAD_PER_CORE: f64 = 0.25;

/// Env var to run gated cells on a loaded machine anyway (the run is then
/// loudly annotated in the artifact instead of refused).
pub const FORCE_ENV: &str = "RDLT_BENCH_FORCE";

#[derive(Debug, Clone, PartialEq)]
pub enum QuietVerdict {
    Quiet,
    /// Machine is loaded; scoreboard runs (or forced gated runs) proceed with
    /// this annotation recorded in the artifact.
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

/// BH2: gated runs REFUSE on a loaded machine (unless forced — then loudly
/// annotated); scoreboard runs proceed annotated.
pub fn quiet_guard(class: Class, load1: f64, ncores: usize, forced: bool) -> Result<QuietVerdict> {
    let threshold = QUIET_LOAD_PER_CORE * ncores as f64;
    if load1 <= threshold {
        return Ok(QuietVerdict::Quiet);
    }
    let note = format!(
        "MACHINE NOT QUIET: loadavg {load1:.2} > {threshold:.2} ({ncores} cores) — number is context, not evidence"
    );
    match class {
        Class::Gated if !forced => Err(BenchError(format!(
            "refusing gated run: {note} (set {FORCE_ENV}=1 to run annotated)"
        ))),
        _ => Ok(QuietVerdict::Annotated(note)),
    }
}

/// Guard using the live machine state.
pub fn quiet_guard_now(class: Class) -> Result<QuietVerdict> {
    let forced = std::env::var(FORCE_ENV).is_ok_and(|v| v == "1");
    quiet_guard(class, loadavg_1min()?, cores(), forced)
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
    fn gated_refuses_on_loaded_machine_unless_forced() {
        let err = quiet_guard(Class::Gated, 6.0, 8, false).unwrap_err().to_string();
        assert!(err.contains("refusing gated run"), "{err}");
        assert!(err.contains(FORCE_ENV), "{err}");
        let forced = quiet_guard(Class::Gated, 6.0, 8, true).unwrap();
        assert!(matches!(forced, QuietVerdict::Annotated(ref n) if n.contains("NOT QUIET")));
    }

    #[test]
    fn scoreboard_annotates_and_quiet_passes() {
        assert_eq!(quiet_guard(Class::Gated, 0.5, 8, false).unwrap(), QuietVerdict::Quiet);
        let v = quiet_guard(Class::Scoreboard, 6.0, 8, false).unwrap();
        assert!(matches!(v, QuietVerdict::Annotated(_)));
    }

    #[test]
    fn protocol_counts_warmups_separately() {
        let mut calls = Vec::new();
        let samples = run_protocol(2, 3, |counted| {
            calls.push(counted);
            Ok(Sample { wall_ms: 1.0, detail: () })
        })
        .unwrap();
        assert_eq!(samples.len(), 3);
        assert_eq!(calls, vec![false, false, true, true, true]);
        assert!(run_protocol(0, 0, |_| Ok(Sample { wall_ms: 0.0, detail: () })).is_err());
    }
}

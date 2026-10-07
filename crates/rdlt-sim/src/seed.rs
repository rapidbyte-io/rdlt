//! Seeds, and the runner that makes a simulation reproducible from one.

#[cfg(test)]
mod tests;

use std::fmt;
use std::fs::File;
use std::future::Future;
use std::io::{BufWriter, Write as _};
use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde_json::{Map, Value, json};

use crate::env::{CORES, SimEnv};

/// Environment variable naming a single seed to replay.
pub const SEED_VAR: &str = "RDLT_SIM_SEED";

/// Environment variable setting how many seeds a simulation suite covers.
pub const SEEDS_VAR: &str = "RDLT_SIM_SEEDS";

/// Environment variable setting the first of the seeds a simulation suite covers, so a large run
/// can be split into shards.
pub const SEEDS_FROM_VAR: &str = "RDLT_SIM_SEEDS_FROM";

/// Environment variable setting how many cores a simulation test's seeds share, where the host's
/// are not the ones to count on.
pub const CORES_VAR: &str = "RDLT_SIM_CORES";

/// The single value a simulation run derives from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seed(u64);

impl Seed {
    /// Wraps a raw seed value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw seed value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Seed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A simulation variable held something other than the number it takes.
#[derive(Debug, thiserror::Error)]
#[error("{variable} must be {expected}, got {value:?}")]
pub struct SeedVarError {
    variable: &'static str,
    expected: &'static str,
    value: String,
}

/// A contiguous run of seeds, wrapping at `u64::MAX`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SeedRange {
    start: u64,
    count: u64,
}

impl SeedRange {
    fn seeds(self) -> impl Iterator<Item = Seed> {
        (0..self.count).map(move |offset| Seed(self.start.wrapping_add(offset)))
    }
}

/// The seeds a simulation test covers.
///
/// A non-empty `RDLT_SIM_SEED` selects exactly that seed. Otherwise the test covers `n` seeds from
/// `RDLT_SIM_SEEDS_FROM` (0 when it is unset or empty), where `n` is `RDLT_SIM_SEEDS` when it is
/// set and non-empty, else `default_count`.
///
/// # Panics
///
/// Panics when either variable holds something other than an unsigned integer.
pub fn seeds(default_count: u64) -> impl Iterator<Item = Seed> {
    let single = std::env::var(SEED_VAR).ok();
    let count = std::env::var(SEEDS_VAR).ok();
    let from = std::env::var(SEEDS_FROM_VAR).ok();
    match select(
        single.as_deref(),
        count.as_deref(),
        from.as_deref(),
        default_count,
    ) {
        Ok(range) => range.seeds(),
        Err(error) => panic!("{error}"),
    }
}

fn select(
    single: Option<&str>,
    count: Option<&str>,
    from: Option<&str>,
    default_count: u64,
) -> Result<SeedRange, SeedVarError> {
    if let Some(value) = single.filter(|value| !value.is_empty()) {
        return Ok(SeedRange {
            start: parse(SEED_VAR, value)?,
            count: 1,
        });
    }
    let count = match count.filter(|value| !value.is_empty()) {
        Some(value) => parse(SEEDS_VAR, value)?,
        None => default_count,
    };
    let start = match from.filter(|value| !value.is_empty()) {
        Some(value) => parse(SEEDS_FROM_VAR, value)?,
        None => 0,
    };
    Ok(SeedRange { start, count })
}

fn parse(variable: &'static str, value: &str) -> Result<u64, SeedVarError> {
    value.trim().parse().map_err(|_| SeedVarError {
        variable,
        expected: "an unsigned integer",
        value: value.to_owned(),
    })
}

/// Runs `scenario` on a fresh single-threaded runtime with a paused clock and a [`SimEnv`] seeded
/// by `seed`.
///
/// # Panics
///
/// Re-raises any panic from `scenario` after printing the seed that reproduces it.
pub fn run<F, Fut, T>(seed: Seed, scenario: F) -> T
where
    F: FnOnce(Arc<SimEnv>) -> Fut,
    Fut: Future<Output = T>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a current-thread runtime with a paused clock builds");
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async { scenario(Arc::new(SimEnv::new(seed))).await })
    }));
    outcome.unwrap_or_else(|payload| {
        report_failure(seed, Weight::One);
        panic::resume_unwind(payload)
    })
}

/// Runs `scenario` for `seed` on a runtime of many threads and the real clock, with a
/// [`SimEnv::threaded`]: as the engine runs in production, so races the paused single thread
/// never meets can happen, and a failure is not replayed exactly.
///
/// # Panics
///
/// Panics when the scenario panics, after printing the seed.
pub fn run_threaded<F, Fut, T>(seed: Seed, scenario: F) -> T
where
    F: FnOnce(Arc<SimEnv>) -> Fut,
    Fut: Future<Output = T>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(crate::env::WORKERS.get())
        .enable_time()
        .build()
        .expect("a runtime of many threads builds");
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async { scenario(Arc::new(SimEnv::threaded(seed))).await })
    }));
    outcome.unwrap_or_else(|payload| {
        report_failure(seed, Weight::Threaded);
        panic::resume_unwind(payload)
    })
}

/// How many threads one seed's run keeps busy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Weight {
    /// A run on one thread, as [`run`] and a run on the simulated network are.
    One,
    /// A run of [`run_threaded`]: its runtime's workers and its compute pool's threads.
    Threaded,
}

impl Weight {
    fn threads(self) -> usize {
        match self {
            Self::One => 1,
            Self::Threaded => CORES.get(),
        }
    }
}

/// What a seed's run says of itself in its timing line, beside its wall time and outcome.
pub trait Recorded {
    /// The fields this result adds to its seed's timing line.
    fn recorded(&self) -> Map<String, Value> {
        Map::new()
    }
}

impl Recorded for () {}

/// Runs `scenario` for each of `seeds`, side by side on as many threads as the cores hold runs
/// of `weight`; what each run returned, in seed order.
///
/// The cores are the host's, or as many as `RDLT_SIM_CORES` says when it is set and non-empty.
/// Each seed's wall time, outcome and what its result records go as one JSON line to
/// `target/sim-timings/<name>.jsonl` beneath the workspace the test runs in, then a summary line
/// of how many seeds a second ran, which standard error shows too. The seeds are run as given,
/// whatever `RDLT_SIM_SEED` and `RDLT_SIM_SEEDS` say.
///
/// # Panics
///
/// Panics when `RDLT_SIM_CORES` holds something other than a positive integer, and once every
/// seed has run, naming each seed whose run panicked.
pub fn for_each_seed<T, F>(
    name: &str,
    seeds: impl IntoIterator<Item = Seed>,
    weight: Weight,
    scenario: F,
) -> Vec<T>
where
    T: Recorded + Send,
    F: Fn(Seed) -> T + Sync,
{
    let here = std::env::current_dir().expect("a test runs in a directory");
    sweep(&here, name, seeds, weight, scenario)
}

#[expect(
    clippy::print_stderr,
    reason = "the seed must reach the test output to be replayable"
)]
pub(crate) fn report_failure(seed: Seed, weight: Weight) {
    eprintln!("rdlt-sim: failing seed {seed}; {}", replay(seed, weight));
}

/// How a failing seed's run of `weight` is run again.
fn replay(seed: Seed, weight: Weight) -> String {
    match weight {
        Weight::One => format!("replay it with `just sim {seed}`"),
        Weight::Threaded => format!(
            "rerun it with `RDLT_SIM_SEED={seed} just stress`, which real threads and the real \
             clock keep from replaying it exactly"
        ),
    }
}

/// Runs [`for_each_seed`] for a test that runs in `here`.
fn sweep<T, F>(
    here: &Path,
    name: &str,
    seeds: impl IntoIterator<Item = Seed>,
    weight: Weight,
    scenario: F,
) -> Vec<T>
where
    T: Recorded + Send,
    F: Fn(Seed) -> T + Sync,
{
    let host = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
    let cores = cores(std::env::var(CORES_VAR).ok().as_deref(), host)
        .unwrap_or_else(|error| panic!("{error}"));
    let seeds: Vec<Seed> = seeds.into_iter().collect();
    drive(
        &timings(here, name),
        &seeds,
        side_by_side(cores.get(), weight),
        scenario,
    )
    .unwrap_or_else(|failed| panic!("rdlt-sim: {name}: {failed}"))
}

/// The cores a test's seeds share: as many as `value`, `RDLT_SIM_CORES`'s, says when it is set
/// and non-empty, else `host`'s.
fn cores(value: Option<&str>, host: NonZeroUsize) -> Result<NonZeroUsize, SeedVarError> {
    match value.filter(|value| !value.is_empty()) {
        Some(value) => value.trim().parse().map_err(|_| SeedVarError {
            variable: CORES_VAR,
            expected: "a positive integer",
            value: value.to_owned(),
        }),
        None => Ok(host),
    }
}

/// The seeds whose runs panicked, of how many ran.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{} of {ran} seeds failed: {}", failed.len(), listed(failed))]
struct Failed {
    failed: Vec<Seed>,
    ran: usize,
}

fn listed(seeds: &[Seed]) -> String {
    let listed: Vec<String> = seeds.iter().map(Seed::to_string).collect();
    listed.join(", ")
}

/// How many runs of `weight` go side by side on `cores`: one at least.
fn side_by_side(cores: usize, weight: Weight) -> NonZeroUsize {
    NonZeroUsize::new(cores / weight.threads()).unwrap_or(NonZeroUsize::MIN)
}

/// Where the sweep `name` writes its timing lines: `target/sim-timings` in the nearest directory
/// at or above `here` that holds `Cargo.lock`, the workspace's root.
fn timings(here: &Path, name: &str) -> PathBuf {
    let workspace = here
        .ancestors()
        .find(|dir| dir.join("Cargo.lock").is_file())
        .unwrap_or_else(|| panic!("no workspace holds {}", here.display()));
    workspace
        .join("target/sim-timings")
        .join(format!("{name}.jsonl"))
}

/// Runs `scenario` for each of `seeds` on `threads` threads, writing each seed's timing line,
/// then the summary line, to `path`.
fn drive<T, F>(
    path: &Path,
    seeds: &[Seed],
    threads: NonZeroUsize,
    scenario: F,
) -> Result<Vec<T>, Failed>
where
    T: Recorded + Send,
    F: Fn(Seed) -> T + Sync,
{
    if let Some(directory) = path.parent() {
        std::fs::create_dir_all(directory).expect("the timing directory is created");
    }
    let file = File::create(path).expect("the timing file is created");
    let lines = Mutex::new(BufWriter::new(file));
    let started = Instant::now();
    let outcomes = run_side_by_side(seeds, threads, &scenario, &lines);
    let elapsed = started.elapsed();
    let (mut results, mut failed) = (Vec::with_capacity(seeds.len()), Vec::new());
    for (seed, outcome) in seeds.iter().zip(outcomes) {
        match outcome {
            Ok(result) => results.push(result),
            Err(_) => failed.push(*seed),
        }
    }
    let summary = summary(seeds.len(), failed.len(), threads, elapsed);
    report_summary(path, &summary);
    let mut lines = lines.into_inner();
    writeln!(lines, "{summary}").expect("the summary line is written");
    lines.flush().expect("the timing lines are written");
    if failed.is_empty() {
        Ok(results)
    } else {
        Err(Failed {
            failed,
            ran: seeds.len(),
        })
    }
}

/// Each seed's outcome, in seed order, the seeds taken in turn by `threads` threads.
fn run_side_by_side<T, F>(
    seeds: &[Seed],
    threads: NonZeroUsize,
    scenario: &F,
    lines: &Mutex<BufWriter<File>>,
) -> Vec<std::thread::Result<T>>
where
    T: Recorded + Send,
    F: Fn(Seed) -> T + Sync,
{
    let slots: Vec<Mutex<Option<std::thread::Result<T>>>> =
        seeds.iter().map(|_| Mutex::new(None)).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for worker in 0..threads.get().min(seeds.len()) {
            // The default builder gives each thread the stack a test's thread has.
            std::thread::Builder::new()
                .name(format!("seeds-{worker}"))
                .spawn_scoped(scope, || {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&seed) = seeds.get(index) else {
                            break;
                        };
                        let began = Instant::now();
                        let outcome = panic::catch_unwind(AssertUnwindSafe(|| scenario(seed)));
                        let line = line(seed, began.elapsed(), &outcome);
                        writeln!(lines.lock(), "{line}").expect("a timing line is written");
                        *slots[index].lock() = Some(outcome);
                    }
                })
                .expect("a thread for seeds starts");
        }
    });
    slots
        .into_iter()
        .map(|slot| slot.into_inner().expect("every seed ran"))
        .collect()
}

/// A seed's timing line: its wall time, its outcome, and what a run that passed records.
fn line<T: Recorded>(seed: Seed, wall: Duration, outcome: &std::thread::Result<T>) -> Value {
    let mut fields = Map::new();
    fields.insert("seed".to_owned(), json!(seed.value()));
    fields.insert("wall_ms".to_owned(), json!(wall.as_secs_f64() * 1e3));
    let outcome = match outcome {
        Ok(result) => {
            fields.extend(result.recorded());
            "passed"
        }
        Err(_) => "failed",
    };
    fields.insert("outcome".to_owned(), json!(outcome));
    Value::Object(fields)
}

/// A sweep's summary line: how many seeds ran and failed, on how many threads, how long they
/// took, and how many ran a second.
fn summary(seeds: usize, failed: usize, threads: NonZeroUsize, elapsed: Duration) -> Value {
    #[expect(clippy::cast_precision_loss, reason = "a seed count is far below 2^52")]
    let rate = seeds as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    json!({
        "seeds": seeds,
        "failed": failed,
        "threads": threads.get(),
        "elapsed_ms": elapsed.as_secs_f64() * 1e3,
        "seeds_per_s": rate,
    })
}

#[expect(
    clippy::print_stderr,
    reason = "the rate reaches the test's output beside its timing file"
)]
fn report_summary(path: &Path, summary: &Value) {
    eprintln!("rdlt-sim: {}: {summary}", path.display());
}

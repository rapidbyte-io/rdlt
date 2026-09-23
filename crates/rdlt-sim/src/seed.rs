//! Seeds, and the runner that makes a simulation reproducible from one.

#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;

use crate::env::SimEnv;

/// Environment variable naming a single seed to replay.
pub const SEED_VAR: &str = "RDLT_SIM_SEED";

/// Environment variable setting how many seeds a simulation suite covers.
pub const SEEDS_VAR: &str = "RDLT_SIM_SEEDS";

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

/// A seed variable held something other than an unsigned integer.
#[derive(Debug, thiserror::Error)]
#[error("{variable} must be an unsigned integer, got {value:?}")]
pub struct SeedVarError {
    variable: &'static str,
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
/// A non-empty `RDLT_SIM_SEED` selects exactly that seed. Otherwise the test covers seeds `0..n`,
/// where `n` is `RDLT_SIM_SEEDS` when it is set and non-empty, else `default_count`.
///
/// # Panics
///
/// Panics when either variable holds something other than an unsigned integer.
pub fn seeds(default_count: u64) -> impl Iterator<Item = Seed> {
    let single = std::env::var(SEED_VAR).ok();
    let count = std::env::var(SEEDS_VAR).ok();
    match select(single.as_deref(), count.as_deref(), default_count) {
        Ok(range) => range.seeds(),
        Err(error) => panic!("{error}"),
    }
}

fn select(
    single: Option<&str>,
    count: Option<&str>,
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
    Ok(SeedRange { start: 0, count })
}

fn parse(variable: &'static str, value: &str) -> Result<u64, SeedVarError> {
    value.trim().parse().map_err(|_| SeedVarError {
        variable,
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
        report_failure(seed);
        panic::resume_unwind(payload)
    })
}

#[expect(
    clippy::print_stderr,
    reason = "the seed must reach the test output to be replayable"
)]
fn report_failure(seed: Seed) {
    eprintln!("rdlt-sim: failing seed {seed}; replay it with `just sim {seed}`");
}

//! The engine's configuration a simulation draws, and how hard its source presses on its budget.

use std::time::Duration;

use rdlt_engine::{BatchPolicy, CommitPolicy, EngineConfig, GrowthLimits, RetryPolicy};

use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::workload::Workload;
use crate::world::Pressure;

/// How often a streaming world plans again: every quarter second, so its runs meet the partitions
/// and rows that arrive.
pub(super) const REPLAN: Duration = Duration::from_millis(250);

/// The engine's configuration, drawn from `rng`.
///
/// Its memory budget is between the least an engine takes, whatever its partitions, and twice
/// that: small enough that a source pressing on it, as [`pressed`] draws, fills its shares.
///
/// A run reads at least one partition more at once than the `endless` partitions it follows
/// without end, which hold their places for as long as it runs, holds to the `growth` limits,
/// and coalesces and shreds as `batch` says. Where `wait` is drawn, the engine waits that long
/// for each connector call.
pub(super) fn config(
    rng: &mut SplitMix64,
    streaming: bool,
    endless: usize,
    growth: GrowthLimits,
    batch: BatchPolicy,
    wait: Option<Duration>,
) -> EngineConfig {
    let least = EngineConfig::least_memory(1);
    let every = rng
        .chance(700)
        .then(|| Duration::from_millis(100 + rng.below(3000)));
    let rows = (every.is_none() || rng.chance(500)).then(|| 5 + rng.below(60));
    let commit = CommitPolicy::new(every, rows, None).expect("the drawn policy has a threshold");
    let retry = RetryPolicy::default()
        .max_attempts(4)
        .initial(Duration::from_millis(1))
        .max_delay(Duration::from_millis(100));
    let lanes = u16::try_from(1 + rng.below(3)).unwrap_or(1);
    let builder = EngineConfig::builder();
    let builder = if streaming {
        builder.replan(REPLAN)
    } else {
        builder
    };
    let builder = match wait {
        Some(wait) => builder.connector_wait(wait),
        None => builder,
    };
    let memory = least + rng.below(least);
    let lane_window = to_usize(1 + rng.below(3));
    let partitions = to_usize(1 + rng.below(4)).max(endless + 1);
    builder
        .memory(memory)
        .lanes(lanes)
        .lane_window(lane_window)
        .partitions(partitions)
        .partition_buffer(to_usize(1 + rng.below(4)))
        .barrier_wait(Duration::from_millis(10 + rng.below(2000)))
        .commit(commit)
        .retry(retry)
        .growth(growth)
        .batch(batch)
        .build()
        .expect("the drawn configuration is valid")
}

/// The engine's wait for each connector call in the world `seed` where its calls may hang,
/// drawn apart from the world, so every other draw of the seed is as it was: 5 to 20 s, longer
/// than any call a clean run makes, and short enough that a run meeting several hangs ends well
/// within the oracle's limit; none elsewhere, so the engine's default holds.
pub(super) fn waited(seed: Seed, hangs: bool) -> Option<Duration> {
    let mut rng = SplitMix64::new(seed.value().rotate_left(37));
    hangs.then(|| Duration::from_secs(5 + rng.below(16)))
}

/// The growth limits of an engine of the world `seed`, drawn apart from the world, so every other
/// draw of the seed is as it was: in a quarter of worlds one to three destination writers open at
/// once, so lanes close the writers they wrote longest ago, and the defaults elsewhere.
pub(super) fn growth(seed: Seed) -> GrowthLimits {
    let mut rng = SplitMix64::new(seed.value().rotate_left(29));
    let defaults = GrowthLimits::default();
    if rng.chance(250) {
        let writers = to_usize(1 + rng.below(3));
        GrowthLimits::new(defaults.child_tables().get(), writers)
            .expect("the drawn limits are valid")
    } else {
        defaults
    }
}

/// The batch policy of an engine of the world `seed`, drawn apart from the world, so every other
/// draw of the seed is as it was: in a quarter of worlds a shredding job takes at most 64 bytes to
/// 64 KiB, so a flush of a few short JSON pushes is cut into several jobs, and the default
/// elsewhere.
pub(super) fn batch(seed: Seed) -> BatchPolicy {
    let mut rng = SplitMix64::new(seed.value().rotate_left(43));
    let defaults = BatchPolicy::default();
    if rng.chance(250) {
        let limit = 64_usize << rng.below(11);
        BatchPolicy::new(
            defaults.target_bytes().get(),
            defaults.max_rows().get(),
            defaults.max_latency(),
            limit,
        )
        .expect("the drawn policy is valid")
    } else {
        defaults
    }
}

/// How many partitions of `workload`'s streams never end.
pub(super) fn endless(workload: &Workload) -> usize {
    workload
        .streams
        .iter()
        .filter(|stream| stream.unbounded)
        .map(|stream| stream.partitions.len())
        .sum()
}

/// How hard the source of the world `seed`, of `workload`, presses on the budget of an engine of
/// `config`, drawn apart from the world, so every other draw of the seed is as it was.
pub(super) fn pressed(seed: Seed, config: &EngineConfig, workload: &Workload) -> Pressure {
    let mut rng = SplitMix64::new(seed.value().rotate_left(17));
    let cursors = config.memory().get() / 64;
    let partitions = workload
        .streams
        .iter()
        .map(|stream| stream.partitions.len());
    let partitions = u64::try_from(partitions.sum::<usize>()).unwrap_or(u64::MAX);
    let mut pressure = Pressure::draw(&mut rng, &config.limits(), cursors, partitions);
    // Spaces fill a spaced push up to its limit, or the frame a transport carries it in, beside
    // the widest fat value it may carry.
    if workload.features.fat {
        let limits = config.limits();
        let room = limits
            .json_push_bytes
            .min(limits.frame_bytes)
            .saturating_sub(crate::fat::least_push() / 16 + (64 << 10));
        let mut drawn = SplitMix64::new(seed.value().rotate_left(59));
        pressure.spaces = usize::try_from(room / 2 + drawn.below((room / 2).max(1))).unwrap_or(0);
    }
    pressure
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(1)
}

#[cfg(test)]
mod tests;

//! The engine's configuration a simulation draws, and how hard its source presses on its budget.

use std::time::Duration;

use rdlt_engine::{CommitPolicy, EngineConfig, RetryPolicy};

use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::workload::Workload;
use crate::world::Pressure;

/// The engine's configuration, drawn from `rng`; a streaming world plans again every quarter
/// second, so its runs meet the partitions and rows that arrive.
///
/// Its memory budget is between the least an engine takes, whatever its partitions, and twice
/// that: small enough that a source pressing on it, as [`pressed`] draws, fills its shares.
///
/// A run reads at least one partition more at once than the `endless` partitions it follows
/// without end, which hold their places for as long as it runs.
pub(super) fn config(rng: &mut SplitMix64, streaming: bool, endless: usize) -> EngineConfig {
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
        builder.replan(Duration::from_millis(250))
    } else {
        builder
    };
    builder
        .memory(least + rng.below(least))
        .lanes(lanes)
        .lane_window(to_usize(1 + rng.below(3)))
        .partitions(to_usize(1 + rng.below(4)).max(endless + 1))
        .partition_buffer(to_usize(1 + rng.below(4)))
        .barrier_wait(Duration::from_millis(10 + rng.below(2000)))
        .commit(commit)
        .retry(retry)
        .build()
        .expect("the drawn configuration is valid")
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
    Pressure::draw(&mut rng, &config.limits(), cursors, partitions)
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(1)
}

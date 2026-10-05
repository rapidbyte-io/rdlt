//! The engine's configuration a simulation draws, and how hard its source presses on its budget.

use std::time::Duration;

use rdlt_connector::Checkpointing;
use rdlt_engine::{CommitPolicy, EngineConfig, GrowthLimits, RetryPolicy};

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
/// without end, which hold their places for as long as it runs, and holds to `growth`.
pub(super) fn config(
    rng: &mut SplitMix64,
    streaming: bool,
    endless: usize,
    growth: GrowthLimits,
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
        .growth(growth)
        .build()
        .expect("the drawn configuration is valid")
}

/// The growth limits of an engine of the world `seed`, drawn apart from the world, so every other
/// draw of the seed is as it was: in a quarter of worlds one to three destination writers open at
/// once, so lanes close the writers they wrote longest ago, and the defaults elsewhere.
///
/// Where `workload` keeps logs and every stream checkpoints as it reads, half its worlds' logs
/// hold 128 KiB to 1 MiB, less than many of their loads send: batches wait for commits to free
/// room. Half of those whose streams push plain Arrow keep a log only twice what their partitions
/// hold unsealed, whose frames are small beside their commits', so chunks gather and are freed.
pub(super) fn growth(seed: Seed, workload: Option<&Workload>) -> GrowthLimits {
    let mut rng = SplitMix64::new(seed.value().rotate_left(29));
    let defaults = GrowthLimits::default();
    let growth = if rng.chance(250) {
        let writers = to_usize(1 + rng.below(3));
        GrowthLimits::new(defaults.child_tables().get(), writers)
            .expect("the drawn limits are valid")
    } else {
        defaults
    };
    let natural = |workload: &Workload| {
        workload.features.wal
            && workload
                .streams
                .iter()
                .all(|stream| stream.checkpointing == Checkpointing::Natural)
    };
    let mut drawn = SplitMix64::new(seed.value().rotate_left(41));
    if workload.is_some_and(natural) && drawn.chance(500) {
        let bytes = (128 << 10) << drawn.below(4);
        // Drawn apart, so every other world's log is as it was.
        let mut sized = SplitMix64::new(seed.value().rotate_left(53));
        let bytes = match workload.and_then(tiny) {
            Some(tiny) if sized.chance(500) => tiny,
            _ => bytes,
        };
        return growth
            .with_log_bytes(bytes)
            .expect("a log holds some bytes");
    }
    growth
}

/// Bytes: what a batch frame of up to eight rows of a stream pushing plain Arrow, unpressed,
/// takes at most.
const PLAIN_FRAME: u64 = 4 << 10;

/// Bytes: a log of twice what `workload`'s partitions hold of batch frames they have not sealed,
/// and at least 16 KiB, where every stream pushes plain Arrow with no columns that drift; none
/// otherwise.
fn tiny(workload: &Workload) -> Option<u64> {
    let plain = workload
        .streams
        .iter()
        .all(|stream| !stream.json && stream.drift.is_empty());
    let open: u64 = workload
        .streams
        .iter()
        .map(|stream| to_u64(stream.partitions.len()).saturating_mul(stream.checkpoint_every))
        .sum();
    plain.then(|| open.saturating_mul(2 * PLAIN_FRAME).max(16 << 10))
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
/// `config`, drawn apart from the world, so every other draw of the seed is as it was; where the
/// engine keeps a `small_log`, its cursors carry no padding, and where it keeps a log smaller than
/// 128 KiB, its batches no ballast either, so its frames are small.
pub(super) fn pressed(
    seed: Seed,
    config: &EngineConfig,
    workload: &Workload,
    small_log: bool,
) -> Pressure {
    let mut rng = SplitMix64::new(seed.value().rotate_left(17));
    let cursors = config.memory().get() / 64;
    let partitions = workload
        .streams
        .iter()
        .map(|stream| stream.partitions.len());
    let partitions = u64::try_from(partitions.sum::<usize>()).unwrap_or(u64::MAX);
    let pressure = Pressure::draw(&mut rng, &config.limits(), cursors, partitions);
    // A cursor is recorded twice over in its seal's frame, and a small log holds no more than a
    // few seals of cursors as long as a budget allows: there cursors carry their offsets alone.
    if config.growth().log_bytes().get() < 128 << 10 {
        return Pressure::default();
    }
    if small_log {
        return Pressure { pad: 0, ..pressure };
    }
    pressure
}

/// Whether an engine of `config` keeps a log smaller than the default.
pub(super) fn small_log(config: &EngineConfig) -> bool {
    config.growth().log_bytes() < GrowthLimits::default().log_bytes()
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(1)
}

#[cfg(test)]
mod tests;

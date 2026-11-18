//! A churning source: while a streaming phase's runs follow it, it retires partitions of its
//! followed streams, adds others and names retired ones again; once they end, it names every
//! partition again, so the phase's last runs read every row.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::workload::Workload;
use crate::world::{Naming, World};

/// What a churning phase's draws mix into the seed: "named" in ASCII.
pub(super) const NAMING: u64 = 0x006e_616d_6564;

/// What the draw of the streams a churning source swaps mixes into the seed: "swap" in ASCII.
const SWAP: u64 = 0x7377_6170;

/// How often, per thousand ticks of arriving rows, the source retires or names a partition.
pub(super) const CHANGES: u64 = 333;

/// The followed unbounded streams, by index, whose source names fewer of their partitions than
/// all at once, each with how many: its changes swap a partition it names for one it does not.
pub(super) type Swapping = BTreeMap<usize, usize>;

/// The streams the source of the world `seed`, of `workload`, swaps, drawn from a generator of
/// their own, so every other draw of the seed is as it was: in half the churning worlds, each
/// followed unbounded stream of two partitions or more, naming one to one fewer than all of them
/// at once; none elsewhere.
pub(super) fn swapping(seed: Seed, workload: &Workload) -> Swapping {
    let mut rng = SplitMix64::new(seed.value() ^ SWAP);
    if !(workload.features.churn && rng.chance(500)) {
        return Swapping::new();
    }
    let streams = workload.streams.iter().enumerate();
    streams
        .filter(|(_, stream)| stream.follows && stream.unbounded && stream.partitions.len() > 1)
        .map(|(index, stream)| {
            let others = u64::try_from(stream.partitions.len() - 1).unwrap_or(1);
            (index, 1 + usize::try_from(rng.below(others)).unwrap_or(0))
        })
        .collect()
}

/// What the source names as a phase begins: every partition of its followed streams, but only as
/// many of the first of a stream `swapping` holds as it names at once; then one to three changes,
/// as [`change`] makes them.
pub(super) fn first(world: &World, swapping: &Swapping, rng: &mut SplitMix64) -> Naming {
    let mut naming: Naming = world
        .workload
        .streams
        .iter()
        .enumerate()
        .filter(|(_, stream)| stream.follows)
        .map(|(index, stream)| {
            let most = swapping.get(&index).copied().unwrap_or(usize::MAX);
            (index, (0..stream.partitions.len().min(most)).collect())
        })
        .collect();
    for _ in 0..=rng.below(3) {
        shift(world, swapping, &mut naming, rng);
    }
    naming
}

/// Retires a partition of a followed stream drawn from `rng` where `naming` names it, and names it
/// where it does not.
pub(super) fn toggle(world: &World, naming: &mut Naming, rng: &mut SplitMix64) {
    let streams: Vec<usize> = naming.keys().copied().collect();
    let Some(count) = u64::try_from(streams.len()).ok().filter(|count| *count > 0) else {
        return;
    };
    let stream = streams[usize::try_from(rng.below(count)).unwrap_or(0)];
    let partitions = u64::try_from(world.workload.streams[stream].partitions.len()).unwrap_or(1);
    let partition = usize::try_from(rng.below(partitions)).unwrap_or(0);
    let named = naming.entry(stream).or_default();
    if !named.remove(&partition) {
        named.insert(partition);
    }
}

/// Retires, in `naming`, a partition it names of a stream drawn from `rng` among those `swapping`
/// holds, and names one of the stream's it does not in its place, so it names as many as before.
fn swap(world: &World, swapping: &Swapping, naming: &mut Naming, rng: &mut SplitMix64) {
    let streams: Vec<usize> = swapping.keys().copied().collect();
    let Some(stream) = picked(rng, &streams) else {
        return;
    };
    let named = naming.entry(stream).or_default();
    let partitions = world.workload.streams[stream].partitions.len();
    let (kept, retired): (Vec<usize>, Vec<usize>) =
        (0..partitions).partition(|partition| named.contains(partition));
    if let (Some(out), Some(back)) = (picked(rng, &kept), picked(rng, &retired)) {
        named.remove(&out);
        named.insert(back);
    }
}

/// Changes what `world` names now: swaps a partition, drawn from `rng`, as [`swap`] does where
/// `swapping` holds any stream, and toggles one as [`toggle`] does otherwise.
pub(super) fn change(world: &World, swapping: &Swapping, rng: &mut SplitMix64) {
    let Some(mut naming) = world.named.naming() else {
        return;
    };
    shift(world, swapping, &mut naming, rng);
    world.named.name(Some(naming));
}

/// Changes `naming` as [`change`] changes what the world names.
fn shift(world: &World, swapping: &Swapping, naming: &mut Naming, rng: &mut SplitMix64) {
    if swapping.is_empty() {
        toggle(world, naming, rng);
    } else {
        swap(world, swapping, naming, rng);
    }
}

/// One of `items`, drawn from `rng`; none where there are none.
fn picked(rng: &mut SplitMix64, items: &[usize]) -> Option<usize> {
    let count = u64::try_from(items.len()).ok().filter(|count| *count > 0)?;
    items.get(usize::try_from(rng.below(count)).ok()?).copied()
}

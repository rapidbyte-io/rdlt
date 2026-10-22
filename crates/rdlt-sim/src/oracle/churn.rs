//! A churning source: while a streaming phase's runs follow it, it retires partitions of its
//! followed streams, adds others and names retired ones again; once they end, it names every
//! partition again, so the phase's last runs read every row.

#[cfg(test)]
mod tests;

use crate::rng::SplitMix64;
use crate::world::{Naming, World};

/// What a churning phase's draws mix into the seed: "named" in ASCII.
pub(super) const NAMING: u64 = 0x006e_616d_6564;

/// How often, per thousand ticks of arriving rows, the source retires or names a partition.
pub(super) const CHANGES: u64 = 333;

/// What the source names as a phase begins: every partition of its followed streams, then one to
/// three of them retired or named again.
pub(super) fn first(world: &World, rng: &mut SplitMix64) -> Naming {
    let mut naming: Naming = world
        .workload
        .streams
        .iter()
        .enumerate()
        .filter(|(_, stream)| stream.follows)
        .map(|(index, stream)| (index, (0..stream.partitions.len()).collect()))
        .collect();
    for _ in 0..=rng.below(3) {
        toggle(world, &mut naming, rng);
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

/// Retires or names, in what `world` names now, a partition [`toggle`] draws from `rng`.
pub(super) fn change(world: &World, rng: &mut SplitMix64) {
    let Some(mut naming) = world.named.naming() else {
        return;
    };
    toggle(world, &mut naming, rng);
    world.named.name(Some(naming));
}

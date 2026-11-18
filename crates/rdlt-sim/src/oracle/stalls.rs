//! Stalls inside a following run: a partition its plans name that no read takes up, or one they
//! dropped whose checkpoints no commit takes, for longer than the engine's timers allow.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{PartitionId, StreamName};
use rdlt_engine::{CommitPolicy, EngineConfig};
use tokio::time::Instant;

use crate::destination::committed_next;
use crate::source::Plans;
use crate::world::World;

/// How long a clean following run of `config` in the engine's process may leave a partition its
/// source names without a read, or one its plans dropped short of what its reads checkpointed.
///
/// A read's end waits for a commit, its barrier answered: the policy's interval, or a re-plan's
/// where it has none. A re-plan then reads the partition again, and the read may wait for a slot
/// behind another whose cursor waits for a commit of its own.
pub(super) fn bound(config: &EngineConfig) -> Duration {
    let every = config
        .commit()
        .and_then(CommitPolicy::every)
        .unwrap_or_default();
    config.replan() * 3 + every + config.barrier_wait() * 2
}

/// The stalls of the runs whose reads `connections` hold, by `now`, each partition of a stream
/// `world`'s source follows held to `bound`: a partition waits for a read from its plan's answer,
/// or from when the source named it where no plan named it since.
pub(super) fn stalls(
    world: &World,
    connections: &[Arc<Plans>],
    now: Instant,
    bound: Duration,
) -> Vec<String> {
    let streams = &world.workload.streams;
    let follows = |stream: usize| streams[stream].follows;
    let held = |stream: usize, partition: usize| {
        let name = StreamName::new(&streams[stream].name).expect("simulated names are valid");
        let id = PartitionId::parse(format!("p{partition}")).expect("valid partition id");
        committed_next(world, &name, &id)
    };
    let named = world.named.since();
    let mut stalls = Vec::new();
    for plans in connections {
        for ((stream, partition), waited) in unread(&plans.waits(now, &named), follows, bound) {
            // A done partition is read no more.
            if held(stream, partition) == Some(u64::MAX) {
                continue;
            }
            stalls.push(format!(
                "stream {} partition p{partition}: its source named it, and it waited {waited:?} \
                 for a read, beyond the {bound:?} a following run allows",
                streams[stream].name
            ));
        }
        let gone = plans.gone();
        for ((stream, partition), taken, at) in undrained(&gone, follows, (now, bound), held) {
            stalls.push(format!(
                "stream {} partition p{partition}: longer than {bound:?} after its run's plans \
                 stopped naming it, the engine had taken its read's checkpoint at {taken}, and \
                 the destination held it at {at:?}",
                streams[stream].name
            ));
        }
    }
    stalls
}

/// Of the partitions `waits` holds, those of streams that `follows` that waited longer than
/// `bound` for a read.
fn unread(
    waits: &BTreeMap<(usize, usize), Duration>,
    follows: impl Fn(usize) -> bool,
    bound: Duration,
) -> Vec<((usize, usize), Duration)> {
    waits
        .iter()
        .filter(|((stream, _), waited)| follows(*stream) && **waited > bound)
        .map(|(key, waited)| (*key, *waited))
        .collect()
}

/// Of the partitions `gone` holds, those of streams that `follows` that their plans stopped naming
/// longer than `bound` before `now`, and that `held` holds short of the checkpoint taken from a
/// read of them; a partition held nowhere is held at its beginning.
fn undrained(
    gone: &[((usize, usize), Instant, u64)],
    follows: impl Fn(usize) -> bool,
    (now, bound): (Instant, Duration),
    held: impl Fn(usize, usize) -> Option<u64>,
) -> Vec<((usize, usize), u64, Option<u64>)> {
    gone.iter()
        .filter(|((stream, _), since, _)| {
            follows(*stream) && now.saturating_duration_since(*since) > bound
        })
        .filter_map(|&((stream, partition), _, taken)| {
            let at = held(stream, partition);
            (at.unwrap_or(0) < taken).then_some(((stream, partition), taken, at))
        })
        .collect()
}

#[cfg(test)]
mod tests;

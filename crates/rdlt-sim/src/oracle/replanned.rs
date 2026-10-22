//! Each run's reads held to its connections' plans, by the invariants of re-planning they check:
//! no partition read twice at once, no read of a partition its run stopped naming that goes on,
//! and what a dropped partition's reads had taken committed once its run succeeded.

#[cfg(test)]
mod tests;

use rdlt_connector::{PartitionId, StreamName};
use rdlt_engine::RunStatus;

use super::Simulation;
use super::scenario::{Executed, Scenario};
use crate::destination::committed_next;
use crate::reached::Reach;
use crate::source::{Findings, Plans};
use crate::world::World;

impl Simulation {
    /// Holds the reads of the runs of `scenario` that ended as `executed` to their connections'
    /// plans.
    pub(super) fn held_to_plans(&self, scenario: Scenario, executed: &[Executed]) {
        let connections = self.world.connections();
        if connections.iter().any(|plans| plans.findings().replanned) {
            self.world.reach(Reach::Replanned);
        }
        // A read placed apart ends only once its stop crosses to it, and sends into what the wire
        // holds meanwhile: only the engine's own process holds a read to its run's plans.
        if self.placing.is_some() {
            return;
        }
        for plans in &connections {
            let findings = plans.findings();
            no_partition_lost_or_doubled(&self.world, &findings);
            // Between the source's answer and the engine's stop another thread's reads go on
            // for as long as the scheduler pauses the thread between them, which no bound holds:
            // only the single thread has nothing between the two.
            if !self.threaded {
                plans_follow_the_source(&self.world, &findings, self.buffer);
            }
        }
        // An attempt that failed leaves seals for the next, so only a run that succeeded in one
        // attempt has committed every seal its reads made.
        let alone = matches!(scenario, Scenario::Plain)
            && executed
                .iter()
                .flat_map(|executed| &executed.reports)
                .all(|report| report.status == RunStatus::Succeeded && report.attempted == 1);
        if alone {
            for plans in &connections {
                removed_drains(&self.world, plans);
            }
        }
    }
}

/// Fails `world` where one connection read a partition twice at once.
fn no_partition_lost_or_doubled(world: &World, findings: &Findings) {
    for (stream, partition) in &findings.doubled {
        world.violation(format!(
            "stream {} partition p{partition}: two reads of it ran at once in one run",
            name(world, *stream)
        ));
    }
}

/// Fails `world` where a read went on once its run's plan stopped naming its partition: beyond the
/// `buffer` events its channel holds, and a send in flight.
fn plans_follow_the_source(world: &World, findings: &Findings, buffer: usize) {
    for ((stream, partition), sent) in late(findings, buffer) {
        world.violation(format!(
            "stream {} partition p{partition}: a read sent {sent} pushes and checkpoints once its \
             run's plan stopped naming the partition, beyond a channel of {buffer} and one more",
            name(world, stream)
        ));
    }
}

/// The partitions of `findings` one read of which sent more, once a plan stopped naming the
/// partition, than a channel of `buffer` events holds and one in flight.
fn late(findings: &Findings, buffer: usize) -> Vec<((usize, usize), u64)> {
    let most = u64::try_from(buffer).unwrap_or(u64::MAX).saturating_add(1);
    findings
        .late
        .iter()
        .filter(|(_, sent)| **sent > most)
        .map(|(key, sent)| (*key, *sent))
        .collect()
}

/// Fails `world` where the destination holds a partition a plan stopped naming short of a
/// checkpoint the engine took from a read of it.
fn removed_drains(world: &World, plans: &Plans) {
    let held = |stream: usize, partition: usize| {
        let named = StreamName::new(name(world, stream)).expect("simulated stream names are valid");
        let id = PartitionId::parse(format!("p{partition}")).expect("valid partition id");
        committed_next(world, &named, &id)
    };
    for ((stream, partition), taken, held) in undrained(&plans.drained(), held) {
        world.violation(format!(
            "stream {} partition p{partition}: once its run's plan stopped naming it, the engine \
             took its read's checkpoint at {taken}, and the destination holds it at {held:?} once \
             the run succeeded",
            name(world, stream)
        ));
    }
}

/// Of the furthest checkpoints `drained` names, those `held` holds their partition short of; a
/// partition held nowhere is held at its beginning.
fn undrained(
    drained: &[((usize, usize), u64)],
    held: impl Fn(usize, usize) -> Option<u64>,
) -> Vec<((usize, usize), u64, Option<u64>)> {
    drained
        .iter()
        .filter_map(|&((stream, partition), taken)| {
            let at = held(stream, partition);
            (at.unwrap_or(0) < taken).then_some(((stream, partition), taken, at))
        })
        .collect()
}

/// The name of `world`'s stream `stream`.
fn name(world: &World, stream: usize) -> &str {
    &world.workload.streams[stream].name
}

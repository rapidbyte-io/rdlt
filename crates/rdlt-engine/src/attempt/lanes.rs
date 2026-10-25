//! The attempt's lanes: how many it runs, the writers they hold open, and their tasks.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::Destination;

use super::RunContext;
use crate::config::EngineConfig;
use crate::error::Error;
use crate::lane::Lanes;
use crate::limits;
use crate::scope::TaskScope;
use crate::table::Tables;

/// The attempt's lanes, their tasks started in `scope`.
pub(super) fn start_lanes(
    context: &RunContext,
    tables: &Arc<Tables>,
    scope: &mut TaskScope<Error>,
) -> Lanes {
    let count = lane_count(
        &context.config,
        context.env.cores(),
        context.destination.as_ref(),
    );
    let writers = held_writers(&context.config, context.destination.as_ref());
    let (lanes, tasks) = Lanes::new(
        (count, writers),
        tables,
        context.config.lane_window(),
        &context.budget,
        (&context.env, &context.tally),
    );
    let cancel = scope.token().clone();
    // A change of a table's schema reserves what its commit records, until the attempt ends.
    tables.charge(context.budget.clone(), cancel.clone());
    for lane in tasks {
        scope.spawn(lane.run(cancel.clone()));
    }
    lanes
}

/// The configured lanes, or one per core of `cores`, never more than the destination's parallel
/// writers, nor than the writers an attempt holds open: each lane holds one at least.
fn lane_count(
    config: &EngineConfig,
    cores: NonZeroUsize,
    destination: &dyn Destination,
) -> NonZeroUsize {
    let limit = usize::from(destination.capabilities().max_parallel_writers.get());
    let lanes = config
        .lanes()
        .map_or(cores.get(), |lanes| usize::from(lanes.get()));
    let lanes = lanes
        .min(limit)
        .min(held_writers(config, destination).get());
    NonZeroUsize::new(lanes).unwrap_or(NonZeroUsize::MIN)
}

/// The writers an attempt holds open at once: the growth limit's, or fewer where `destination`
/// carries fewer calls at once beside a session's other call.
pub(super) fn held_writers(config: &EngineConfig, destination: &dyn Destination) -> NonZeroUsize {
    let writers = config.growth().writers();
    destination
        .max_calls()
        .map_or(writers, |calls| writers.min(limits::carried(calls)))
}

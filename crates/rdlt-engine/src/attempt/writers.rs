//! The attempt's writers: how many it runs, the table writers they hold open, and their tasks.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::Destination;

use super::RunContext;
use crate::config::EngineConfig;
use crate::error::Error;
use crate::limits;
use crate::scope::TaskScope;
use crate::table::Tables;
use crate::writer::Writers;

/// The attempt's writers, their tasks started in `scope`.
pub(super) fn start_writers(
    context: &RunContext,
    tables: &Arc<Tables>,
    scope: &mut TaskScope<Error>,
) -> Writers {
    let count = writer_count(
        &context.config,
        context.env.cores(),
        context.destination.as_ref(),
    );
    let table_writers = held_table_writers(&context.config, context.destination.as_ref());
    let (writers, tasks) = Writers::new(
        (count, table_writers),
        tables,
        context.config.writer_window(),
        &context.budget,
        (&context.env, &context.tally),
    );
    context
        .live
        .writers(Arc::clone(writers.calls()), writers.queues());
    let cancel = scope.token().clone();
    // A change of a table's schema reserves what its commit records, until the attempt ends.
    tables.charge(context.budget.clone(), cancel.clone());
    for writer in tasks {
        scope.spawn(writer.run(cancel.clone()));
    }
    writers
}

/// The configured writers, or one per core of `cores`, never more than the destination runs at
/// once, nor than the table writers an attempt holds open: each writer holds one at least.
fn writer_count(
    config: &EngineConfig,
    cores: NonZeroUsize,
    destination: &dyn Destination,
) -> NonZeroUsize {
    let limit = usize::from(destination.capabilities().max_parallel_writers.get());
    let writers = config
        .writers()
        .map_or(cores.get(), |writers| usize::from(writers.get()));
    let writers = writers
        .min(limit)
        .min(held_table_writers(config, destination).get());
    NonZeroUsize::new(writers).unwrap_or(NonZeroUsize::MIN)
}

/// The table writers an attempt holds open at once: the growth limit's, or fewer where
/// `destination` carries fewer calls at once beside a session's other call.
pub(super) fn held_table_writers(
    config: &EngineConfig,
    destination: &dyn Destination,
) -> NonZeroUsize {
    let table_writers = config.growth().table_writers();
    destination.max_calls().map_or(table_writers, |calls| {
        table_writers.min(limits::carried(calls))
    })
}

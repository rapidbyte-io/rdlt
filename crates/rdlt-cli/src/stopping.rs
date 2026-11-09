//! How the connectors a process spawned are stopped as it ends: within a bound, and at once for a
//! signal that comes while they stop.

#[cfg(test)]
mod tests;

use std::time::Duration;

use rdlt::{Interrupts, StopsSpawned};
use rdlt_host::Lingering;
use tokio::runtime::Runtime;

/// How long the connectors a process spawned have to stop, each with its group, once it has
/// ended: their grace, and what seeing a killed group empty takes.
pub(crate) const STOPPING: Duration = Duration::from_secs(20);

/// Stops what the process spawned, through `stops`, on `signals`, and answers what lingers and
/// the exit status of the last signal heard, `heard` being that of a signal that ended the work.
///
/// A signal heard while its connectors stop is its second once one was heard: what it spawned is
/// then killed at once.
pub(crate) fn stopped(
    signals: &Runtime,
    stops: StopsSpawned,
    interrupts: &mut Interrupts,
    heard: Option<i32>,
) -> (Result<(), Lingering>, Option<i32>) {
    signals.block_on(async {
        let mut stopping = tokio::task::spawn_blocking(move || stops.stop());
        let mut heard = heard;
        loop {
            tokio::select! {
                biased;
                // Stopped connectors go first: a signal at the same moment comes too late.
                stopped = &mut stopping => return (stopped.unwrap_or(Ok(())), heard),
                status = interrupts.heard() => {
                    if heard.replace(status).is_some() {
                        // No patience is left: every group is killed, and seen to be.
                        let killed = rdlt_host::stop_spawned(Duration::ZERO);
                        let stopped = stopping.await.unwrap_or(Ok(()));
                        return (both(stopped, killed), heard);
                    }
                }
            }
        }
    })
}

/// What two stops of the same groups answer together: every group either names.
pub(crate) fn both(
    first: Result<(), Lingering>,
    second: Result<(), Lingering>,
) -> Result<(), Lingering> {
    let mut groups: Vec<u32> = [first, second]
        .into_iter()
        .filter_map(Result::err)
        .flat_map(|lingering| lingering.groups)
        .collect();
    groups.sort_unstable();
    groups.dedup();
    if groups.is_empty() {
        return Ok(());
    }
    Err(Lingering { groups })
}

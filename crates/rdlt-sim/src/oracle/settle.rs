//! Settling after a run: its aborted tasks are let finish, and none may outlive it, nor anything
//! its connectors held for it.

#[cfg(test)]
mod tests;

use std::time::Duration;

use rdlt_connector::limits::ListenLimits;
use tokio::runtime::Handle;
use tokio::time::Instant;

use crate::network::Placing;
use crate::seed::Seed;
use crate::world::World;

/// How long a task may go on once its run ended: one still alive then outlived its run.
const OUTLIVED: Duration = Duration::from_secs(1);

/// How often settling looks again for a task still alive: a step of the simulated network.
const LOOK: Duration = Duration::from_millis(1);

/// Lets aborted tasks finish, ending once none is left.
///
/// # Panics
///
/// Panics, naming the seed, when a task is still alive [`OUTLIVED`] after the run ended.
pub(super) async fn settle(seed: Seed) {
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    let started = Instant::now();
    loop {
        let alive = Handle::current().metrics().num_alive_tasks();
        if alive == 0 {
            return;
        }
        assert!(
            started.elapsed() < OUTLIVED,
            "seed {seed}: {alive} tasks outlived their runs"
        );
        tokio::time::sleep(LOOK).await;
    }
}

/// How long a connector's host may run a task, or its connectors hold a session, writer or read,
/// once the engine's tasks have ended.
///
/// A connection whose host vanished behind a partition ends by the connector's own limits: its
/// pings go unanswered for the connector's keep-alive interval and patience, 35 s together, the
/// `PINGS` below, and the writes that close it then take none of their bytes for as long as the
/// connector's send limit, which also keeps the pings from being sent. The bound is those and a
/// margin.
fn served_outlived() -> Duration {
    const PINGS: Duration = Duration::from_secs(35);
    PINGS + ListenLimits::default().send + Duration::from_secs(10)
}

/// The tasks `hosts` run now.
fn alive(hosts: &[Handle]) -> usize {
    hosts
        .iter()
        .map(|host| host.metrics().num_alive_tasks())
        .sum()
}

/// Lets the connectors release what they held for the runs that ended, ending once `hosts`, the
/// connectors' hosts where they have their own, run no task and `world`'s connectors hold no
/// session, writer or read.
///
/// # Panics
///
/// Panics, naming the seed, when any is still alive [`served_outlived`] after it began.
pub(super) async fn released(seed: Seed, world: &World, hosts: &[Handle]) {
    let started = Instant::now();
    loop {
        let (tasks, held) = (alive(hosts), world.owners.held());
        if tasks == 0 && held.is_empty() {
            return;
        }
        assert!(
            started.elapsed() < served_outlived(),
            "seed {seed}: {tasks} tasks on the connectors' hosts, and {held}, outlived their runs"
        );
        tokio::time::sleep(LOOK).await;
    }
}

/// Settles as [`settle`] does, then lets the connectors release what they held, as [`released`]
/// does, on `placing`'s hosts where it places them on a network.
pub(super) async fn settled(seed: Seed, world: &World, placing: Option<&Placing>) {
    settle(seed).await;
    let hosts = placing.map(Placing::hosts).unwrap_or_default();
    released(seed, world, &hosts).await;
}

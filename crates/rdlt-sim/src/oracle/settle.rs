//! Settling after a run: its aborted tasks are let finish, and none may outlive it.

#[cfg(test)]
mod tests;

use std::time::Duration;

use tokio::time::Instant;

use crate::seed::Seed;

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
        let alive = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
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

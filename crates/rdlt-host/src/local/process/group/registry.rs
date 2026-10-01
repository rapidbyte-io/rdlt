//! Every group this process spawned and has not seen end, and what stops them all.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use super::{Held, WATCH};

/// Every group this process spawned and has not seen end, by its leader's process id, with
/// what stops it; and the groups that ended with members remaining.
#[derive(Default)]
struct Groups {
    live: BTreeMap<u32, Arc<Held>>,
    remaining: Vec<u32>,
}

static GROUPS: Mutex<Option<Groups>> = Mutex::new(None);

/// Signalled when a group ends.
static ENDED: Condvar = Condvar::new();

fn groups() -> MutexGuard<'static, Option<Groups>> {
    GROUPS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Lists the group `id` names, which `held` stops, as one this process spawned.
pub(super) fn enter(id: u32, held: Arc<Held>) {
    groups().get_or_insert_default().live.insert(id, held);
}

/// Unlists the group `id` names, which ended, or was never owned: one that ended and was not
/// seen `emptied` is kept as remaining.
pub(super) fn leave(id: u32, emptied: bool) {
    let mut groups = groups();
    let groups = groups.get_or_insert_default();
    groups.live.remove(&id);
    if !emptied {
        tracing::error!(group = id, "a connector's process group kept a member");
        groups.remaining.push(id);
    }
    ENDED.notify_all();
}

/// The process ids of the connectors this process spawned and has not seen end, each the id of
/// the process group its connector leads.
pub fn spawned() -> Vec<u32> {
    let groups = groups();
    groups
        .as_ref()
        .map(|groups| groups.live.keys().copied().collect())
        .unwrap_or_default()
}

/// Connector process groups a host could not see stopped.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("connector process groups {groups:?} were not seen to end")]
pub struct Lingering {
    /// The groups' ids, each its connector's process id: those still stopping when the wait
    /// ended, and those that kept a member after they were killed.
    pub groups: Vec<u32>,
}

/// Stops every connector this process spawned, each with its whole process group, and waits
/// up to `patience` for them to end: what a host calls before it exits, and when it is
/// interrupted.
///
/// Each connector is stopped as dropping it stops it: `SIGTERM` to its group, `SIGKILL` once
/// its grace has passed, and its group seen empty. It blocks, so call it outside a runtime, or
/// on a thread that may block.
///
/// # Errors
///
/// [`Lingering`] names the groups not seen to end within `patience`, and those that kept a
/// member after they were killed.
pub fn stop_spawned(patience: Duration) -> Result<(), Lingering> {
    let until = Instant::now().checked_add(patience);
    let mut guard = groups();
    let groups = guard.get_or_insert_default();
    for held in groups.live.values() {
        held.stop();
    }
    loop {
        let groups = guard.get_or_insert_default();
        let left = until.map_or(WATCH, |until| {
            until.saturating_duration_since(Instant::now())
        });
        if groups.live.is_empty() || left.is_zero() {
            let mut lingering: Vec<u32> = groups.live.keys().copied().collect();
            lingering.append(&mut groups.remaining);
            lingering.sort_unstable();
            if lingering.is_empty() {
                return Ok(());
            }
            return Err(Lingering { groups: lingering });
        }
        let waited = ENDED.wait_timeout(guard, left);
        guard = waited.unwrap_or_else(PoisonError::into_inner).0;
    }
}

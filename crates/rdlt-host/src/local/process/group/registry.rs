//! Every group this process spawned and has not seen end, and what stops them all.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use super::{EMPTYING, Held, WATCH};

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
    /// The groups' ids, each its connector's process id: those not seen to end once killed, and
    /// those that kept a living member after they were.
    pub groups: Vec<u32>,
}

/// How long the groups a stop kills at the end of its patience have to be seen to end.
const KILLED: Duration = EMPTYING.saturating_add(Duration::from_secs(1));

/// Stops every connector this process spawned, each with its whole process group, and waits
/// for them to end: what a host calls before it exits, and when it is interrupted.
///
/// Each connector is stopped as dropping it stops it: `SIGTERM` to its group, `SIGKILL` once
/// its grace has passed, and its group seen to have no living member. A group still stopping
/// once `patience` has passed is killed then, whatever is left of its grace, so nothing this
/// process spawned outlives the call by its grace: no patience kills at once. It blocks, for
/// `patience` and at most six seconds more, so call it outside a runtime, or on a thread that
/// may block.
///
/// # Errors
///
/// [`Lingering`] names the groups not seen to end once killed, and those that kept a living
/// member after they were.
pub fn stop_spawned(patience: Duration) -> Result<(), Lingering> {
    let mut guard = groups();
    for held in guard.get_or_insert_default().live.values() {
        held.stop();
    }
    guard = ended(guard, patience);
    for held in guard.get_or_insert_default().live.values() {
        held.kill();
    }
    guard = ended(guard, KILLED);
    let groups = guard.get_or_insert_default();
    let mut lingering: Vec<u32> = groups.live.keys().copied().collect();
    lingering.append(&mut groups.remaining);
    lingering.sort_unstable();
    if lingering.is_empty() {
        return Ok(());
    }
    Err(Lingering { groups: lingering })
}

/// Waits until every group has ended, for `patience` at most.
fn ended(
    mut guard: MutexGuard<'static, Option<Groups>>,
    patience: Duration,
) -> MutexGuard<'static, Option<Groups>> {
    let until = Instant::now().checked_add(patience);
    loop {
        let left = until.map_or(WATCH, |until| {
            until.saturating_duration_since(Instant::now())
        });
        if guard.get_or_insert_default().live.is_empty() || left.is_zero() {
            return guard;
        }
        let waited = ENDED.wait_timeout(guard, left);
        guard = waited.unwrap_or_else(PoisonError::into_inner).0;
    }
}

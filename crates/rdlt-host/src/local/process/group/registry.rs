//! Every group this process spawned and has not seen end, and what stops them all.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::{EMPTYING, Held, WATCH};

/// Every group this process spawned and has not seen end, by its leader's process id, with
/// what stops it; and the groups that ended with members remaining.
#[derive(Default)]
struct Groups {
    live: BTreeMap<u32, Arc<Held>>,
    /// The thread that owns each group, until it is joined once the group has ended.
    threads: BTreeMap<u32, JoinHandle<()>>,
    remaining: Vec<u32>,
}

/// The groups a process owns at once, at most: each has a thread, which wakes every few
/// milliseconds for as long as its connector runs.
pub(super) const OWNED: usize = 1024;

/// The groups that ended with members remaining a process keeps to report, at most.
const REMAINING: usize = 1024;

/// Refuses a group more than a process owns, of the `owned` it does.
fn room(owned: usize) -> std::io::Result<()> {
    if owned < OWNED {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "this process owns {owned} connectors, as many as it may at once"
    )))
}

/// Keeps `id` among the `remaining`, the latest [`REMAINING`] of them.
fn remember(remaining: &mut Vec<u32>, id: u32) {
    if remaining.len() >= REMAINING {
        remaining.remove(0);
    }
    remaining.push(id);
}

/// Whether this process may own a group more.
///
/// # Errors
///
/// It owns as many as it may: [`OWNED`].
pub(in crate::local::process) fn has_room() -> std::io::Result<()> {
    room(groups().get_or_insert_default().live.len())
}

/// Joins the threads of the groups that have ended, each of which is past its last use of
/// what this holds.
fn join(mut guard: MutexGuard<'static, Option<Groups>>) {
    let groups = guard.get_or_insert_default();
    let ended: Vec<u32> = groups
        .threads
        .keys()
        .filter(|id| !groups.live.contains_key(id))
        .copied()
        .collect();
    let threads: Vec<JoinHandle<()>> = ended
        .iter()
        .filter_map(|id| groups.threads.remove(id))
        .collect();
    drop(guard);
    for thread in threads {
        thread.join().ok();
    }
}

/// How many threads own a group or are yet to be joined.
#[cfg(test)]
pub(in crate::local::process) fn threads() -> usize {
    groups().get_or_insert_default().threads.len()
}

static GROUPS: Mutex<Option<Groups>> = Mutex::new(None);

/// Signalled when a group ends.
static ENDED: Condvar = Condvar::new();

fn groups() -> MutexGuard<'static, Option<Groups>> {
    GROUPS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Lists the group `id` names, which `held` stops and `thread` owns, as one this process
/// spawned; the threads of groups that have ended are joined.
pub(super) fn enter(id: u32, held: Arc<Held>, thread: JoinHandle<()>) {
    let mut guard = groups();
    let groups = guard.get_or_insert_default();
    groups.live.insert(id, held);
    groups.threads.insert(id, thread);
    join(guard);
}

/// Unlists the group `id` names, whose thread never had it, and joins the thread.
pub(super) fn disown(id: u32) {
    let mut guard = groups();
    guard.get_or_insert_default().live.remove(&id);
    ENDED.notify_all();
    join(guard);
}

/// Unlists the group `id` names, which ended: one that was not seen `emptied` is kept as
/// remaining.
pub(super) fn leave(id: u32, emptied: bool) {
    let mut groups = groups();
    let groups = groups.get_or_insert_default();
    groups.live.remove(&id);
    if !emptied {
        tracing::error!(group = id, "a connector's process group kept a member");
        remember(&mut groups.remaining, id);
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
    join(guard);
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

/// Stops what this process spawned when it is dropped, a panic's unwinding included: what a
/// host holds for as long as it runs, so that it leaves nothing it spawned however it ends.
///
/// A host that ends in order calls [`stop`](Self::stop), and learns what lingers. What ends a
/// process without unwinding it, a signal nobody hears or an abort, drops nothing.
#[derive(Debug)]
#[must_use = "what it spawned is stopped when this is dropped"]
pub struct StopsSpawned {
    patience: Duration,
}

impl StopsSpawned {
    /// What stops every connector this process spawned, as [`stop_spawned`] does within
    /// `patience`, once it is dropped.
    pub fn within(patience: Duration) -> Self {
        Self { patience }
    }

    /// Stops every connector this process spawned now, as [`stop_spawned`] does.
    ///
    /// # Errors
    ///
    /// [`Lingering`], as [`stop_spawned`] answers it.
    pub fn stop(self) -> Result<(), Lingering> {
        let stopped = stop_spawned(self.patience);
        std::mem::forget(self);
        stopped
    }
}

impl Drop for StopsSpawned {
    fn drop(&mut self) {
        // Dropped without being asked: nobody is left to tell what lingers but the log.
        if let Err(lingering) = stop_spawned(self.patience) {
            tracing::error!("{lingering}");
        }
    }
}

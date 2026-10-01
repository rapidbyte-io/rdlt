//! A connector's process group, owned by the thread that reaps its leader.
//!
//! A group's id is its leader's process id, and belongs to no other process while the leader
//! is unreaped. So the thread sees the leader exit without reaping it, signals the group while
//! the id is still its own, and only then reaps. Once the leader is reaped the group is never
//! signalled again: it is only asked, with the null signal, whether any member is left.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::process::{Child, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use rustix::process::{
    Pid, Signal, WaitId, WaitIdOptions, WaitIdStatus, kill_process, kill_process_group,
    test_kill_process_group, waitid,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// How often the thread that owns a group asks whether its leader has exited, or a stop or a
/// kill was asked.
const WATCH: Duration = Duration::from_millis(5);

/// How long a group killed with `SIGKILL` has to be seen empty before its members are
/// reported as remaining: a process killed so ends at once, unless the kernel holds it.
const EMPTYING: Duration = Duration::from_secs(5);

/// Every group this process spawned and has not seen end, by its leader's process id, with
/// what stops it; and the groups that ended with members remaining.
#[derive(Default)]
struct Groups {
    live: BTreeMap<u32, Arc<AtomicBool>>,
    remaining: Vec<u32>,
}

static GROUPS: Mutex<Option<Groups>> = Mutex::new(None);

/// Signalled when a group ends.
static ENDED: Condvar = Condvar::new();

fn groups() -> std::sync::MutexGuard<'static, Option<Groups>> {
    GROUPS.lock().unwrap_or_else(PoisonError::into_inner)
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
    for stop in groups.live.values() {
        stop.store(true, Ordering::SeqCst);
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

/// A connector's process and the group it leads, with what stops and kills them.
pub(super) struct Owned {
    pub(super) child: Child,
    /// How long the group has to end, once stopped, before it is killed.
    pub(super) grace: Duration,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) killed: Option<CancellationToken>,
    /// Told the leader's exit, once its group is empty.
    pub(super) exit: watch::Sender<Option<ExitStatus>>,
}

impl Owned {
    /// Hands the group to a thread of its own, which stops, kills and reaps it, whatever
    /// becomes of the runtime that spawned it.
    pub(super) fn reaped(mut self) -> std::io::Result<()> {
        let id = self.child.id();
        groups()
            .get_or_insert_default()
            .live
            .insert(id, Arc::clone(&self.stop));
        let reaping = std::thread::Builder::new().name(format!("rdlt-reap-{id}"));
        let reaped = reaping.spawn(move || {
            let (status, emptied) = self.ended();
            let mut groups = groups();
            let groups = groups.get_or_insert_default();
            groups.live.remove(&id);
            if !emptied {
                tracing::error!(group = id, "a connector's process group kept a member");
                groups.remaining.push(id);
            }
            self.exit.send_replace(status);
            ENDED.notify_all();
        });
        reaped.map(drop)
    }

    /// Owns the group until it has ended: the leader's exit, and whether the group was seen
    /// empty after it was killed.
    fn ended(&mut self) -> (Option<ExitStatus>, bool) {
        let Some(group) = i32::try_from(self.child.id()).ok().and_then(Pid::from_raw) else {
            return (self.child.wait().ok(), true);
        };
        let mut stdin = self.child.stdin.take();
        let mut killing: Option<Instant> = None;
        // Until the leader has exited, a kill has come, or a stop's grace has passed.
        loop {
            match Leader::of(&asked(group)) {
                Leader::Running => {}
                Leader::Exited => break,
                // Its id may be another's by now: nothing is signalled, and nothing waited for.
                Leader::Lost => {
                    tracing::error!(group = self.child.id(), "a connector was reaped elsewhere");
                    return (None, emptied(group));
                }
            }
            if self
                .killed
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                break;
            }
            if killing.is_none() && self.stop.load(Ordering::SeqCst) {
                // The end of its standard input and the signal both ask a connector to stop.
                drop(stdin.take());
                signal(group, Signal::TERM);
                killing = Instant::now().checked_add(self.grace);
            }
            if killing.is_some_and(|killing| Instant::now() >= killing) {
                break;
            }
            std::thread::sleep(WATCH);
        }
        // The leader is unreaped, exited or not: the group's id is still its own. Whatever
        // is left of the group ends now, the connector's own members with it.
        signal(group, Signal::KILL);
        let status = self.child.wait().ok();
        (status, emptied(group))
    }
}

/// Sends `signal` to the group `leader` was started to lead, and to `leader` itself, which
/// may have left it for another: only while `leader` is this process's unreaped child, so that
/// both ids are its own.
fn signal(leader: Pid, signal: Signal) {
    kill_process_group(leader, signal).ok();
    kill_process(leader, signal).ok();
}

/// What a leader is to this process, asked without reaping it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Leader {
    /// A child of this process that has not exited: its id is its own.
    Running,
    /// A child of this process that exited and is unreaped: its id is still its own.
    Exited,
    /// No child of this process any longer: something else reaped it, as a process that
    /// ignores `SIGCHLD` or waits for any child does, and its id may be another's.
    Lost,
}

impl Leader {
    fn of(answer: &rustix::io::Result<Option<WaitIdStatus>>) -> Self {
        match answer {
            Ok(None) => Self::Running,
            Ok(Some(_)) => Self::Exited,
            Err(_) => Self::Lost,
        }
    }
}

/// Asks whether the leader `group` names has exited, without reaping it.
fn asked(group: Pid) -> rustix::io::Result<Option<WaitIdStatus>> {
    let unreaped = WaitIdOptions::EXITED | WaitIdOptions::NOWAIT | WaitIdOptions::NOHANG;
    waitid(WaitId::Pid(group), unreaped)
}

/// Whether `group`, killed and its leader reaped, is seen empty within [`EMPTYING`].
///
/// The null signal sends nothing: it asks whether a member is left.
fn emptied(group: Pid) -> bool {
    let until = Instant::now() + EMPTYING;
    while test_kill_process_group(group).is_ok() {
        if Instant::now() >= until {
            return false;
        }
        std::thread::sleep(WATCH);
    }
    true
}

/// Hears this process being interrupted or asked to terminate (`SIGINT`, `SIGTERM`).
///
/// A host that spawns connectors owns their process groups, which a terminal's Ctrl-C does not
/// reach: it listens before it spawns, awaits [`heard`](Self::heard) beside its work, and once
/// that answers calls [`stop_spawned`] and exits. From the moment it listens, neither signal
/// ends the process by itself.
#[derive(Debug)]
pub struct Interrupts {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl Interrupts {
    /// Starts listening, within a runtime.
    ///
    /// # Errors
    ///
    /// The error of installing the signals' handlers.
    pub fn listen() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    /// Waits for either signal, and answers the exit status a process so ended has: 130 for an
    /// interrupt, 143 for a termination.
    pub async fn heard(&mut self) -> i32 {
        tokio::select! {
            biased;
            _ = self.interrupt.recv() => 130,
            _ = self.terminate.recv() => 143,
        }
    }
}

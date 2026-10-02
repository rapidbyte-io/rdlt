//! A connector's process group, owned by the thread that reaps its leader.
//!
//! A group's id is its leader's process id, and belongs to no other process while the leader
//! is this process's unreaped child. So the group is signalled only while its leader is seen
//! to be that, under a lock the thread that reaps it takes too; the thread sees the leader exit
//! without reaping it, kills the group while the id is still its own, and only then reaps.
//! Once the leader is reaped the group is never signalled again: it is only asked whether a
//! living member is left.

mod interrupts;
mod members;
mod registry;
#[cfg(test)]
mod tests;

use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use rustix::process::{
    Pid, Signal, WaitId, WaitIdOptions, WaitIdStatus, kill_process, kill_process_group, waitid,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::local::grants::Lease;
use crate::local::sandbox::Stops;
pub use interrupts::Interrupts;
pub(super) use registry::has_room;
#[cfg(test)]
pub(super) use registry::threads;
pub use registry::{Lingering, StopsSpawned, spawned, stop_spawned};

/// How often the thread that owns a group asks whether its leader has exited, a kill was
/// asked, or a stop's grace has passed.
const WATCH: Duration = Duration::from_millis(5);

/// How often a killed group is asked whether a living member is left: asking may read the state
/// of every process there is.
const MEMBERS: Duration = Duration::from_millis(25);

/// How long a group killed with `SIGKILL` has to be seen empty before its members are
/// reported as remaining: a process killed so ends at once, unless the kernel holds it.
const EMPTYING: Duration = Duration::from_secs(5);

/// Starts the thread that owns a group.
pub(super) type Threaded = fn(
    std::thread::Builder,
    Box<dyn FnOnce() + Send>,
) -> std::io::Result<std::thread::JoinHandle<()>>;

/// What stops a group, shared by the connector's handle, the host that stops every group, and
/// the thread that owns it.
pub(super) struct Held {
    state: Mutex<State>,
}

/// A group as those that signal it see it.
struct State {
    /// The group's leader, while it is this process's unreaped child for all that is known.
    leader: Option<Pid>,
    /// The leader's standard input, whose end asks a connector to stop.
    stdin: Option<ChildStdin>,
    /// How the connector is asked to stop.
    stops: Stops,
    /// How long the group has to end, once stopped, before it is killed.
    grace: Duration,
    /// When the group, stopped, is killed.
    killing: Option<Instant>,
}

impl Held {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Asks the group to stop, before it returns: the end of its leader's standard input and,
    /// where the connector is stopped by it, `SIGTERM` to every member, sent once, while its
    /// leader is seen to be this process's unreaped child, running or exited.
    ///
    /// The thread that owns the group kills it once its grace has passed.
    pub(super) fn stop(&self) {
        let mut state = self.state();
        let Some(leader) = state.leader else {
            return;
        };
        // A leader that exited and is unreaped still names its group: its members are asked.
        if state.killing.is_some() || Leader::of(&asked(leader)) == Leader::Lost {
            return;
        }
        drop(state.stdin.take());
        if state.stops == Stops::BySignal {
            signal(leader, Signal::TERM);
        }
        state.killing = Instant::now().checked_add(state.grace);
    }

    /// Has the thread that owns the group kill it now, whatever is left of its grace.
    pub(super) fn kill(&self) {
        self.state().killing = Some(Instant::now());
    }
}

/// How a group is stopped: how its connector is asked, and how long it then has to end.
#[derive(Clone, Copy, Debug)]
pub(super) struct Terms {
    pub(super) stops: Stops,
    pub(super) grace: Duration,
}

/// A connector to start: its command, and what stops, kills and tells of it.
pub(super) struct Starting {
    pub(super) command: Command,
    pub(super) terms: Terms,
    pub(super) killed: Option<CancellationToken>,
    pub(super) exit: watch::Sender<Option<ExitStatus>>,
    /// What its placement holds, released by this connector once it is reaped.
    pub(super) lease: Arc<Lease>,
}

/// A connector that started, on the thread that owns its group, which waits to be told
/// whether the group is kept.
pub(super) struct Started {
    /// What stops the group.
    pub(super) held: Arc<Held>,
    /// The leader's process id.
    pub(super) id: u32,
    pub(super) stdout: Option<ChildStdout>,
    pub(super) stderr: Option<ChildStderr>,
    keep: SyncSender<bool>,
}

impl Started {
    /// Leaves the group to its thread, which kills and reaps it once it is stopped, killed or
    /// its leader exits, whatever becomes of the runtime that asked for it.
    pub(super) fn kept(self) {
        self.keep.send(true).ok();
    }

    /// Has the thread kill the connector and its group and reap it, before this returns: the
    /// end of one that started and could not be used.
    pub(super) fn discarded(self) {
        self.keep.send(false).ok();
        registry::disown(self.id);
    }
}

/// Starts `starting`'s command on a thread of its own, started by `threaded`, which owns the
/// group the connector leads for its whole life.
///
/// The connector is the child of that thread: what ends a process with its parent, as a
/// parent-death signal does, ends it with the thread that owns it and with no other.
pub(super) fn start(starting: Starting, threaded: Threaded) -> std::io::Result<Started> {
    type Told = (Arc<Held>, u32, Option<ChildStdout>, Option<ChildStderr>);
    let (tell, told) = sync_channel::<std::io::Result<Told>>(1);
    let (keep, kept) = sync_channel::<bool>(1);
    let owning = move || {
        // Held until the connector is reaped, whichever way this ends.
        let Starting {
            mut command,
            terms,
            killed,
            exit,
            lease: _held,
        } = starting;
        let spawned = command.spawn();
        // The command holds this process's copies of the connector's descriptors: dropped,
        // the connector's exit closes its socket.
        drop(command);
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) => return drop(tell.send(Err(error))),
        };
        let (id, stdout, stderr) = (child.id(), child.stdout.take(), child.stderr.take());
        let mut owned = Owned::new(child, terms, killed, exit);
        if tell.send(Ok((owned.held(), id, stdout, stderr))).is_err() {
            return owned.discarded();
        }
        match kept.recv() {
            Ok(true) => {
                let (_, emptied) = owned.ended(emptied);
                registry::leave(id, emptied);
            }
            // Whoever asked for the connector could not use it, or is gone.
            Ok(false) | Err(_) => owned.discarded(),
        }
    };
    let owner = std::thread::Builder::new().name("rdlt-connector".to_owned());
    let thread = threaded(owner, Box::new(owning))?;
    let ended = || std::io::Error::other("the thread that owns a connector ended");
    let (held, id, stdout, stderr) = match told.recv().map_err(|_| ended()).flatten() {
        Ok(told) => told,
        Err(error) => {
            thread.join().ok();
            return Err(error);
        }
    };
    // Listed before its thread is told to keep it, so the thread's removal comes after.
    registry::enter(id, Arc::clone(&held), thread);
    Ok(Started {
        held,
        id,
        stdout,
        stderr,
        keep,
    })
}

/// A connector's process and the group it leads, with what kills them.
pub(super) struct Owned {
    child: Child,
    held: Arc<Held>,
    killed: Option<CancellationToken>,
    /// Told the leader's exit, once it is reaped.
    exit: watch::Sender<Option<ExitStatus>>,
}

impl Owned {
    /// `child` and the group it leads, which is stopped as `terms` say, is killed at once
    /// when `killed` is cancelled, and whose leader's `exit` is told.
    pub(super) fn new(
        mut child: Child,
        terms: Terms,
        killed: Option<CancellationToken>,
        exit: watch::Sender<Option<ExitStatus>>,
    ) -> Self {
        let state = State {
            leader: Some(Pid::from_child(&child)),
            stdin: child.stdin.take(),
            stops: terms.stops,
            grace: terms.grace,
            killing: None,
        };
        let held = Arc::new(Held {
            state: Mutex::new(state),
        });
        Self {
            child,
            held,
            killed,
            exit,
        }
    }

    /// What stops the group.
    pub(super) fn held(&self) -> Arc<Held> {
        Arc::clone(&self.held)
    }

    /// Kills the connector and its group, and reaps it: the end of one that started and could
    /// not be owned.
    pub(super) fn discarded(mut self) {
        let group = Pid::from_child(&self.child);
        let held = self.held();
        let state = held.state();
        if Leader::of(&asked(group)) != Leader::Lost {
            reap(&mut self.child, group, state);
        }
    }

    /// Owns the group until it has ended: the leader's exit, which is told as soon as it is
    /// reaped, and whether the group, asked through `emptied` after that, was seen empty.
    fn ended(&mut self, emptied: impl FnOnce(Pid) -> bool) -> (Option<ExitStatus>, bool) {
        let group = Pid::from_child(&self.child);
        let held = self.held();
        // Until the leader has exited, a kill has come, or a stop's grace has passed.
        loop {
            let mut state = held.state();
            let due = match Leader::of(&asked(group)) {
                Leader::Running => self.due(&state),
                Leader::Exited => true,
                // Its id may be another's by now: nothing is signalled, and nothing waited for.
                Leader::Lost => {
                    state.leader = None;
                    drop(state);
                    tracing::error!(group = self.child.id(), "a connector was reaped elsewhere");
                    return (None, emptied(group));
                }
            };
            if due {
                let status = reap(&mut self.child, group, state);
                self.exit.send_replace(status);
                return (status, emptied(group));
            }
            drop(state);
            std::thread::sleep(WATCH);
        }
    }

    /// Whether the group, its leader running, is to be killed now: a kill has come, or the
    /// grace of the stop `state` records has passed.
    fn due(&self, state: &State) -> bool {
        let killed = self.killed.as_ref();
        killed.is_some_and(CancellationToken::is_cancelled)
            || state
                .killing
                .is_some_and(|killing| Instant::now() >= killing)
    }
}

/// Kills `group` and its leader `child`, and reaps `child`, which was just seen to be this
/// process's unreaped child: the group's id is still its own, exited or not, and from here on,
/// as `state` then says, nobody's to signal.
fn reap(child: &mut Child, group: Pid, mut state: MutexGuard<'_, State>) -> Option<ExitStatus> {
    signal(group, Signal::KILL);
    let status = child.wait().ok();
    state.leader = None;
    drop(state.stdin.take());
    status
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

/// Whether `group`, killed and its leader reaped, is seen to have no living member within
/// [`EMPTYING`].
fn emptied(group: Pid) -> bool {
    let until = Instant::now().checked_add(EMPTYING);
    while members::living(group) {
        if until.is_none_or(|until| Instant::now() >= until) {
            return false;
        }
        std::thread::sleep(MEMBERS);
    }
    true
}

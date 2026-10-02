//! A connector's process: spawned with its socket on file descriptor 3, leading a process group
//! of its own, its output drained into `tracing`, and reaped by a thread that owns the group.
//!
//! The group is the host's for its whole life. It is stopped with `SIGTERM`, then `SIGKILL`
//! after a grace period; killed at once when a kill says so; and killed when its leader exits
//! by itself. The thread outlives the runtime that spawned the connector, so a connector whose
//! runtime is dropped is stopped all the same.

mod command;
mod group;
mod output;
#[cfg(test)]
mod tests;

use std::fmt;
use std::os::fd::{OwnedFd, RawFd};
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::ConnectorId;

use super::binary::Binary;
use super::grants::Lease;
use crate::provider::Digest;
use crate::secrets::Redactions;
use output::{Draining, Stream, Tail, drain, logging};
use tokio::process::{ChildStderr, ChildStdout};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::kills::Kills;
#[cfg(target_os = "linux")]
pub(crate) use command::executed_from;
use command::{Commanded, command};
pub(crate) use command::{Confinement, Unspawned, inheriting};
pub use group::{Interrupts, Lingering, StopsSpawned, spawned, stop_spawned};

/// The descriptor a connector finds its socket at.
pub(crate) const SOCKET_FD: RawFd = 3;

/// The descriptor a sandbox's launcher, or a script's interpreter, finds the connector's
/// program at.
pub(crate) const PROGRAM_FD: RawFd = 4;

/// The descriptor a sandbox's launcher finds the first path its connector is granted at, the
/// others following in turn: those between [`PROGRAM_FD`] and this are the launcher's own.
pub(crate) const GRANTS_FD: RawFd = 10;

/// How a connector's process is started.
#[derive(Clone, Debug)]
pub(crate) struct Launch {
    pub(crate) id: ConnectorId,
    /// The binary, open since it was placed: what is hashed and what is executed.
    pub(crate) binary: Arc<Binary>,
    /// The digest the binary had when placed, where it is checked: a binary whose bytes
    /// changed since is not spawned again.
    pub(crate) digest: Option<Digest>,
    /// The variables of this process's environment the connector's environment keeps.
    pub(crate) env_passthrough: Vec<String>,
    /// How long a stopped connector has to exit before it is killed.
    pub(crate) grace: Duration,
    /// What kills it at once, when anything does.
    pub(crate) kills: Option<Kills>,
    /// What is told its process id once it is spawned, when anything is.
    pub(crate) told: Option<Told>,
    /// The sandbox it runs in; none for a trusted binary.
    pub(crate) confinement: Option<Confinement>,
    /// What its placement holds: what it is granted, and the programs it runs.
    pub(crate) lease: Arc<Lease>,
    /// The bytes of state one request to it may carry, where not the protocol's: the host's own
    /// limit, so the connector takes what the host sends.
    pub(crate) state_bytes: Option<u64>,
}

/// The bytes of state a spawned connector is told one request may carry, given the host's
/// `limits`: the host's state limit, where it raises the protocol's.
///
/// The host's limit bounds what it takes, which its memory may hold to less than the protocol's;
/// what it sends, the engine's commits among it, it never lowers.
pub(crate) fn told_state(limits: &rdlt_wire::Limits) -> Option<u64> {
    Some(limits.state_bytes).filter(|bytes| *bytes > rdlt_wire::limits::STATE_BYTES)
}

/// What a host is told of each connector it spawns: its process id.
#[derive(Clone)]
pub(crate) struct Told(Arc<dyn Fn(u32) + Send + Sync>);

impl Told {
    pub(crate) fn new(told: impl Fn(u32) + Send + Sync + 'static) -> Self {
        Self(Arc::new(told))
    }
}

impl fmt::Debug for Told {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Told")
    }
}

/// A running connector's process, which dropping stops, with every process of its group.
pub(crate) struct Process {
    /// What stops the process and its group.
    held: Arc<group::Held>,
    /// What the kill that kills it cancels, when one may.
    killed: Option<CancellationToken>,
    /// The process's exit, once it has exited and is reaped.
    exit: watch::Receiver<Option<ExitStatus>>,
    /// Whether its standard error has closed.
    stderr_closed: watch::Receiver<bool>,
    tail: Arc<Tail>,
    /// What the connector was sent that nothing it says may show.
    redactions: Redactions,
}

impl Drop for Process {
    fn drop(&mut self) {
        // Asked before this returns: a host that drops its connectors and exits has stopped
        // them, though only one that waits sees them end.
        self.held.stop();
    }
}

/// The host's end of a spawned connector's socket, and the connector's process.
pub(crate) type Launched = (Box<dyn crate::network::Stream>, Process);

/// The steps of owning a spawned connector that can fail, each of which a test fails in turn.
pub(crate) struct Steps {
    stdout: fn(std::process::ChildStdout) -> std::io::Result<ChildStdout>,
    stderr: fn(std::process::ChildStderr) -> std::io::Result<ChildStderr>,
    thread: group::Threaded,
    /// Whether the kernel marks descriptors close-on-exec in one call.
    marks_at_once: fn() -> bool,
}

impl Steps {
    const TAKEN: Self = Self {
        stdout: ChildStdout::from_std,
        stderr: ChildStderr::from_std,
        thread: |thread, owning| thread.spawn(owning),
        marks_at_once: rdlt_adopt::marks_at_once,
    };
}

impl Process {
    /// Spawns `launch`'s binary serving the other end of `socket` at file descriptor 3, its
    /// output scrubbed of `redactions`, taking `steps`: a connector that started and a step
    /// then fails is killed and reaped before the failure is returned.
    fn spawn_by(
        launch: &Launch,
        socket: OwnedFd,
        redactions: Redactions,
        steps: &Steps,
    ) -> Result<Self, Unspawned> {
        group::has_room()?;
        let Commanded {
            command,
            stops,
            held,
        } = command(launch, socket, steps.marks_at_once)?;
        let (exit_sender, exit) = watch::channel(None);
        let killed = launch.kills.as_ref().map(Kills::next);
        let starting = group::Starting {
            command,
            terms: group::Terms {
                stops,
                grace: launch.grace,
            },
            killed: killed.clone(),
            exit: exit_sender,
            lease: Arc::clone(&launch.lease),
        };
        // Owned from here on by its thread: whatever fails next, the connector does not
        // outlive it.
        let mut started = group::start(starting, steps.thread)?;
        drop(held);
        let (tail, stderr_closed) = match drained(&mut started, &launch.id, &redactions, steps) {
            Ok(drained) => drained,
            Err(error) => {
                started.discarded();
                return Err(error.into());
            }
        };
        let (held, pid) = (Arc::clone(&started.held), started.id);
        started.kept();
        // Told once it is owned, and before anything is asked of it.
        if let Some(Told(told)) = &launch.told {
            told(pid);
        }
        Ok(Self {
            held,
            killed,
            exit,
            stderr_closed,
            tail,
            redactions,
        })
    }

    /// Spawns `launch`'s binary as [`launched`](Self::launched) does, on a thread that may
    /// block: the binary is hashed, and a sandbox may be tried, before it is spawned.
    pub(crate) async fn launching(
        launch: Launch,
        redactions: Redactions,
    ) -> Result<Launched, Box<(Launch, Unspawned)>> {
        let asked = launch.clone();
        let launching =
            tokio::task::spawn_blocking(move || match Self::launched(&launch, redactions) {
                Ok(launched) => Ok(launched),
                Err(unspawned) => Err(Box::new((launch, unspawned))),
            });
        match launching.await {
            Ok(launched) => launched,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            // The runtime is shutting down: nothing was spawned, or what was is dropped.
            Err(error) => Err(Box::new((asked, std::io::Error::other(error).into()))),
        }
    }

    /// Spawns `launch`'s binary serving one end of a new socket pair; the other end, and the
    /// process.
    ///
    /// Where a kill may kill the process, the other end counts it as landed once it ends after
    /// the kill: a socket ends when no process holds its other end.
    pub(crate) fn launched(launch: &Launch, redactions: Redactions) -> Result<Launched, Unspawned> {
        let (host, connector) = std::os::unix::net::UnixStream::pair()?;
        let process = Self::spawn_by(launch, connector.into(), redactions, &Steps::TAKEN)?;
        host.set_nonblocking(true)?;
        let host = tokio::net::UnixStream::from_std(host)?;
        let host = match (&launch.kills, &process.killed) {
            (Some(kills), Some(killed)) => kills.watch(host, killed.clone()),
            _ => Box::new(host) as Box<dyn crate::network::Stream>,
        };
        Ok((host, process))
    }

    /// What the connector left on its standard error, and how it exited, once it has: waits
    /// `patience` for it to exit and its standard error to close, as a failed transport suggests.
    pub(crate) async fn last_words(&self, patience: Duration) -> LastWords {
        self.witness().last_words(patience).await
    }

    /// A witness to how the process ends, which outlives it.
    pub(crate) fn witness(&self) -> Witness {
        Witness {
            exit: self.exit.clone(),
            stderr_closed: self.stderr_closed.clone(),
            tail: Arc::clone(&self.tail),
            redactions: self.redactions.clone(),
        }
    }
}

/// A witness to how a spawned connector ends: its exit, and the last bytes of its standard error.
#[derive(Clone, Debug)]
pub struct Witness {
    exit: watch::Receiver<Option<ExitStatus>>,
    stderr_closed: watch::Receiver<bool>,
    tail: Arc<Tail>,
    redactions: Redactions,
}

impl Witness {
    /// What the connector left on its standard error, and how it exited, once it has: waits
    /// `patience` for it to exit and its standard error to close, as a failed transport suggests.
    pub async fn last_words(&self, patience: Duration) -> LastWords {
        let (mut closed, mut exit) = (self.stderr_closed.clone(), self.exit.clone());
        let ended = async {
            closed.wait_for(|closed| *closed).await.ok();
            exit.wait_for(Option::is_some).await.ok();
        };
        tokio::time::timeout(patience, ended).await.ok();
        LastWords {
            exit: *self.exit.borrow(),
            stderr: self.tail.words(&self.redactions),
        }
    }
}

/// How a connector ended, as its transport's errors carry it: what it wrote is shown on one
/// line, scrubbed of the secrets it was sent, and never obeyed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub struct LastWords {
    /// How it exited, if it has.
    pub exit: Option<ExitStatus>,
    /// The end of its standard error, in [`LAST_WORDS_BYTES`](crate::limits::LAST_WORDS_BYTES)
    /// at most, each line end and every other character a reader could be deceived by escaped.
    pub stderr: String,
}

impl fmt::Display for LastWords {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.exit {
            Some(exit) => write!(formatter, "the connector exited ({exit})")?,
            None => write!(formatter, "the connector is running")?,
        }
        if self.stderr.is_empty() {
            write!(formatter, ", and wrote nothing to its standard error")
        } else {
            write!(formatter, "; its standard error ends: {}", self.stderr)
        }
    }
}

/// Drains `started`'s standard output and error into `tracing`: the tail of its standard
/// error, and what tells when that has closed.
fn drained(
    started: &mut group::Started,
    connector: &ConnectorId,
    redactions: &Redactions,
    steps: &Steps,
) -> std::io::Result<(Arc<Tail>, watch::Receiver<bool>)> {
    let pid = started.id;
    let tail = Arc::new(Tail::default());
    let (closed, stderr_closed) = watch::channel(false);
    let stdout = started.stdout.take().map(steps.stdout).transpose()?;
    let stderr = started.stderr.take().map(steps.stderr).transpose()?;
    if let Some(stdout) = stdout {
        let draining = Draining {
            kept: None,
            redactions: redactions.clone(),
            log: logging(connector.clone(), pid, Stream::Stdout),
        };
        tokio::spawn(drain(stdout, draining));
    }
    if let Some(stderr) = stderr {
        let draining = Draining {
            kept: Some((Arc::clone(&tail), closed)),
            redactions: redactions.clone(),
            log: logging(connector.clone(), pid, Stream::Stderr),
        };
        tokio::spawn(drain(stderr, draining));
    }
    Ok((tail, stderr_closed))
}

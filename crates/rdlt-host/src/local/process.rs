//! A connector's process: spawned with its socket on file descriptor 3, leading a process group
//! of its own, its output drained into `tracing`, and reaped by a thread that owns the group.
//!
//! The group is the host's for its whole life. It is stopped with `SIGTERM`, then `SIGKILL`
//! after a grace period; killed at once when a kill says so; and killed when its leader exits
//! by itself. The thread outlives the runtime that spawned the connector, so a connector whose
//! runtime is dropped is stopped all the same.

mod descriptors;
mod group;
mod output;
#[cfg(test)]
mod tests;

use std::ffi::OsString;
use std::fmt;
use std::os::fd::{OwnedFd, RawFd};
use std::os::unix::process::CommandExt as _;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use command_fds::{CommandFdExt as _, FdMapping};
use rdlt_connector::ConnectorId;

use super::binary::Binary;
use super::sandbox::{Confined, Grants, Sandbox, SandboxError, Stops};
use crate::provider::Digest;
use crate::secrets::Redactions;
use output::{Draining, Stream, Tail, drain, logging};
use tokio::process::{ChildStderr, ChildStdout};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::kills::Kills;
pub use group::{Interrupts, Lingering, StopsSpawned, spawned, stop_spawned};

/// The descriptor a connector finds its socket at.
pub(crate) const SOCKET_FD: RawFd = 3;

/// The descriptor a sandbox's launcher, or a script's interpreter, finds the connector's
/// program at.
pub(crate) const PROGRAM_FD: RawFd = 4;

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
    /// The sandbox it runs in and what it is granted there; none for a trusted binary.
    pub(crate) confinement: Option<(Arc<dyn Sandbox>, Grants)>,
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

/// Why a connector's command could not be made.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Unspawned {
    /// Its sandbox cannot be used.
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    /// Its binary changed since it was placed.
    #[error("the binary's digest is {found}, not {expected}")]
    Changed {
        /// The digest it was placed with.
        expected: Digest,
        /// The digest it has.
        found: Digest,
    },
    /// The operating system refused a step.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// The command that starts `launch`'s binary serving `socket` at file descriptor 3, with only
/// the environment `launch` keeps, in a process group of its own, holding no other descriptor
/// of this process; and how the connector is asked to stop.
fn command(launch: &Launch, socket: OwnedFd) -> Result<Commanded, Unspawned> {
    if let Some(expected) = launch.digest {
        let found = launch.binary.digest()?;
        if found != expected {
            return Err(Unspawned::Changed { expected, found });
        }
    }
    let args = [OsString::from(format!("--rdlt-fd={SOCKET_FD}"))];
    let kept = |name: &String| Some((OsString::from(name), std::env::var_os(name)?));
    let env: Vec<(OsString, OsString)> = launch.env_passthrough.iter().filter_map(kept).collect();
    let mut given = vec![FdMapping {
        parent_fd: socket,
        child_fd: SOCKET_FD,
    }];
    let (mut command, stops, executed) = if let Some((sandbox, grants)) = &launch.confinement {
        given.push(program(&launch.binary)?);
        let confined = Confined {
            program: PROGRAM_FD,
            args: &args,
            env: &env,
            socket: SOCKET_FD,
            grants,
        };
        let launcher = sandbox.launcher(&confined)?;
        (launcher.command, launcher.stops, None)
    } else {
        let (mut command, executed) = trusted(&launch.binary, &mut given)?;
        command.args(&args).env_clear().envs(env);
        (command, Stops::BySignal, executed)
    };
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // A group of its own, which this host owns: what the connector starts ends with it.
        .process_group(0);
    command
        .fd_mappings(descriptors::mappings(given)?)
        .map_err(|_| std::io::Error::other("a descriptor is given twice"))?;
    Ok(Commanded {
        command,
        stops,
        executed,
    })
}

/// A connector's command, how the connector is asked to stop, and the descriptor the command
/// executes, held open until it has.
struct Commanded {
    command: Command,
    stops: Stops,
    executed: Option<OwnedFd>,
}

/// Gives `command`'s process `file` at descriptor `at`, and no other descriptor of this
/// process beside the standard streams the command names.
pub(crate) fn given(command: &mut Command, file: &std::fs::File, at: RawFd) -> std::io::Result<()> {
    let mapping = FdMapping {
        parent_fd: file.try_clone()?.into(),
        child_fd: at,
    };
    command
        .fd_mappings(descriptors::mappings(vec![mapping])?)
        .map_err(|_| std::io::Error::other("a descriptor is given twice"))?;
    Ok(())
}

/// `binary`'s open file, given at [`PROGRAM_FD`].
fn program(binary: &Binary) -> std::io::Result<FdMapping> {
    Ok(FdMapping {
        parent_fd: binary.file().try_clone()?.into(),
        child_fd: PROGRAM_FD,
    })
}

/// The command that executes `binary`'s open file, not its path: whatever the path names by
/// now, what was opened, and hashed, is what runs.
///
/// A script's interpreter opens the script by the name it was executed by, so a script is
/// also `given` at [`PROGRAM_FD`], which its interpreter holds; a binary is given nowhere.
#[cfg(target_os = "linux")]
fn trusted(
    binary: &Binary,
    given: &mut Vec<FdMapping>,
) -> std::io::Result<(Command, Option<OwnedFd>)> {
    use std::os::fd::AsRawFd as _;
    if binary.is_script()? {
        given.push(program(binary)?);
        return Ok((Command::new(format!("/proc/self/fd/{PROGRAM_FD}")), None));
    }
    // Above every descriptor a connector is given, so that giving those replaces none it is
    // executed from; closed on exec, and here once the command has spawned.
    let executed = rustix::io::fcntl_dupfd_cloexec(binary.file(), PROGRAM_FD + 1)?;
    let command = Command::new(format!("/proc/self/fd/{}", executed.as_raw_fd()));
    Ok((command, Some(executed)))
}

/// The command that executes `binary` by its path: this platform executes no open file, so
/// its digest is neither checked nor reported.
#[cfg(not(target_os = "linux"))]
fn trusted(
    binary: &Binary,
    _given: &mut Vec<FdMapping>,
) -> std::io::Result<(Command, Option<OwnedFd>)> {
    Ok((Command::new(binary.path()), None))
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
}

impl Steps {
    const TAKEN: Self = Self {
        stdout: ChildStdout::from_std,
        stderr: ChildStderr::from_std,
        thread: |thread, owning| thread.spawn(owning),
    };
}

impl Process {
    /// What the connector is sent that nothing it says may show: filled once its
    /// configuration's secrets are resolved.
    pub(crate) fn redactions(&self) -> &Redactions {
        &self.redactions
    }

    /// Spawns `launch`'s binary serving the other end of `socket` at file descriptor 3, taking
    /// `steps`: a connector that started and a step then fails is killed and reaped before the
    /// failure is returned.
    fn spawn_by(launch: &Launch, socket: OwnedFd, steps: &Steps) -> Result<Self, Unspawned> {
        group::has_room()?;
        let Commanded {
            command,
            stops,
            executed,
        } = command(launch, socket)?;
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
        };
        // Owned from here on by its thread: whatever fails next, the connector does not
        // outlive it.
        let mut started = group::start(starting, steps.thread)?;
        drop(executed);
        let redactions = Redactions::new();
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
    pub(crate) async fn launching(launch: Launch) -> Result<Launched, Box<(Launch, Unspawned)>> {
        let asked = launch.clone();
        let launching = tokio::task::spawn_blocking(move || match Self::launched(&launch) {
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
    pub(crate) fn launched(launch: &Launch) -> Result<Launched, Unspawned> {
        let spawning = descriptors::spawning();
        let (host, connector) = std::os::unix::net::UnixStream::pair()?;
        let process = Self::spawn_by(launch, connector.into(), &Steps::TAKEN)?;
        drop(spawning);
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

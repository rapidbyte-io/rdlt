//! A connector's process: spawned with its socket on file descriptor 3, leading a process group
//! of its own, its output drained into `tracing`, and reaped by a thread that owns the group.
//!
//! The group is the host's for its whole life. It is stopped with `SIGTERM`, then `SIGKILL`
//! after a grace period; killed at once when a kill says so; and killed when its leader exits
//! by itself. The thread outlives the runtime that spawned the connector, so a connector whose
//! runtime is dropped is stopped all the same.

mod group;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::fmt;
use std::os::fd::OwnedFd;
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use command_fds::{CommandFdExt as _, FdMapping};
use rdlt_connector::ConnectorId;

use crate::provider::Digest;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, BufReader};
use tokio::process::{ChildStderr, ChildStdout};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::kills::Kills;
pub use group::{Interrupts, Lingering, StopsSpawned, spawned, stop_spawned};

/// Bytes of a connector's standard error kept for the errors of its transport.
pub(crate) const TAIL_BYTES: usize = 8 * 1024;

/// How a connector's process is started.
#[derive(Clone, Debug)]
pub(crate) struct Launch {
    pub(crate) id: ConnectorId,
    pub(crate) path: std::path::PathBuf,
    /// The digest the binary had when placed, when supervised: a binary changed since is not
    /// spawned again.
    pub(crate) digest: Option<Digest>,
    /// The variables of this process's environment the connector's environment keeps.
    pub(crate) env_passthrough: Vec<String>,
    /// How long a stopped connector has to exit before it is killed.
    pub(crate) grace: Duration,
    /// What kills it at once, when anything does.
    pub(crate) kills: Option<Kills>,
}

/// The last bytes a connector wrote to its standard error, and whether it has closed it.
#[derive(Debug, Default)]
pub(crate) struct Tail {
    bytes: Mutex<VecDeque<u8>>,
}

impl Tail {
    fn push(&self, line: &[u8]) {
        let mut bytes = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        bytes.extend(line);
        let excess = bytes.len().saturating_sub(TAIL_BYTES);
        bytes.drain(..excess);
    }

    /// The kept bytes, as text.
    pub(crate) fn text(&self) -> String {
        let bytes = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (front, back) = bytes.as_slices();
        String::from_utf8_lossy(&[front, back].concat()).into_owned()
    }
}

/// The command that starts `launch`'s binary serving `socket` at file descriptor 3, with a
/// cleared environment, in a process group of its own.
fn command(launch: &Launch, socket: OwnedFd) -> std::io::Result<Command> {
    let mut command = Command::new(&launch.path);
    command
        .arg("--rdlt-fd=3")
        .env_clear()
        .envs(
        launch
            .env_passthrough
            .iter()
            .filter_map(|name| Some((name, std::env::var_os(name)?))),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // A group of its own, which this host owns: what the connector starts ends with it.
        .process_group(0);
    command
        .fd_mappings(vec![FdMapping {
            parent_fd: socket,
            child_fd: 3,
        }])
        .map_err(|_| std::io::Error::other("file descriptor 3 is mapped twice"))?;
    Ok(command)
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
}

impl Drop for Process {
    fn drop(&mut self) {
        // Asked before this returns: a host that drops its connectors and exits has stopped
        // them, though only one that waits sees them end.
        self.held.stop();
    }
}

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
    /// Spawns `launch`'s binary serving the other end of `socket` at file descriptor 3.
    pub(crate) fn spawn(launch: &Launch, socket: OwnedFd) -> std::io::Result<Self> {
        Self::spawn_by(launch, socket, &Steps::TAKEN)
    }

    /// Spawns `launch`'s binary as [`spawn`](Self::spawn) does, taking `steps`: a connector
    /// that started and a step then fails is killed and reaped before the failure is returned.
    fn spawn_by(launch: &Launch, socket: OwnedFd, steps: &Steps) -> std::io::Result<Self> {
        group::has_room()?;
        let mut command = command(launch, socket)?;
        let child = command.spawn()?;
        // The command holds this process's copy of the connector's end: dropped, the connector's
        // exit closes the socket.
        drop(command);
        let (exit_sender, exit) = watch::channel(None);
        let killed = launch.kills.as_ref().map(Kills::next);
        // Owned from here on: whatever fails next, the connector does not outlive it.
        let mut owned = group::Owned::new(child, launch.grace, killed.clone(), exit_sender);
        let held = owned.held();
        let (tail, stderr_closed) = match drained(owned.child(), &launch.id, steps) {
            Ok(drained) => drained,
            Err(error) => {
                owned.discarded();
                return Err(error);
            }
        };
        owned.reaped(steps.thread)?;
        Ok(Self {
            held,
            killed,
            exit,
            stderr_closed,
            tail,
        })
    }

    /// Spawns `launch`'s binary serving one end of a new socket pair; the other end, and the
    /// process.
    ///
    /// Where a kill may kill the process, the other end counts it as landed once it ends after
    /// the kill: a socket ends when no process holds its other end.
    pub(crate) fn launched(
        launch: &Launch,
    ) -> std::io::Result<(Box<dyn crate::network::Stream>, Self)> {
        let (host, connector) = std::os::unix::net::UnixStream::pair()?;
        let process = Self::spawn(launch, connector.into())?;
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
        }
    }
}

/// A witness to how a spawned connector ends: its exit, and the last bytes of its standard error.
#[derive(Clone, Debug)]
pub struct Witness {
    exit: watch::Receiver<Option<ExitStatus>>,
    stderr_closed: watch::Receiver<bool>,
    tail: Arc<Tail>,
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
            stderr: self.tail.text(),
        }
    }
}

/// How a connector ended, as its transport's errors carry it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub struct LastWords {
    /// How it exited, if it has.
    pub exit: Option<ExitStatus>,
    /// The last bytes of its standard error.
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
            write!(formatter, "; its standard error ends:\n{}", self.stderr)
        }
    }
}

/// Drains `child`'s standard output and error into `tracing`: the tail of its standard error,
/// and what tells when that has closed.
fn drained(
    child: &mut std::process::Child,
    connector: &ConnectorId,
    steps: &Steps,
) -> std::io::Result<(Arc<Tail>, watch::Receiver<bool>)> {
    let pid = Some(child.id());
    let tail = Arc::new(Tail::default());
    let (closed, stderr_closed) = watch::channel(false);
    let stdout = child.stdout.take().map(steps.stdout).transpose()?;
    let stderr = child.stderr.take().map(steps.stderr).transpose()?;
    if let Some(stdout) = stdout {
        tokio::spawn(drain(stdout, connector.clone(), pid, Stream::Stdout, None));
    }
    if let Some(stderr) = stderr {
        let kept = Some((Arc::clone(&tail), closed));
        tokio::spawn(drain(stderr, connector.clone(), pid, Stream::Stderr, kept));
    }
    Ok((tail, stderr_closed))
}

#[derive(Clone, Copy, Debug)]
enum Stream {
    Stdout,
    Stderr,
}

/// Forwards each line of `output` to `tracing`, in pieces of at most [`TAIL_BYTES`], so a
/// connector that never ends a line cannot grow this process's memory: standard output, which a
/// connector should not use, as a warning; standard error as information, keeping its last bytes
/// in `kept`'s tail.
async fn drain(
    output: impl AsyncRead + Unpin,
    connector: ConnectorId,
    pid: Option<u32>,
    stream: Stream,
    kept: Option<(Arc<Tail>, watch::Sender<bool>)>,
) {
    let mut reader = BufReader::new(output);
    let mut line = Vec::new();
    loop {
        line.clear();
        let mut piece = (&mut reader).take(TAIL_BYTES as u64);
        match piece.read_until(b'\n', &mut line).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        forward(
            &line,
            &connector,
            pid,
            stream,
            kept.as_ref().map(|(tail, _)| &**tail),
        );
    }
    if let Some((_, closed)) = kept {
        closed.send_replace(true);
    }
}

/// Forwards `line`, a line of `stream` or a piece of one, to `tracing`, and keeps it in `tail`.
fn forward(
    line: &[u8],
    connector: &ConnectorId,
    pid: Option<u32>,
    stream: Stream,
    tail: Option<&Tail>,
) {
    if let Some(tail) = tail {
        tail.push(line);
    }
    let text = String::from_utf8_lossy(line);
    let text = text.trim_end();
    match stream {
        Stream::Stdout => {
            tracing::warn!(connector = %connector, pid, "connector wrote to stdout: {text}");
        }
        Stream::Stderr => tracing::info!(connector = %connector, pid, "{text}"),
    }
}

/// Whether `path` is a file this process may execute.
pub(crate) fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

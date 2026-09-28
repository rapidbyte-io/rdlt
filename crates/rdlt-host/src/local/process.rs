//! A connector's process: spawned with its socket on file descriptor 3, its output drained into
//! `tracing`, and stopped by a reaper task with `SIGTERM`, then `SIGKILL` after a grace period.

use std::collections::VecDeque;
use std::fmt;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use command_fds::{CommandFdExt as _, FdMapping};
use rdlt_connector::ConnectorId;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::kills::Kills;

/// Bytes of a connector's standard error kept for the errors of its transport.
pub(crate) const TAIL_BYTES: usize = 8 * 1024;

/// How a connector's process is started.
#[derive(Clone, Debug)]
pub(crate) struct Launch {
    pub(crate) id: ConnectorId,
    pub(crate) path: std::path::PathBuf,
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

/// A running connector's process, which dropping stops.
pub(crate) struct Process {
    stop: CancellationToken,
    /// The process's exit, once it has exited.
    exit: watch::Receiver<Option<ExitStatus>>,
    /// Whether its standard error has closed.
    stderr_closed: watch::Receiver<bool>,
    tail: Arc<Tail>,
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl Process {
    /// Spawns `launch`'s binary serving the other end of `socket` at file descriptor 3.
    pub(crate) fn spawn(launch: &Launch, socket: OwnedFd) -> std::io::Result<Self> {
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
            .kill_on_drop(true);
        command
            .fd_mappings(vec![FdMapping {
                parent_fd: socket,
                child_fd: 3,
            }])
            .map_err(|_| std::io::Error::other("file descriptor 3 is mapped twice"))?;
        let mut child = command.spawn()?;
        // The command holds this process's copy of the connector's end: dropped, the connector's
        // exit closes the socket.
        drop(command);
        let pid = child.id();
        let tail = Arc::new(Tail::default());
        let (closed, stderr_closed) = watch::channel(false);
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(drain(stdout, launch.id.clone(), pid, Stream::Stdout, None));
        }
        if let Some(stderr) = child.stderr.take() {
            let kept = Some((Arc::clone(&tail), closed));
            tokio::spawn(drain(stderr, launch.id.clone(), pid, Stream::Stderr, kept));
        }
        let stdin = child.stdin.take();
        let stop = CancellationToken::new();
        let (exit_sender, exit) = watch::channel(None);
        let killed = launch.kills.as_ref().map(Kills::next);
        tokio::spawn(reap(
            child,
            stdin,
            launch.grace,
            (stop.clone(), killed),
            exit_sender,
        ));
        Ok(Self {
            stop,
            exit,
            stderr_closed,
            tail,
        })
    }

    /// Spawns `launch`'s binary serving one end of a new socket pair; the other end, and the
    /// process.
    pub(crate) fn launched(launch: &Launch) -> std::io::Result<(tokio::net::UnixStream, Self)> {
        let (host, connector) = std::os::unix::net::UnixStream::pair()?;
        let process = Self::spawn(launch, connector.into())?;
        host.set_nonblocking(true)?;
        Ok((tokio::net::UnixStream::from_std(host)?, process))
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

/// Waits for `child` to exit, reporting it through `exit`; once `stop` is cancelled, closes its
/// standard input and sends `SIGTERM`, and after `grace`, `SIGKILL`; once `killed` is, `SIGKILL`
/// at once, through the child's own handle, so no reused process id is signalled.
async fn reap(
    mut child: Child,
    stdin: Option<ChildStdin>,
    grace: Duration,
    (stop, killed): (CancellationToken, Option<CancellationToken>),
    exit: watch::Sender<Option<ExitStatus>>,
) {
    let killing = async {
        match &killed {
            Some(killed) => killed.cancelled().await,
            None => std::future::pending().await,
        }
    };
    let status = tokio::select! {
        biased;
        status = child.wait() => status,
        () = killing => {
            child.start_kill().ok();
            child.wait().await
        }
        () = stop.cancelled() => {
            drop(stdin);
            terminate(&child);
            if let Ok(status) = tokio::time::timeout(grace, child.wait()).await {
                status
            } else {
                child.start_kill().ok();
                child.wait().await
            }
        }
    };
    exit.send_replace(status.ok());
}

/// Sends `SIGTERM` to `child`, if it is still running.
fn terminate(child: &Child) {
    let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        return;
    };
    let pid = nix::unistd::Pid::from_raw(pid);
    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM).ok();
}

/// Whether `path` is a file this process may execute.
pub(crate) fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

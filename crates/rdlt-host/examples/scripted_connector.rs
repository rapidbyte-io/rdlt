//! A source connector for the host's tests of process placement, whose configuration scripts how
//! it behaves: what it writes to its standard output and error, when it crashes, and what its
//! environment must hold.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use rdlt_connector::ConnectContext;
use rdlt_connector::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How the connector behaves.
#[derive(Debug, Default, Deserialize, JsonSchema)]
struct Script {
    /// Rows its one stream, `rows`, reads; none reads until stopped.
    rows: Option<u64>,
    /// Bytes its check writes to standard output first, in lines.
    #[serde(default)]
    stdout_bytes: usize,
    /// Bytes its check writes to standard output first, with no line break.
    #[serde(default)]
    stdout_unbroken_bytes: usize,
    /// A last word its check writes to standard error before it exits with status 3.
    crash: Option<String>,
    /// The row at which a read crashes, once: the first time, it creates `marker` and exits.
    crash_once_at: Option<u64>,
    /// The file that says a read has crashed already.
    marker: Option<PathBuf>,
    /// The environment its check requires: each variable's value, or its absence.
    #[serde(default)]
    env: BTreeMap<String, Option<String>>,
    /// Whether it outlives its socket once it has served it, and what then ends it.
    linger: Option<Linger>,
    /// Milliseconds its check takes.
    #[serde(default)]
    slow_check_ms: u64,
    /// Where it writes its process id as it connects.
    pid_file: Option<PathBuf>,
    /// Whether its check fails when a process it starts inherits file descriptor 3.
    #[serde(default)]
    probe_fd_3: bool,
    /// Whether its check fails unless it started with its standard streams and file descriptor
    /// 3 open, and no other descriptor.
    #[serde(default)]
    only_its_descriptors: bool,
    /// Its whole environment, when its check requires it to be exactly this.
    whole_env: Option<BTreeMap<String, String>>,
    /// Paths its check requires not to exist for it.
    #[serde(default)]
    absent: Vec<PathBuf>,
    /// Files its check requires to read.
    #[serde(default)]
    readable: Vec<PathBuf>,
    /// A file its check requires to create, and writes `written` to.
    writes: Option<PathBuf>,
    /// An address its check requires a TCP connection to succeed to, or to fail to.
    connects: Option<(String, bool)>,
    /// The most processes its check may see in `/proc`, itself included.
    sees_processes: Option<usize>,
    /// A command line its connect starts with `/bin/sh -c`, in a process that outlives every
    /// stop but a kill: it ignores `SIGTERM`, and holds none of the connector's streams.
    starts: Option<String>,
    /// What it says back wherever it can, as a careless connector does with its credentials:
    /// on its standard output and error as it connects, in the error its check fails with,
    /// and in the error a read fails with, once, at row `fail_once_at`.
    said: Option<serde_json::Value>,
    /// The row at which a read fails, once: the first time, it creates `marker` and fails.
    fail_once_at: Option<u64>,
}

/// A cause that says `0`, as a driver's error says what it was given.
#[derive(Debug)]
struct Driver(String);

impl std::fmt::Display for Driver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "the driver refused {}", self.0)
    }
}

impl std::error::Error for Driver {}

/// An error that says `said` in every form a connector may: as JSON and as `Debug` writes it,
/// in its message and in its cause.
fn saying(said: &serde_json::Value) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Transient,
        format!("it said {said} and {said:?}"),
    )
    .with_source(Driver(format!("{said:#}")))
}

/// The descriptors that were open as the connector started, before it opened any.
static STARTED_WITH: std::sync::OnceLock<Vec<i32>> = std::sync::OnceLock::new();

/// The descriptors open now, but what lists them.
fn open_descriptors() -> Vec<i32> {
    let listed: Vec<i32> = std::fs::read_dir("/dev/fd")
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse().ok())
                .collect()
        })
        .unwrap_or_default();
    // The listing's own descriptor is closed by now, and no longer there.
    let mut open: Vec<i32> = listed
        .into_iter()
        .filter(|fd| std::fs::metadata(format!("/dev/fd/{fd}")).is_ok())
        .collect();
    open.sort_unstable();
    open
}

impl Script {
    /// Checks what the connector can see and reach of its host, as the script requires.
    fn confined(&self) -> std::result::Result<(), String> {
        let started_with = STARTED_WITH.get().cloned().unwrap_or_default();
        if self.only_its_descriptors && started_with != [0, 1, 2, 3] {
            return Err(format!("started with descriptors {started_with:?}"));
        }
        if let Some(whole) = &self.whole_env {
            let found: BTreeMap<String, String> = std::env::vars().collect();
            if found != *whole {
                let names: Vec<&String> = found.keys().collect();
                return Err(format!("the environment holds {names:?}"));
            }
        }
        for path in &self.absent {
            if std::fs::symlink_metadata(path).is_ok() {
                return Err(format!("{} is there", path.display()));
            }
        }
        for path in &self.readable {
            std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
        }
        if let Some(path) = &self.writes {
            std::fs::write(path, "written")
                .map_err(|error| format!("{}: {error}", path.display()))?;
        }
        if let Some((address, reached)) = &self.connects {
            let address: std::net::SocketAddr = address.parse().map_err(|_| "no address")?;
            let patience = std::time::Duration::from_secs(2);
            let connected = std::net::TcpStream::connect_timeout(&address, patience).is_ok();
            if connected != *reached {
                return Err(format!("connecting to {address} succeeded: {connected}"));
            }
        }
        if let Some(most) = self.sees_processes {
            let numbered = |entry: std::io::Result<std::fs::DirEntry>| {
                let name = entry.ok()?.file_name();
                name.to_str()?.parse::<u32>().ok()
            };
            let processes = std::fs::read_dir("/proc")
                .map(|entries| entries.filter_map(numbered).count())
                .map_err(|error| format!("/proc: {error}"))?;
            if processes > most {
                return Err(format!("{processes} processes are seen"));
            }
        }
        Ok(())
    }
}

/// What ends a connector that outlives its socket.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Linger {
    /// `SIGTERM`, at any time.
    Terminable,
    /// Nothing but `SIGKILL`.
    Forever,
}

/// How the connector lingers once it has served its socket, as its configuration said.
static LINGER: std::sync::OnceLock<Linger> = std::sync::OnceLock::new();

#[derive(Debug)]
struct Scripted {
    script: Script,
}

/// Writes `words` to standard error and exits with status 3, as a crashing connector does.
fn crash(words: &str) -> ! {
    let mut stderr = std::io::stderr();
    writeln!(stderr, "{words}").ok();
    stderr.flush().ok();
    std::process::exit(3)
}

#[source(id = "test.scripted")]
impl SourceConnector for Scripted {
    type Config = Script;

    async fn connect(script: Script, _context: &ConnectContext) -> Result<Self> {
        if let Some(linger) = script.linger {
            LINGER.set(linger).ok();
            if matches!(linger, Linger::Terminable) {
                exit_on_sigterm();
            }
        }
        if let Some(pid_file) = &script.pid_file {
            std::fs::write(pid_file, std::process::id().to_string())
                .map_err(|error| ConnectorError::internal(error.to_string()))?;
        }
        if let Some(said) = &script.said {
            let (mut stdout, mut stderr) = (std::io::stdout(), std::io::stderr());
            writeln!(stdout, "connecting with {said}").ok();
            writeln!(stderr, "connecting with {said:?}").ok();
            writeln!(stderr, "{said:#}").ok();
        }
        if let Some(started) = &script.starts {
            std::process::Command::new("/bin/sh")
                .args(["-c", &format!("trap '' TERM; {started}")])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|error| ConnectorError::internal(error.to_string()))?;
        }
        Ok(Self { script })
    }

    async fn check(&self) -> Result<()> {
        let script = &self.script;
        let mut stdout = std::io::stdout();
        for _ in 0..script.stdout_bytes / 64 {
            writeln!(stdout, "{}", "o".repeat(63)).ok();
        }
        let chunk = vec![b'u'; 64 * 1024];
        for _ in 0..script.stdout_unbroken_bytes / chunk.len() {
            stdout.write_all(&chunk).ok();
        }
        stdout.flush().ok();
        if let Some(words) = &script.crash {
            crash(words);
        }
        tokio::time::sleep(std::time::Duration::from_millis(script.slow_check_ms)).await;
        if script.probe_fd_3 {
            let inherited = std::process::Command::new("/bin/sh")
                .args(["-c", "[ -e /dev/fd/3 ]"])
                .status()
                .map_err(|error| ConnectorError::internal(error.to_string()))?;
            if inherited.success() {
                return Err(ConnectorError::internal(
                    "a child inherited the host's socket",
                ));
            }
        }
        script.confined().map_err(ConnectorError::config)?;
        if let (Some(said), None) = (&script.said, script.fail_once_at) {
            return Err(saying(said));
        }
        for (name, expected) in &script.env {
            let found = std::env::var(name).ok();
            if &found != expected {
                let message = format!("`{name}` is {found:?}, not {expected:?}");
                return Err(ConnectorError::config(message));
            }
        }
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Rows)
    }
}

struct Rows;

/// The next row to read.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Next {
    next: u64,
}

impl ReadStream<Scripted> for Rows {
    type Cursor = Next;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("rows").expect("a valid name"))
    }

    async fn read(
        &self,
        source: &Scripted,
        _partition: &Partition,
        cursor: Next,
        out: &mut Emitter<Next>,
    ) -> Result<()> {
        let script = &source.script;
        let mut next = cursor.next;
        while script.rows.is_none_or(|rows| next < rows) {
            if script.crash_once_at == Some(next)
                && let Some(marker) = &script.marker
                && std::fs::File::create_new(marker).is_ok()
            {
                crash(&format!("crashing at row {next}"));
            }
            if script.fail_once_at == Some(next)
                && let (Some(marker), Some(said)) = (&script.marker, &script.said)
                && std::fs::File::create_new(marker).is_ok()
            {
                return Err(saying(said));
            }
            out.rows(&[serde_json::json!({ "id": next })]).await?;
            next += 1;
            if out.checkpoint_due() {
                out.checkpoint(&Next { next }).await?;
            }
            tokio::task::yield_now().await;
        }
        out.checkpoint(&Next { next }).await
    }
}

/// Exits at the first `SIGTERM` from now on, from a thread of its own.
fn exit_on_sigterm() {
    let (registered, ready) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime starts");
        runtime.block_on(async {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM is watched");
            registered.send(()).ok();
            terminate.recv().await;
            std::process::exit(0);
        });
    });
    ready.recv().ok();
}

fn main() -> ExitCode {
    STARTED_WITH.set(open_descriptors()).ok();
    let served = rdlt_connector::serve::<Scripted>();
    if LINGER.get().is_some() {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    }
    served
}

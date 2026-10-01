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
    let served = rdlt_connector::serve::<Scripted>();
    if LINGER.get().is_some() {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    }
    served
}

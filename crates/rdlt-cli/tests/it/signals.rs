//! `rdlt run` stopped by signals: one stops after a commit, two at once, and each exits with 128
//! and the first signal.

use std::path::PathBuf;
use std::time::Duration;

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStderr};

use crate::support::{Scratch, guarded};

/// The whole of any one test here, however its steps go, so that it fails rather than hangs.
const WHOLE_TEST: Duration = Duration::from_secs(240);

/// A wait of a test here for something the run says it did.
const SAYS: Duration = Duration::from_secs(120);

/// A pipeline that follows an unbounded log into SQLite, committing every second: its document's
/// path, and its database's.
fn following(scratch: &Scratch) -> (String, PathBuf) {
    let database = scratch.path().join("followed.db");
    let document = scratch.document(
        "followed.yaml",
        &format!(
            "version: 1\npipeline: followed\nsource:\n  connector: io.rapidbyte.log\n  config: {{ seed: 3, streams: [{{ name: events, partitions: 1, messages: 100000000, per_second: 1000, bounded: false }}] }}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {} }}\nstreams:\n  - name: events\n    read: incremental\ncommit: {{ every: 1s }}\nuntil: forever\n",
            database.display()
        ),
    );
    (
        document.to_str().expect("a UTF-8 path").to_owned(),
        database,
    )
}

/// Waits for `stderr` to say `what`, within [`SAYS`].
async fn said(stderr: &mut Lines<BufReader<ChildStderr>>, what: &str) {
    let telling = async {
        while let Some(line) = stderr.next_line().await.expect("stderr reads") {
            if line.contains(what) {
                return;
            }
        }
        panic!("rdlt ended without saying: {what}");
    };
    tokio::time::timeout(SAYS, telling)
        .await
        .expect("it said so within its bound");
}

/// Spawns `rdlt run` on `document` with `args` and answers it, guarded, with its standard error's
/// lines.
fn spawned(
    scratch: &Scratch,
    document: &str,
    args: &[&str],
) -> (Child, Lines<BufReader<ChildStderr>>) {
    let run = [&["run", document][..], args].concat();
    let mut child = scratch.command(&run).spawn().expect("rdlt starts");
    guarded(&child);
    let stderr = BufReader::new(child.stderr.take().expect("piped")).lines();
    (child, stderr)
}

fn pid(child: &Child) -> Pid {
    let id = child.id().expect("it runs");
    Pid::from_raw(i32::try_from(id).expect("a process's id is an i32"))
}

fn following_scratch() -> Scratch {
    Scratch::new()
        .connector("serve_log", "log")
        .connector("serve_reference", "sqlite")
}

/// Starts `rdlt run` on the following pipeline, waits for its first commit, sends `signal`, and
/// answers its exit code and the run's outcome.
async fn stopped(signal: Signal) -> (Option<i32>, serde_json::Value) {
    let scratch = following_scratch();
    let (document, _) = following(&scratch);
    let (child, mut stderr) = spawned(&scratch, &document, &["--output", "json"]);
    said(&mut stderr, "committed").await;
    kill(pid(&child), signal).expect("signalled");
    let output = tokio::time::timeout(SAYS, child.wait_with_output())
        .await
        .expect("rdlt ends")
        .expect("its output reads");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("a JSON outcome");
    (output.status.code(), report)
}

#[tokio::test]
async fn one_sigint_stops_cleanly_after_a_commit_and_exits_130() {
    let (code, outcome) = tokio::time::timeout(WHOLE_TEST, stopped(Signal::SIGINT))
        .await
        .expect("a test here ends within its limit");
    assert_eq!(code, Some(130));
    assert_eq!(outcome["report"]["status"], "stopped");
}

#[tokio::test]
async fn sigterm_exits_143() {
    let (code, _) = tokio::time::timeout(WHOLE_TEST, stopped(Signal::SIGTERM))
        .await
        .expect("a test here ends within its limit");
    assert_eq!(code, Some(143));
}

/// While this connection holds SQLite's write lock no commit lands, so the first signal cannot
/// end the run however long it waits: only the second can, and the test ends only if it does.
async fn two_presses() {
    let scratch = following_scratch();
    let (document, database) = following(&scratch);
    let (child, mut stderr) = spawned(&scratch, &document, &["--output", "json"]);
    said(&mut stderr, "committed").await;
    let hold = rusqlite::Connection::open(&database).expect("the database opens");
    hold.busy_timeout(SAYS).expect("the lock is waited for");
    hold.execute_batch("BEGIN IMMEDIATE")
        .expect("the write lock is taken");
    let run = pid(&child);
    kill(run, Signal::SIGINT).expect("signalled");
    said(&mut stderr, "stopping after the next commit").await;
    kill(run, Signal::SIGINT).expect("signalled again");
    let output = tokio::time::timeout(SAYS, child.wait_with_output())
        .await
        .expect("the second signal ends the run")
        .expect("its output reads");
    // The hold outlives the process, so a second signal that cancelled nothing fails above.
    drop(hold);
    assert_eq!(output.status.code(), Some(130));
    let outcome: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("a JSON outcome");
    assert_eq!(outcome["report"]["status"], "cancelled", "{outcome}");
}

#[tokio::test]
async fn two_sigints_stop_at_once_and_exit_130() {
    tokio::time::timeout(WHOLE_TEST, two_presses())
        .await
        .expect("a test here ends within its limit");
}

/// A signal while connectors are placed ends the run promptly; the connectors it spawned are
/// stopped by what the command line holds, whose stopping of every process group `rdlt-host`'s
/// own tests hold.
#[tokio::test]
async fn a_sigint_during_placement_exits_130_promptly() {
    // The destination's binary never answers its configuration, so placement waits on it.
    let scratch = Scratch::new()
        .connector("serve_source", "generator")
        .connector("serve_hang", "memory");
    let document = scratch.document(
        "hung.yaml",
        "version: 1\npipeline: hung\nsource:\n  connector: io.rapidbyte.generator\n  config: { seed: 1, streams: [{ name: events, rows: 10 }] }\ndestination:\n  connector: io.rapidbyte.memory\n  config: { store: hung }\nstreams: [{ name: events }]\n",
    );
    let mut child = scratch
        .command(&["run", document.to_str().unwrap()])
        .spawn()
        .expect("rdlt starts");
    guarded(&child);
    tokio::time::sleep(Duration::from_secs(2)).await;
    kill(pid(&child), Signal::SIGINT).expect("signalled");
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("rdlt ends promptly")
        .expect("it ends");
    assert_eq!(status.code(), Some(130));
}

/// A pipe whose buffer is full and whose reader reads nothing: its write end, for a child's
/// standard output, and the reader, to hold while the child runs.
async fn stuck_pipe() -> (std::process::Stdio, tokio::net::unix::pipe::Receiver) {
    let (writer, reader) = tokio::net::unix::pipe::pipe().expect("a pipe");
    writer.writable().await.expect("an empty pipe is writable");
    let mut written = 0;
    loop {
        match writer.try_write(&[b'x'; 4_096]) {
            Ok(wrote) => written += wrote,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("the pipe fills: {error}"),
        }
    }
    assert!(written >= 4_096, "the pipe is full");
    let blocking = writer.into_blocking_fd().expect("the pipe blocks again");
    (std::process::Stdio::from(blocking), reader)
}

/// A reader of the events that stops reading holds up neither the run nor its signals: the run
/// commits on, the first signal is heard, and the exit comes within the wait for what was said.
#[tokio::test]
async fn a_stuck_event_reader_holds_up_neither_the_run_nor_the_exit() {
    let scratch = following_scratch();
    let (document, _) = following(&scratch);
    let (stdout, _reader) = stuck_pipe().await;
    let mut command = scratch.command(&["run", &document, "--events", "-"]);
    command.stdout(stdout);
    let mut child = command.spawn().expect("rdlt starts");
    guarded(&child);
    let mut stderr = BufReader::new(child.stderr.take().expect("piped")).lines();
    tokio::time::timeout(WHOLE_TEST, async {
        said(&mut stderr, "committed").await;
        said(&mut stderr, "committed").await;
        kill(pid(&child), Signal::SIGINT).expect("signalled");
        let status = tokio::time::timeout(SAYS, child.wait())
            .await
            .expect("a stuck reader does not hold the exit")
            .expect("it ends");
        assert_eq!(status.code(), Some(130));
    })
    .await
    .expect("a test here ends within its limit");
}

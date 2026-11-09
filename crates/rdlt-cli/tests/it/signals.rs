//! `rdlt run` stopped by signals: one stops after a commit, two at once, and each exits with 128
//! and the first signal.

use std::path::PathBuf;
use std::time::Duration;

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStderr};

use crate::support::{SAYS, Scratch, bounded, guarded, said};
use crate::verbs::{memory_document, reference_connectors};

/// The whole of any one test here, however its steps go, so that it fails rather than hangs.
const WHOLE_TEST: Duration = Duration::from_secs(240);

/// A pipeline that follows an unbounded log into SQLite, committing every second: its document's
/// path, and its database's.
pub(crate) fn following(scratch: &Scratch) -> (String, PathBuf) {
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

pub(crate) fn pid(child: &Child) -> Pid {
    let id = child.id().expect("it runs");
    Pid::from_raw(i32::try_from(id).expect("a process's id is an i32"))
}

pub(crate) fn following_scratch() -> Scratch {
    Scratch::new()
        .connector("serve_log", "log")
        .connector("serve_reference", "sqlite")
}

/// Starts `rdlt run` on the following pipeline, waits for its first commit, sends `signal`, and
/// answers its exit code and the run's outcome.
async fn stopped(signal: Signal) -> (Option<i32>, serde_json::Value) {
    let scratch = following_scratch();
    let (document, database) = following(&scratch);
    let (child, mut stderr) = spawned(&scratch, &document, &["--output", "json"]);
    said(&mut stderr, "committed").await;
    kill(pid(&child), signal).expect("signalled");
    let output = tokio::time::timeout(SAYS, child.wait_with_output())
        .await
        .expect("rdlt ends")
        .expect("its output reads");
    let outcome: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("a JSON outcome");
    // What a stop reports is what the destination holds, once and exactly.
    let held: i64 = rusqlite::Connection::open(&database)
        .expect("the database opens")
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .expect("the rows count");
    assert_eq!(
        serde_json::json!(held),
        outcome["report"]["rows"],
        "{outcome}"
    );
    (output.status.code(), outcome)
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

/// The rows the line `line` says a commit landed, where it says that.
fn committed_in(line: &str) -> Option<i64> {
    line.split("committed ")
        .nth(1)?
        .split(' ')
        .next()?
        .parse()
        .ok()
}

/// Reads `stderr` until `told` and the commits it tells of come to `rows` rows, within
/// [`SAYS`], and answers their sum: every commit that landed in those rows has been heard of.
async fn told_of_rows(stderr: &mut Lines<BufReader<ChildStderr>>, mut told: i64, rows: i64) -> i64 {
    let telling = async {
        while told < rows {
            let line = stderr.next_line().await.expect("stderr reads");
            let line = line.expect("rdlt ended before telling of what it committed");
            told += committed_in(&line).unwrap_or(0);
        }
        told
    };
    tokio::time::timeout(SAYS, telling)
        .await
        .expect("every commit is told of within its bound")
}

/// While this connection holds SQLite's write lock no commit lands, so before the destination's
/// own busy wait passes, about 30 s, the first signal alone cannot end the run: only the second
/// can, and `cancelled` says it did.
async fn two_presses() {
    let scratch = following_scratch();
    let (document, database) = following(&scratch);
    let (child, mut stderr) = spawned(&scratch, &document, &["--output", "json"]);
    let told = told_of_rows(&mut stderr, 0, 1).await;
    let hold = rusqlite::Connection::open(&database).expect("the database opens");
    hold.busy_timeout(SAYS).expect("the lock is waited for");
    hold.execute_batch("BEGIN IMMEDIATE")
        .expect("the write lock is taken");
    let landed: i64 = hold
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .expect("the rows count");
    // A commit that landed before the lock has been told of, so the stop must make a new one.
    told_of_rows(&mut stderr, told, landed).await;
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

/// The children of the process `parent`, as `pgrep` lists them.
fn children(parent: u32) -> Vec<i32> {
    let listed = std::process::Command::new("pgrep")
        .args(["-P", &parent.to_string()])
        .output()
        .expect("pgrep runs");
    String::from_utf8_lossy(&listed.stdout)
        .lines()
        .filter_map(|pid| pid.trim().parse().ok())
        .collect()
}

/// A signal while connectors are placed ends the verb promptly, and leaves none of what it
/// spawned running, `run`, `check` and any other verb that places them alike.
#[tokio::test]
async fn a_sigint_during_placement_exits_130_promptly_and_leaves_no_connector() {
    // The destination's binary never answers its configuration, so placement waits on it.
    let scratch = Scratch::new()
        .connector("serve_source", "generator")
        .connector("serve_hang", "memory");
    let document = scratch.document(
        "hung.yaml",
        "version: 1\npipeline: hung\nsource:\n  connector: io.rapidbyte.generator\n  config: { seed: 1, streams: [{ name: events, rows: 10 }] }\ndestination:\n  connector: io.rapidbyte.memory\n  config: { store: hung }\nstreams: [{ name: events }]\n",
    );
    for verb in ["run", "check"] {
        let child = scratch
            .command(&[verb, document.to_str().expect("a UTF-8 path")])
            .spawn()
            .expect("rdlt starts");
        guarded(&child);
        let rdlt = child.id().expect("it runs");
        // Both connectors have started: the destination's is waited on.
        let spawned = bounded(async {
            loop {
                let spawned = children(rdlt);
                if spawned.len() >= 2 {
                    break spawned;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        kill(pid(&child), Signal::SIGINT).expect("signalled");
        let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
            .await
            .expect("rdlt ends promptly")
            .expect("it ends");
        assert_eq!(output.status.code(), Some(130), "{verb}");
        let said: serde_json::Value = serde_json::from_slice(&output.stdout).expect("one value");
        assert_eq!(said["error"]["kind"], "signal", "{verb}: {said}");
        let gone = spawned.iter().map(|connector| Pid::from_raw(*connector));
        bounded(async {
            for connector in gone {
                while kill(connector, None).is_ok() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        })
        .await;
    }
}

/// `--events` naming a FIFO no one reads hangs its open on the blocking pool, where a run's
/// exit does not wait for it: a signal ends the process at once.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_hung_open_of_the_events_file_cannot_hold_the_exit() {
    let scratch = following_scratch();
    let (document, _) = following(&scratch);
    let fifo = scratch.path().join("events.fifo");
    let made = std::process::Command::new("mkfifo").arg(&fifo).status();
    assert!(made.expect("mkfifo runs").success(), "mkfifo makes a pipe");
    let mut child = scratch
        .command(&[
            "run",
            &document,
            "--events",
            fifo.to_str().expect("a UTF-8 path"),
        ])
        .spawn()
        .expect("rdlt starts");
    guarded(&child);
    let tasks = format!("/proc/{}/task", child.id().expect("it runs"));
    bounded(async {
        loop {
            let waits = std::fs::read_dir(&tasks).expect("its threads list");
            let hung = waits.flatten().any(|task| {
                std::fs::read_to_string(task.path().join("wchan")).is_ok_and(|wait| {
                    wait.contains("wait_for_partner") || wait.contains("fifo_open")
                })
            });
            if hung {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    kill(pid(&child), Signal::SIGINT).expect("signalled");
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("a hung open does not hold the exit")
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
        let signalled = std::time::Instant::now();
        kill(pid(&child), Signal::SIGINT).expect("signalled");
        // The next commit, and the five seconds each of the last writes may take.
        let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
            .await
            .expect("a stuck reader does not hold the exit")
            .expect("it ends");
        assert_eq!(status.code(), Some(130));
        // A reader that stopped is still given its time to catch up.
        let waited = signalled.elapsed();
        assert!(waited >= Duration::from_secs(4), "it waited {waited:?}");
    })
    .await
    .expect("a test here ends within its limit");
}

/// Started as `nohup` starts it, with hangups ignored, a run goes on through one: the operator
/// who logged out has not asked it to stop.
///
/// macOS has no `/proc` that says a signal is ignored.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_run_started_under_nohup_survives_a_hangup() {
    tokio::time::timeout(WHOLE_TEST, async {
        let scratch = following_scratch();
        let (document, _) = following(&scratch);
        let mut child = scratch
            .wrapped("nohup", &["run", &document, "--output", "json"])
            .spawn()
            .expect("rdlt starts under nohup");
        guarded(&child);
        let mut stderr = BufReader::new(child.stderr.take().expect("piped")).lines();
        said(&mut stderr, "committed").await;
        let run = pid(&child);
        kill(run, Signal::SIGHUP).expect("hung up on");
        said(&mut stderr, "committed").await;
        said(&mut stderr, "committed").await;
        kill(run, Signal::SIGTERM).expect("terminated");
        let status = bounded(child.wait()).await.expect("the run ends");
        assert_eq!(status.code(), Some(143), "the hangup stopped nothing");
    })
    .await
    .expect("a test here ends within its limit");
}

/// A run that ends with a reader of its events stuck exits by its own outcome: the reader lost
/// only what it left.
#[tokio::test]
async fn a_stuck_event_reader_of_a_run_that_ends_exits_by_its_outcome() {
    tokio::time::timeout(WHOLE_TEST, async {
        let scratch = reference_connectors();
        let document = memory_document(&scratch, "stuck", "[{ id: 1 }]");
        let (stdout, _reader) = stuck_pipe().await;
        let mut command = scratch.command(&[
            "run",
            document.to_str().expect("a UTF-8 path"),
            "--events",
            "-",
        ]);
        command.stdout(stdout);
        let mut child = command.spawn().expect("rdlt starts");
        guarded(&child);
        let status = bounded(child.wait()).await.expect("it ends");
        assert_eq!(status.code(), Some(0), "a stuck reader is no failure");
    })
    .await
    .expect("a test here ends within its limit");
}

/// An events file that stalls is a failure to write: the run is over and the file is not.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_events_file_that_stalls_exits_74() {
    tokio::time::timeout(WHOLE_TEST, async {
        let scratch = reference_connectors();
        let document = memory_document(&scratch, "stalled", "[{ id: 1 }]");
        let fifo = scratch.path().join("stalled.fifo");
        let made = std::process::Command::new("mkfifo").arg(&fifo).status();
        assert!(made.expect("mkfifo runs").success(), "mkfifo makes a pipe");
        // Held full by this process, which reads none of it: the run's writes stall.
        let filler = tokio::net::unix::pipe::OpenOptions::new()
            .read_write(true)
            .open_sender(&fifo)
            .expect("a pipe open both ways");
        filler.writable().await.expect("an empty pipe is writable");
        while filler.try_write(&[b'x'; 4_096]).is_ok() {}
        let output = bounded(crate::support::output_of(&mut scratch.command(&[
            "run",
            document.to_str().expect("a UTF-8 path"),
            "--events",
            fifo.to_str().expect("a UTF-8 path"),
            "--output",
            "json",
        ])))
        .await;
        assert_eq!(output.status.code(), Some(74), "{output:?}");
        drop(filler);
    })
    .await
    .expect("a test here ends within its limit");
}

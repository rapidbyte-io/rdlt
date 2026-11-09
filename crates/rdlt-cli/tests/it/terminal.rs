//! `rdlt run` at a terminal: its results in plain text, a status line that redraws, and a note
//! on a line of its own, through a pseudo-terminal.

use std::io::Read as _;
use std::thread::JoinHandle;

use nix::pty::openpty;
use nix::sys::signal::{Signal, kill};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::signals::{following, following_scratch, pid};
use crate::support::{bounded, guarded};
use crate::verbs::{memory_document, reference_connectors};

/// The sequence that clears a terminal's line, which the status line and every note begin with.
const CLEAR: &str = "\r\x1b[2K";

/// Starts `command` with a new pseudo-terminal as its standard output and error: the process,
/// what it writes in chunks, and the thread reading them, which ends when it does.
fn on_a_terminal(
    command: &mut Command,
) -> (Child, mpsc::UnboundedReceiver<Vec<u8>>, JoinHandle<()>) {
    let pty = openpty(None, None).expect("a pseudo-terminal");
    // Copies closed on exec, as openpty's are not: else the next process this test spawns holds
    // the terminal open past the end of the run, and its reader never ends.
    let master = pty.master.try_clone().expect("a descriptor");
    let terminal = pty.slave.try_clone().expect("a descriptor");
    drop((pty.master, pty.slave));
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(
            terminal.try_clone().expect("a descriptor"),
        ))
        .stderr(std::process::Stdio::from(terminal));
    let child = command.spawn().expect("rdlt starts");
    guarded(&child);
    let (chunks, read) = mpsc::unbounded_channel();
    let mut master = std::fs::File::from(master);
    let reading = std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        while let Ok(count) = master.read(&mut buffer) {
            if count == 0 || chunks.send(buffer[..count].to_vec()).is_err() {
                break;
            }
        }
    });
    (child, read, reading)
}

/// What a run writes to a terminal until it ends, signalled once its status line counts a commit,
/// and how it ended.
async fn watched(
    mut child: Child,
    mut read: mpsc::UnboundedReceiver<Vec<u8>>,
) -> (String, std::process::ExitStatus) {
    let run = pid(&child);
    let mut seen = String::new();
    let mut signalled = false;
    while let Some(chunk) = read.recv().await {
        seen.push_str(&String::from_utf8_lossy(&chunk));
        let committed = seen
            .split("rows committed in ")
            .skip(1)
            .any(|rest| !rest.starts_with("0 "));
        if committed && !signalled {
            signalled = true;
            kill(run, Signal::SIGINT).expect("signalled");
        }
    }
    let status = child.wait().await.expect("it ends");
    (seen, status)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_at_a_terminal_has_a_status_line_its_notes_and_a_plain_result() {
    let scratch = following_scratch();
    let (document, _) = following(&scratch);
    let mut command = scratch.command(&["run", &document]);
    let (child, read, reading) = on_a_terminal(&mut command);
    // The command held its copies of the terminal: with them gone, its end is the process's.
    drop(command);
    let (seen, status) = bounded(watched(child, read)).await;
    reading.join().expect("the reading thread ends");
    assert_eq!(status.code(), Some(130), "{seen:?}");
    assert!(
        seen.contains(&format!("{CLEAR}Reading")),
        "a status line, redrawn: {seen:?}"
    );
    assert!(
        seen.contains(&format!("{CLEAR}stopping after the next commit")),
        "a note on a line of its own: {seen:?}"
    );
    assert!(
        !seen.contains("attempt 1: committed"),
        "no line per event on a terminal: {seen:?}"
    );
    // The result is plain, standard output being a terminal.
    assert!(seen.contains("Stopped: committed "), "{seen:?}");
    assert!(!seen.contains("\"report\""), "{seen:?}");
}

/// The status line is ended before the result, so the two are never on one line.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_ends_at_a_terminal_ends_its_status_line_before_its_result() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "ended", "[{ id: 1 }]");
    let mut command = scratch.command(&["run", document.to_str().expect("a UTF-8 path")]);
    let (child, mut read, reading) = on_a_terminal(&mut command);
    drop(command);
    let mut seen = String::new();
    let status = bounded(async {
        let mut child = child;
        while let Some(chunk) = read.recv().await {
            seen.push_str(&String::from_utf8_lossy(&chunk));
        }
        child.wait().await.expect("it ends")
    })
    .await;
    reading.join().expect("the reading thread ends");
    assert_eq!(status.code(), Some(0), "{seen:?}");
    assert!(seen.contains("\r\nSucceeded: committed "), "{seen:?}");
}

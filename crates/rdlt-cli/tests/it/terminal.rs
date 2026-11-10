//! What a person sees at a terminal: `rdlt` through a pseudo-terminal, its screen read back
//! through `vt100`, and what it writes off a terminal held to its contract.

use std::io::{Read as _, Write as _};
use std::os::fd::OwnedFd;
use std::process::{ExitStatus, Stdio};
use std::thread::JoinHandle;
use std::time::Duration;

use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::pty::{self, Winsize};
use nix::sys::signal::{self, Signal};
use nix::sys::stat::Mode;
use nix::unistd;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::signals::{children, following, following_scratch, pid};
use crate::support::{SAYS, Scratch, bounded, example, guarded, output_of};
use crate::verbs::{memory_document, reference_connectors};

/// A terminal that shows UTF-8, in colour.
const UTF8: [(&str, &str); 2] = [("TERM", "xterm-256color"), ("LC_ALL", "C.UTF-8")];

/// A terminal of `rows` rows and `columns` columns.
#[derive(Clone, Copy)]
struct Size {
    rows: u16,
    columns: u16,
}

/// What most tests of this file look at their output on.
const STANDARD: Size = Size {
    rows: 40,
    columns: 200,
};

/// The titles a screen was told to set, which a real terminal would have obeyed.
#[derive(Default)]
struct Titles(Vec<Vec<u8>>);

impl vt100::Callbacks for Titles {
    fn set_window_title(&mut self, _: &mut vt100::Screen, title: &[u8]) {
        self.0.push(title.to_vec());
    }
}

/// A pseudo-terminal, closed on exec as `openpty` does not leave it: its master, and its slave
/// for a child's standard streams.
fn pseudo_terminal(size: Size) -> (OwnedFd, OwnedFd) {
    let winsize = Winsize {
        ws_row: size.rows,
        ws_col: size.columns,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let opened = pty::openpty(Some(&winsize), None).expect("a pseudo-terminal");
    // Copies closed on exec: else the next process this test spawns holds the terminal open past
    // the end of the run, and its reader never ends.
    let master = opened.master.try_clone().expect("a descriptor");
    let slave = opened.slave.try_clone().expect("a descriptor");
    (master, slave)
}

/// `command` spawned with `slave` as its standard streams, which end with the process.
fn spawned_on(mut command: Command, slave: &OwnedFd) -> Child {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(slave.try_clone().expect("a descriptor")))
        .stderr(Stdio::from(slave.try_clone().expect("a descriptor")));
    let child = command.spawn().expect("rdlt starts");
    guarded(&child);
    child
}

/// `rdlt` on a pseudo-terminal, and the screen it has drawn so far.
struct Screened {
    child: Child,
    parser: vt100::Parser<Titles>,
    read: mpsc::UnboundedReceiver<Vec<u8>>,
    reading: JoinHandle<()>,
    /// Every byte the process has written so far.
    written: Vec<u8>,
}

impl Screened {
    /// Starts `command` on a new terminal of `size`.
    fn started(command: Command, size: Size) -> Self {
        let (master, slave) = pseudo_terminal(size);
        let child = spawned_on(command, &slave);
        drop(slave);
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
        Self {
            child,
            parser: vt100::Parser::new_with_callbacks(
                std::num::NonZeroU16::new(size.rows).expect("a screen has rows"),
                std::num::NonZeroU16::new(size.columns).expect("a screen has columns"),
                0,
                Titles::default(),
            ),
            read,
            reading,
            written: Vec::new(),
        }
    }

    fn screen(&self) -> &vt100::Screen {
        self.parser.screen()
    }

    /// Reads until `seen` holds of the screen, within the bound on what a test waits for.
    async fn until(&mut self, what: &str, mut seen: impl FnMut(&vt100::Screen) -> bool) {
        bounded(async {
            let mut held = seen(self.parser.screen());
            while !held {
                let Some(chunk) = self.read.recv().await else {
                    panic!(
                        "rdlt ended before {what}: {}",
                        self.parser.screen().contents()
                    );
                };
                self.written.extend_from_slice(&chunk);
                // Looked at between escapes, so a frame the same chunk erases is still seen, and
                // the whole chunk is kept, so nothing the test later looks for is lost.
                for piece in chunk.split_inclusive(|byte| *byte == 0x1b) {
                    self.parser.process(piece);
                    held |= seen(self.parser.screen());
                }
            }
        })
        .await;
    }

    /// Waits for the end of the process and of its output, and answers how it ended and the
    /// screen it left.
    async fn finished(mut self) -> Finished {
        let status = bounded(async {
            while let Some(chunk) = self.read.recv().await {
                self.written.extend_from_slice(&chunk);
                self.parser.process(&chunk);
            }
            self.child.wait().await.expect("it ends")
        })
        .await;
        self.reading.join().expect("the reading thread ends");
        Finished {
            status,
            parser: self.parser,
            written: self.written,
        }
    }
}

/// How a process that ran on a terminal ended, and what it left there.
struct Finished {
    status: ExitStatus,
    parser: vt100::Parser<Titles>,
    written: Vec<u8>,
}

impl Finished {
    fn screen(&self) -> &vt100::Screen {
        self.parser.screen()
    }

    /// The screen's text, one element per row.
    fn rows(&self) -> Vec<String> {
        let columns = self.screen().size().1.get();
        self.screen().rows(0, columns).collect()
    }
}

/// `rdlt args` on a terminal of `size`, its variables `extra` beside a UTF-8 terminal's.
fn on_a_terminal(scratch: &Scratch, args: &[&str], size: Size, extra: &[(&str, &str)]) -> Screened {
    let mut command = scratch.command(args);
    command.envs(UTF8).envs(extra.iter().copied());
    Screened::started(command, size)
}

/// A screen's text with what differs from run to run replaced: durations, rates, bytes, paths.
fn normalized(screen: &vt100::Screen, scratch: &Scratch) -> String {
    let directory = scratch.path().to_string_lossy().into_owned();
    let mut text = screen.contents().replace(&directory, "<scratch>");
    for (pattern, name) in [
        (r"\d+(\.\d+)?(ms|µs|s)\b", "<time>"),
        (r"\d+(\.\d+)?[kM]? rows/s", "<rate>"),
        (r"\d+(\.\d+)? (B|KiB|MiB|GiB)\b", "<bytes>"),
    ] {
        let regex = regex::Regex::new(pattern).expect("a pattern that parses");
        text = regex.replace_all(&text, name).into_owned();
    }
    text.trim_end().to_owned()
}

/// A row of `screen` reaching its last column: a line written to the last column could wrap, and
/// nothing rdlt writes may.
fn row_reaching_the_edge(screen: &vt100::Screen) -> Option<String> {
    let columns = screen.size().1.get();
    screen
        .rows(0, columns)
        .find(|row| row.trim_end().chars().count() >= usize::from(columns))
}

/// Whether the screen shows a run that has committed some rows.
fn committed_rows(screen: &vt100::Screen) -> bool {
    screen
        .contents()
        .lines()
        .any(|line| line.contains(" committed") && !line.contains(" 0 rows committed"))
}

/// Asserts that a process wrote no spinner and moved no cursor: nothing was animated.
fn assert_not_animated(finished: &Finished) {
    let written = String::from_utf8_lossy(&finished.written);
    assert!(
        !written.contains("\u{1b}[1A") && !written.contains("\u{1b}[2K"),
        "a redraw moved over the terminal: {written:?}"
    );
    assert!(
        !written.contains(|c: char| ('\u{2800}'..='\u{28ff}').contains(&c)),
        "a spinner was drawn: {written:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_ends_leaves_only_its_summary_on_the_screen() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "ended", "[{ id: 1 }, { id: 2 }, { id: 3 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let finished = on_a_terminal(&scratch, &["run", document], STANDARD, &[])
        .finished()
        .await;
    assert_eq!(finished.status.code(), Some(0));
    insta::assert_snapshot!(
        "run_succeeded_screen",
        normalized(finished.screen(), &scratch)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_shows_what_it_committed_and_one_press_stops_it_after_its_next_commit() {
    let scratch = following_scratch();
    let (document, _) = following(&scratch);
    let mut running = on_a_terminal(&scratch, &["run", &document], STANDARD, &[]);
    running
        .until("the first commit and its stream", |screen| {
            committed_rows(screen) && screen.contents().contains("events")
        })
        .await;
    let shown = running.screen().contents();
    assert!(shown.contains("followed"), "the pipeline is named: {shown}");
    assert!(shown.contains("log → sqlite"), "{shown}");
    assert!(
        shown.contains("Reading") || shown.contains("Committing"),
        "{shown}"
    );
    assert!(shown.contains("rows/s"), "{shown}");
    assert!(shown.contains("partition"), "{shown}");
    assert!(shown.contains("events"), "a line per stream: {shown}");
    assert!(
        !shown.contains("attempt 1: committed"),
        "no line per event on a terminal: {shown}"
    );
    signal::kill(pid(&running.child), Signal::SIGINT).expect("signalled");
    running
        .until("the stop is shown", |screen| {
            screen
                .contents()
                .contains("Stopping after the next commit (Ctrl-C again to stop now)")
        })
        .await;
    let finished = running.finished().await;
    assert_eq!(finished.status.code(), Some(130));
    let text = finished.screen().contents();
    assert!(text.contains("■ Stopped  followed"), "{text}");
    assert!(
        text.contains("Run it again to continue from the last commit."),
        "{text}"
    );
    assert!(text.contains("STREAM"), "the table of streams: {text}");
    assert!(
        !text.contains("Stopping after"),
        "what was live is gone: {text}"
    );
}

/// The phase line changes on each press: the run cannot end between them, as the commit the
/// first press waits for cannot land while this test holds the database's write lock.
#[tokio::test(flavor = "multi_thread")]
async fn two_presses_change_the_phase_line_as_each_is_heard() {
    let scratch = following_scratch();
    let (document, database) = following(&scratch);
    let mut running = on_a_terminal(&scratch, &["run", &document], STANDARD, &[]);
    running.until("the first commit", committed_rows).await;
    let hold = rusqlite::Connection::open(&database).expect("the database opens");
    hold.busy_timeout(SAYS).expect("the lock is waited for");
    hold.execute_batch("BEGIN IMMEDIATE")
        .expect("the write lock is taken");
    let run = pid(&running.child);
    signal::kill(run, Signal::SIGINT).expect("signalled");
    running
        .until("the first press is shown", |screen| {
            screen
                .contents()
                .contains("Stopping after the next commit (Ctrl-C again to stop now)")
        })
        .await;
    signal::kill(run, Signal::SIGINT).expect("signalled again");
    running
        .until("the second press is shown", |screen| {
            screen.contents().contains("Stopping now")
        })
        .await;
    let finished = running.finished().await;
    drop(hold);
    assert_eq!(finished.status.code(), Some(130));
    let text = finished.screen().contents();
    assert!(text.contains("■ Cancelled  followed"), "{text}");
}

/// A terminal whose output buffer is full and that nobody reads: every write to it blocks.
fn full_terminal(size: Size) -> (OwnedFd, OwnedFd) {
    let (master, slave) = pseudo_terminal(size);
    let mut writer = std::fs::File::from(slave.try_clone().expect("a descriptor"));
    fcntl(&writer, FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).expect("the terminal does not block");
    let mut written = 0;
    loop {
        match writer.write(&[b'x'; 4096]) {
            Ok(wrote) => written += wrote,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("the terminal fills: {error}"),
        }
    }
    fcntl(&writer, FcntlArg::F_SETFL(OFlag::empty())).expect("the terminal blocks again");
    assert!(written > 0, "the terminal is full");
    (master, slave)
}

/// What the painter's thread is for: a terminal that stopped reading blocks every write to it,
/// and neither a run's signals nor its exit may wait on that.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_nobody_reads_holds_up_neither_a_signal_nor_the_exit() {
    let scratch = following_scratch();
    let (document, _) = following(&scratch);
    let fifo = scratch.path().join("events.fifo");
    unistd::mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR).expect("a fifo");
    // A reader of the events, so the test hears of a commit while the terminal says nothing.
    let events = tokio::net::unix::pipe::OpenOptions::new()
        .open_receiver(&fifo)
        .expect("the fifo opens for reading");
    let mut events = BufReader::new(events).lines();
    let (master, slave) = full_terminal(STANDARD);
    let mut command = scratch.command(&[
        "run",
        &document,
        "--events",
        fifo.to_str().expect("a UTF-8 path"),
    ]);
    command.envs(UTF8);
    let mut child = spawned_on(command, &slave);
    bounded(async {
        while let Some(line) = events.next_line().await.expect("the events read") {
            if line.contains("\"committed\"") {
                return;
            }
        }
        panic!("the run ended without a commit");
    })
    .await;
    assert_exits_within_its_bound(&mut child).await;
    drop((master, slave));
}

/// The signal goes in, and the process exits with it within the waits its last writes to a stuck
/// terminal are given.
async fn assert_exits_within_its_bound(child: &mut Child) {
    let signalled = std::time::Instant::now();
    signal::kill(pid(child), Signal::SIGINT).expect("signalled");
    let status = tokio::time::timeout(Duration::from_secs(60), child.wait())
        .await
        .expect("a terminal nobody reads does not hold the exit")
        .expect("it ends");
    assert_eq!(status.code(), Some(130));
    assert!(
        signalled.elapsed() < Duration::from_secs(30),
        "it ended {:?} after the signal, past the bound its waits add to",
        signalled.elapsed()
    );
}

/// The same for a certification, whose last words have no run's bounded path to go through.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_nobody_reads_holds_up_neither_a_certification_s_signal_nor_its_exit() {
    let scratch = Scratch::new();
    let (master, slave) = full_terminal(STANDARD);
    let command = certifying(
        &scratch,
        r#"{"seed": 11, "streams": [{"name": "events", "rows": 40000, "partitions": 1, "batch_rows": 50}]}"#,
        &[],
    );
    let mut child = spawned_on(command, &slave);
    let rdlt = child.id().expect("it runs");
    bounded(async {
        while children(rdlt).is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_exits_within_its_bound(&mut child).await;
    drop((master, slave));
}

/// The run's lines reach no screen's last column, and streams past its height collapse.
#[tokio::test(flavor = "multi_thread")]
async fn a_narrow_screen_wraps_no_line_and_a_short_one_collapses_its_streams() {
    let scratch = following_scratch();
    let document = many_streams(&scratch, 8);
    for size in [
        Size {
            rows: 24,
            columns: 40,
        },
        Size {
            rows: 6,
            columns: 200,
        },
    ] {
        let mut running = on_a_terminal(&scratch, &["run", &document], size, &[]);
        let short = size.rows == 6;
        running
            .until("a commit, and the streams that fit", |screen| {
                committed_rows(screen) && (!short || screen.contents().contains("more"))
            })
            .await;
        assert_eq!(
            row_reaching_the_edge(running.screen()),
            None,
            "at {} columns:\n{}",
            size.columns,
            running.screen().contents()
        );
        signal::kill(pid(&running.child), Signal::SIGINT).expect("signalled");
        let finished = running.finished().await;
        assert_eq!(finished.status.code(), Some(130));
        assert_eq!(
            row_reaching_the_edge(finished.screen()),
            None,
            "the end at {} columns:\n{}",
            size.columns,
            finished.screen().contents()
        );
    }
}

/// A certification's clause lines and verdict reach no narrow screen's last column.
#[tokio::test(flavor = "multi_thread")]
async fn a_certification_wraps_its_lines_on_a_narrow_screen() {
    let scratch = Scratch::new();
    let narrow = Size {
        rows: 24,
        columns: 40,
    };
    let finished = certifying_on_a_terminal(
        &scratch,
        r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#,
        &["--require", "partial"],
        narrow,
    )
    .finished()
    .await;
    assert_eq!(finished.status.code(), Some(0));
    assert_eq!(
        row_reaching_the_edge(finished.screen()),
        None,
        "{}",
        finished.screen().contents()
    );
}

/// A document following `streams` unbounded logs into SQLite, committing every second.
fn many_streams(scratch: &Scratch, streams: usize) -> String {
    let logged: Vec<String> = (0..streams)
        .map(|index| {
            format!(
                "{{ name: stream_{index}, partitions: 1, messages: 100000000, per_second: 500, bounded: false }}"
            )
        })
        .collect();
    let chosen: Vec<String> = (0..streams)
        .map(|index| format!("{{ name: stream_{index}, read: incremental }}"))
        .collect();
    let document = scratch.document(
        "many.yaml",
        &format!(
            "version: 1\npipeline: many\nsource: {{ connector: io.rapidbyte.log, config: {{ seed: 3, streams: [{}] }} }}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {} }}\nstreams: [{}]\ncommit: {{ every: 1s }}\nuntil: forever\n",
            logged.join(", "),
            scratch.path().join("many.db").display(),
            chosen.join(", ")
        ),
    );
    document.to_str().expect("a UTF-8 path").to_owned()
}

/// What a run of a small pipeline leaves on a terminal given `args` and the variables `extra`.
async fn small_run(args: &[&str], extra: &[(&str, &str)]) -> Finished {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "coloured", "[{ id: 1 }]");
    let run = [&["run", document.to_str().expect("a UTF-8 path")], args].concat();
    on_a_terminal(&scratch, &run, STANDARD, extra)
        .finished()
        .await
}

/// The foreground colour of the mark that begins the row showing `text`.
fn mark_color(finished: &Finished, text: &str) -> vt100::Color {
    let rows = finished.rows();
    let row = rows
        .iter()
        .position(|row| row.contains(text))
        .unwrap_or_else(|| panic!("{text} is not shown: {rows:?}"));
    let row = u16::try_from(row).expect("a screen's row");
    finished
        .screen()
        .cell(row, 0)
        .expect("a row has a first cell")
        .fgcolor()
}

#[tokio::test(flavor = "multi_thread")]
async fn colour_follows_the_terminal_the_flag_and_the_environment() {
    let green = vt100::Color::Idx(2);
    let plain = small_run(&[], &[]).await;
    assert_eq!(mark_color(&plain, "Succeeded"), green);
    let refused = small_run(&[], &[("NO_COLOR", "1")]).await;
    assert_eq!(mark_color(&refused, "Succeeded"), vt100::Color::Default);
    let empty = small_run(&[], &[("NO_COLOR", "")]).await;
    assert_eq!(
        mark_color(&empty, "Succeeded"),
        green,
        "an empty NO_COLOR is none"
    );
    let never = small_run(&["--color", "never"], &[("CLICOLOR_FORCE", "1")]).await;
    assert_eq!(mark_color(&never, "Succeeded"), vt100::Color::Default);
    let both = small_run(&[], &[("NO_COLOR", "1"), ("CLICOLOR_FORCE", "1")]).await;
    assert_eq!(
        mark_color(&both, "Succeeded"),
        vt100::Color::Default,
        "NO_COLOR is checked first"
    );
}

/// `TERM=dumb` on a terminal: no colour, no animation, and a line per event, as off a terminal.
#[tokio::test(flavor = "multi_thread")]
async fn a_dumb_terminal_is_not_animated_and_has_no_colour() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "dumb", "[{ id: 1 }]");
    let mut command = scratch.command(&["run", document.to_str().expect("a UTF-8 path")]);
    command.env("TERM", "dumb").env("LC_ALL", "C.UTF-8");
    let mut running = Screened::started(command, STANDARD);
    running
        .until("its events are told", |screen| {
            screen.contents().contains("attempt 1: committed")
        })
        .await;
    let finished = running.finished().await;
    assert_eq!(finished.status.code(), Some(0));
    let text = finished.screen().contents();
    assert!(text.contains("✓ Succeeded"), "{text}");
    assert_eq!(
        mark_color(&finished, "Succeeded"),
        vt100::Color::Default,
        "{text}"
    );
}

/// A locale that does not say UTF-8 shows ASCII marks.
#[tokio::test(flavor = "multi_thread")]
async fn an_ascii_locale_shows_ascii_marks() {
    let finished = small_run(&[], &[("LC_ALL", "C")]).await;
    assert_eq!(finished.status.code(), Some(0));
    let text = finished.screen().contents();
    assert!(text.contains("ok Succeeded"), "{text}");
    assert!(!text.contains('✓') && !text.contains('·'), "{text}");
}

/// `--quiet` at a terminal: no progress at all, the result still printed, and none of what a
/// verb would go on to suggest.
#[tokio::test(flavor = "multi_thread")]
async fn quiet_at_a_terminal_shows_only_the_result() {
    let finished = small_run(&["--quiet"], &[]).await;
    assert_eq!(finished.status.code(), Some(0));
    assert_not_animated(&finished);
    let text = finished.screen().contents();
    assert!(text.contains("Succeeded"), "{text}");
    assert!(!text.contains("attempt"), "{text}");
    let scratch = Scratch::new();
    let finished = on_a_terminal(&scratch, &["init", "--quiet"], STANDARD, &[])
        .finished()
        .await;
    assert_eq!(finished.status.code(), Some(0));
    assert_not_animated(&finished);
    let text = finished.screen().contents();
    assert!(text.contains("Created"), "{text}");
    assert!(!text.contains("Next:"), "{text}");
}

/// A verb at a terminal exits as soon as it is done: the log writer's copy of what the painter
/// listens on does not keep it waiting for a disconnect that never comes.
#[tokio::test(flavor = "multi_thread")]
async fn a_verb_at_a_terminal_exits_well_inside_its_wait_for_the_terminal() {
    let scratch = Scratch::new();
    let started = std::time::Instant::now();
    let finished = on_a_terminal(&scratch, &["init"], STANDARD, &[])
        .finished()
        .await;
    assert_eq!(finished.status.code(), Some(0));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "init took {:?}, as if it had waited out the terminal",
        started.elapsed()
    );
}

/// Events that go to the terminal, by standard output or by a path that resolves to it, are not
/// animated over, whose redraw would move over rows it does not own, nor repeated as a line each.
#[tokio::test(flavor = "multi_thread")]
async fn events_on_the_terminal_are_neither_animated_over_nor_told_again() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "told", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    for sink in ["-", "/dev/stdout"] {
        let finished = on_a_terminal(
            &scratch,
            &["run", document, "--events", sink],
            STANDARD,
            &[],
        )
        .finished()
        .await;
        assert_eq!(finished.status.code(), Some(0), "{sink}");
        assert_not_animated(&finished);
        let written = String::from_utf8_lossy(&finished.written);
        assert!(written.contains("\"committed\""), "{sink}: {written}");
        assert!(!written.contains("attempt 1:"), "{sink}: {written}");
    }
}

/// A note nobody reads is given up on within its bound: that of a collector that cannot be set
/// up comes before anything else a verb writes, and only standard error is stuck here.
#[tokio::test(flavor = "multi_thread")]
async fn a_note_on_a_terminal_nobody_reads_is_given_up_on_within_its_bound() {
    let scratch = Scratch::new();
    let (master, slave) = full_terminal(STANDARD);
    let mut command = scratch.command(&["connectors", "--output", "json"]);
    command
        .envs(UTF8)
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "https://127.0.0.1:1")
        .env("OTEL_EXPORTER_OTLP_CERTIFICATE", "/nonexistent/ca.pem")
        .stderr(Stdio::from(slave.try_clone().expect("a descriptor")));
    let output = output_of(&mut command).await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    drop((master, slave));
}

/// A certification's JSON on a terminal nobody reads is given up on within its bound: the verb
/// exits by its outcome and not when the terminal reads again.
#[tokio::test(flavor = "multi_thread")]
async fn a_certification_s_result_on_a_terminal_nobody_reads_holds_up_nothing() {
    let scratch = Scratch::new();
    let (master, slave) = full_terminal(STANDARD);
    let command = certifying(
        &scratch,
        r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#,
        &["--require", "partial", "--output", "json"],
    );
    let mut child = spawned_on(command, &slave);
    let status = bounded(child.wait()).await.expect("it ends");
    assert_eq!(status.code(), Some(0));
    drop((master, slave));
}

/// Events that go to a file leave the terminal to the live region.
#[tokio::test(flavor = "multi_thread")]
async fn events_in_a_file_leave_the_terminal_animated() {
    let scratch = following_scratch();
    let (document, _) = following(&scratch);
    let events = scratch.path().join("events.ndjson");
    let events = events.to_str().expect("a UTF-8 path");
    let mut running = on_a_terminal(
        &scratch,
        &["run", &document, "--events", events],
        STANDARD,
        &[],
    );
    running.until("the live region", committed_rows).await;
    signal::kill(pid(&running.child), Signal::SIGINT).expect("signalled");
    assert_eq!(running.finished().await.status.code(), Some(130));
}

/// Logs asked for at a level, or through `RUST_LOG`, and export to a collector, are written
/// straight to the terminal: nothing animates over them.
#[tokio::test(flavor = "multi_thread")]
async fn nothing_animates_where_logs_write_to_the_terminal_on_their_own() {
    for (args, extra) in [
        (["--log-level", "debug"], []),
        (["--log-level", "trace"], []),
    ] {
        let finished = small_run(&args, &extra).await;
        assert_eq!(finished.status.code(), Some(0));
        assert_not_animated(&finished);
        // The log lines themselves, not the coloured result beside them on the same terminal.
        let written = String::from_utf8_lossy(&finished.written);
        let logs: Vec<&str> = written
            .lines()
            .filter(|line| line.contains("DEBUG"))
            .collect();
        assert!(!logs.is_empty(), "{written:?}");
        assert!(
            logs.iter().all(|line| line.contains('\u{1b}')),
            "logs at a colour terminal are coloured: {logs:?}"
        );
    }
    let finished = small_run(&[], &[("RUST_LOG", "error")]).await;
    assert_eq!(finished.status.code(), Some(0));
    assert_not_animated(&finished);
    let finished = small_run(
        &[],
        &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")],
    )
    .await;
    assert_eq!(finished.status.code(), Some(0));
    assert_not_animated(&finished);
}

/// A database the connector cannot open is hinted at, with room for the path being wrong.
#[tokio::test(flavor = "multi_thread")]
async fn a_database_that_cannot_be_opened_gets_a_hint_that_allows_the_path_is_wrong() {
    let scratch = reference_connectors();
    let path = scratch.path().join("absent").join("x.db");
    let document = scratch.document(
        "missing.yaml",
        &format!(
            "version: 1\npipeline: missing\nsource: {{ connector: io.rapidbyte.memory, config: {{ streams: {{ users: [{{ id: 1 }}] }} }} }}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {} }}\nstreams: [{{ name: users }}]\n",
            path.display()
        ),
    );
    let finished = on_a_terminal(
        &scratch,
        &["check", document.to_str().expect("a UTF-8 path")],
        STANDARD,
        &[],
    )
    .finished()
    .await;
    assert_eq!(finished.status.code(), Some(2));
    let text = finished.screen().contents();
    assert!(text.contains("the path may be wrong"), "{text}");
    assert!(text.contains("--writable"), "{text}");
}

/// Off a terminal `--quiet` drops the event lines and keeps the result.
#[tokio::test]
async fn quiet_off_a_terminal_drops_event_lines_and_keeps_the_result() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "quiet", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let output = scratch
        .rdlt(&["run", document, "--output", "plain", "--quiet"])
        .await;
    assert_eq!(output.status.code(), Some(0));
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(out.starts_with("Succeeded"), "{out}");
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(!err.contains("committed"), "{err}");
    // A note, of a collector that cannot be set up here, is dropped with the rest.
    let mut command = scratch.command(&["connectors", "--output", "json", "--quiet"]);
    command
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "https://127.0.0.1:1")
        .env("OTEL_EXPORTER_OTLP_CERTIFICATE", "/nonexistent/ca.pem");
    let said = output_of(&mut command).await;
    assert_eq!(said.status.code(), Some(0), "{said:?}");
    assert!(said.stderr.is_empty(), "{said:?}");
}

/// Logs off a terminal carry no colour.
#[tokio::test]
async fn logs_off_a_terminal_carry_no_colour() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "logged", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let output = scratch
        .rdlt(&["run", document, "--output", "json", "--log-level", "debug"])
        .await;
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(said.contains("DEBUG"), "{said}");
    assert!(!said.contains('\u{1b}'), "{said:?}");
}

/// Colour into a pipe comes only from the flag or `CLICOLOR_FORCE`, and never animates.
#[tokio::test]
async fn colour_into_a_pipe_comes_from_the_flag_alone_and_never_animates() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "piped", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let always = scratch
        .rdlt(&["run", document, "--output", "plain", "--color", "always"])
        .await;
    let coloured = String::from_utf8_lossy(&always.stdout).into_owned();
    assert!(coloured.contains("\u{1b}["), "{coloured:?}");
    let pattern = regex::Regex::new("\u{1b}\\[[0-9;]*m").expect("a pattern that parses");
    let plain = pattern.replace_all(&coloured, "");
    assert!(plain.starts_with("Succeeded: committed 1 rows"), "{plain}");
    let stderr = String::from_utf8_lossy(&always.stderr);
    assert!(!stderr.contains("\u{1b}["), "no animation: {stderr:?}");
    assert!(!stderr.contains('\r'), "no animation: {stderr:?}");
    let mut forced = scratch.command(&["run", document, "--output", "plain"]);
    forced.env("CLICOLOR_FORCE", "1");
    let forced = output_of(&mut forced).await;
    assert!(String::from_utf8_lossy(&forced.stdout).contains("\u{1b}["));
    let mut refused = scratch.command(&["run", document, "--output", "plain"]);
    refused.env("NO_COLOR", "1");
    let refused = output_of(&mut refused).await;
    assert!(!String::from_utf8_lossy(&refused.stdout).contains('\u{1b}'));
    let json = scratch
        .rdlt(&["run", document, "--output", "json", "--color", "always"])
        .await;
    let said: serde_json::Value = serde_json::from_slice(&json.stdout).expect("JSON, uncoloured");
    assert_eq!(said["report"]["status"], "succeeded");
}

/// Off a terminal nothing any verb prints holds an escape byte.
#[tokio::test]
async fn off_a_terminal_no_verb_writes_an_escape_byte() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "bytes", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let verbs: [&[&str]; 10] = [
        &["check", document],
        &["discover", document],
        &["run", document],
        &["connectors"],
        &["state", "reset", document, "users"],
        &["connector", "new", "elsewhere"],
        &["init", "starter.yaml"],
        &["schema", "--document"],
        &["completions", "bash"],
        &["run", "absent.yaml"],
    ];
    let config = scratch.document(
        "config.json",
        r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#,
    );
    let config = config.to_str().expect("a UTF-8 path");
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    let certify = [
        "certify",
        binary,
        "--role",
        "source",
        "--config-file",
        config,
        "--require",
        "partial",
        "--env",
        "LLVM_PROFILE_FILE",
        "--trust-binaries",
    ];
    for output in ["plain", "json"] {
        for verb in verbs {
            let args = [verb, &["--output", output]].concat();
            let said = scratch.rdlt(&args).await;
            assert_no_escape(&args, &said);
        }
        let args = [&certify[..], &["--output", output]].concat();
        let said = output_of(&mut scratch.bare_command(&args)).await;
        assert_no_escape(&args, &said);
        std::fs::remove_dir_all(scratch.path().join("rdlt-connector-elsewhere"))
            .expect("the project is removed");
        std::fs::remove_file(scratch.path().join("starter.yaml")).expect("the starter is removed");
    }
}

/// Asserts that `said`, what `args` printed, holds no escape byte.
fn assert_no_escape(args: &[&str], said: &std::process::Output) {
    for bytes in [&said.stdout, &said.stderr] {
        assert!(
            !bytes.contains(&0x1b),
            "{args:?}: {}",
            String::from_utf8_lossy(bytes)
        );
    }
}

/// Each verb's result and a failure with its hint, as a person reads them.
#[tokio::test(flavor = "multi_thread")]
async fn each_verb_s_result_and_a_failure_read_as_recorded() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "shown", "[{ id: 1 }, { id: 2 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let mut screens = Vec::new();
    let verbs: [(&str, Vec<&str>); 8] = [
        ("check", vec!["check", document]),
        ("discover", vec!["discover", document]),
        ("connectors", vec!["connectors"]),
        ("init", vec!["init", "written.yaml"]),
        ("connector new", vec!["connector", "new", "tickets"]),
        ("run, for the reset", vec!["run", document]),
        ("state reset", vec!["state", "reset", document, "users"]),
        ("failed check", vec!["check", "absent.yaml"]),
    ];
    for (name, args) in verbs {
        let finished = on_a_terminal(&scratch, &args, STANDARD, &[])
            .finished()
            .await;
        let expected = if name == "failed check" { 74 } else { 0 };
        assert_eq!(
            finished.status.code(),
            Some(expected),
            "{name}: {}",
            finished.screen().contents()
        );
        if name != "run, for the reset" {
            let shown = normalized(finished.screen(), &scratch);
            screens.push(format!("{name}:\n{shown}"));
        }
    }
    insta::assert_snapshot!("verbs_at_a_terminal", screens.join("\n\n"));
}

/// A connector, or a document, cannot set the title, clear the screen or fake a line.
#[tokio::test(flavor = "multi_thread")]
async fn what_a_connector_says_cannot_steer_the_terminal() {
    let scratch = reference_connectors().connector("serve_failing", "failing");
    let hostile = "here\\e[2J\\e]0;owned\\a\\e[1;1Hplanted";
    let database = scratch.path().join("hostile.db").display().to_string();
    let document = scratch.document(
        "hostile.yaml",
        &format!(
            "version: 1\npipeline: hostile\nsource: {{ connector: io.rapidbyte.failing, config: {{ fail: check, token: \"{hostile}\" }} }}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {database} }}\nstreams: [{{ name: rows }}]\n"
        ),
    );
    let finished = on_a_terminal(
        &scratch,
        &["check", document.to_str().expect("a UTF-8 path")],
        STANDARD,
        &[],
    )
    .finished()
    .await;
    assert_eq!(finished.status.code(), Some(2));
    assert_eq!(
        finished.parser.callbacks().0,
        Vec::<Vec<u8>>::new(),
        "the title stayed"
    );
    let text = finished.screen().contents();
    assert!(
        text.contains("the check refused"),
        "the error is there: {text}"
    );
    assert!(text.contains("\\u{1b}"), "shown, not obeyed: {text}");
    assert_eq!(
        finished.screen().cursor_position().1,
        0,
        "the cursor ended a line of its own: {text}"
    );
    let bad_name = "\"\\e[2Jusers\"";
    let unwelcome = scratch.document(
        "unwelcome.yaml",
        &format!(
            "version: 1\npipeline: unwelcome\nsource: {{ connector: io.rapidbyte.memory, config: {{ streams: {{ {bad_name}: [{{ id: 1 }}] }} }} }}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {database} }}\nstreams: [{{ name: {bad_name} }}]\n"
        ),
    );
    let refused = on_a_terminal(
        &scratch,
        &["check", unwelcome.to_str().expect("a UTF-8 path")],
        STANDARD,
        &[],
    )
    .finished()
    .await;
    assert_ne!(refused.status.code(), Some(0));
    assert_eq!(refused.parser.callbacks().0, Vec::<Vec<u8>>::new());
    let rows = refused.rows();
    assert!(
        rows.iter().any(|row| row.contains("\\u{1b}")),
        "the name is shown, not obeyed, and the screen was not cleared: {rows:?}"
    );
}

/// `rdlt certify` of the generator source, with configuration `config` and `args`.
fn certifying(scratch: &Scratch, config: &str, args: &[&str]) -> Command {
    certifying_target(scratch, &example("serve_source"), config, args)
}

/// `rdlt certify` of the source `target`, with configuration `config` and `args`.
fn certifying_target(
    scratch: &Scratch,
    target: &std::path::Path,
    config: &str,
    args: &[&str],
) -> Command {
    let file = scratch.document("config.json", config);
    let mut command = Command::new(env!("CARGO_BIN_EXE_rdlt"));
    command
        .arg("certify")
        .arg(target)
        .args(["--role", "source", "--trust-binaries", "--config-file"])
        .arg(file)
        .args(["--env", "LLVM_PROFILE_FILE"])
        .args(args)
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .env_remove("CLICOLOR_FORCE")
        .env_remove("NO_COLOR")
        .envs(UTF8)
        .process_group(0)
        .kill_on_drop(true);
    command
}

/// `rdlt certify` of the generator source, on a terminal of `size`.
fn certifying_on_a_terminal(
    scratch: &Scratch,
    config: &str,
    args: &[&str],
    size: Size,
) -> Screened {
    Screened::started(certifying(scratch, config, args), size)
}

/// The generator source behind a launcher that waits two seconds the first time it starts: the
/// certification's first clause, which waits on that start, takes that long at least.
fn slow_to_start(scratch: &Scratch) -> std::path::PathBuf {
    let launcher = scratch.path().join("slow-to-start");
    let script = format!(
        "#!/bin/sh\nif [ ! -e '{started}' ]; then : > '{started}'; sleep 2; fi\nexec '{binary}' \"$@\"\n",
        started = scratch.path().join("started").display(),
        binary = example("serve_source").display(),
    );
    std::fs::write(&launcher, script).expect("the launcher is written");
    let mode = std::os::unix::fs::PermissionsExt::from_mode(0o755);
    std::fs::set_permissions(&launcher, mode).expect("the launcher is executable");
    launcher
}

/// A certification says each clause as it finishes, with how long one that took seconds took,
/// and a verdict, and leaves no spinner.
#[tokio::test(flavor = "multi_thread")]
async fn a_certification_at_a_terminal_says_each_clause_as_it_finishes_and_a_verdict() {
    let scratch = Scratch::new();
    let command = certifying_target(
        &scratch,
        &slow_to_start(&scratch),
        r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#,
        &["--require", "partial"],
    );
    let mut running = Screened::started(command, STANDARD);
    running
        .until("the spinner names the clause", |screen| {
            screen.contents().contains("Certifying the source: P-")
        })
        .await;
    running
        .until("a clause is shown", |screen| {
            screen.contents().contains("✓ P-")
        })
        .await;
    let finished = running.finished().await;
    let text = finished.screen().contents();
    assert_eq!(finished.status.code(), Some(0), "{text}");
    assert!(
        text.contains("✓ Passed  source") || text.contains("■ Incomplete  source"),
        "a verdict, naming its role: {text}"
    );
    assert!(text.contains("io.rapidbyte.generator"), "{text}");
    assert!(
        !text.contains("Certifying the source"),
        "the spinner is cleared: {text}"
    );
    // The first clause waited two seconds on the connector's first start, and a time shows from
    // a second on: a second's margin, whatever the machine's speed or the ticks' timing.
    let first = text
        .lines()
        .find(|line| line.trim_start().starts_with(['✓', '✗', '–']))
        .expect("a clause's line");
    let timed = regex::Regex::new(r"\s\d+s$").expect("a pattern that parses");
    assert!(timed.is_match(first.trim_end()), "{first:?} in {text}");
}

/// A role the connector does not serve says nothing as it is found, as its report may never print.
#[tokio::test(flavor = "multi_thread")]
async fn a_role_the_connector_does_not_serve_says_nothing_at_a_terminal() {
    let scratch = Scratch::new();
    let file = scratch.document(
        "config.json",
        r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#,
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_rdlt"));
    command
        .arg("certify")
        .arg(example("serve_source"))
        .args(["--trust-binaries", "--require", "partial", "--config-file"])
        .arg(file)
        .args(["--env", "LLVM_PROFILE_FILE"])
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .env_remove("CLICOLOR_FORCE")
        .env_remove("NO_COLOR")
        .envs(UTF8)
        .process_group(0)
        .kill_on_drop(true);
    let finished = Screened::started(command, STANDARD).finished().await;
    let text = finished.screen().contents();
    assert_eq!(finished.status.code(), Some(0), "{text}");
    assert!(text.contains("✓ P-"), "the role it serves: {text}");
    assert!(
        !text.contains("D-") && !text.contains("destination"),
        "the role it does not: {text}"
    );
}

/// Where nothing animates a certification's report still says each clause and its verdict,
/// whole at its role's end, and JSON at a terminal is all that is printed.
#[tokio::test(flavor = "multi_thread")]
async fn a_certification_at_a_terminal_is_whole_when_quiet_and_only_json_when_asked() {
    let config = r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#;
    let scratch = Scratch::new();
    let quiet = certifying_on_a_terminal(
        &scratch,
        config,
        &["--require", "partial", "--quiet"],
        STANDARD,
    )
    .finished()
    .await;
    assert_eq!(quiet.status.code(), Some(0));
    assert_not_animated(&quiet);
    let text = quiet.screen().contents();
    assert!(text.contains("✓ P-"), "{text}");
    assert!(
        text.contains("✓ Passed  source") || text.contains("■ Incomplete  source"),
        "{text}"
    );
    let scratch = Scratch::new();
    let json = certifying_on_a_terminal(
        &scratch,
        config,
        &["--require", "partial", "--output", "json"],
        STANDARD,
    )
    .finished()
    .await;
    assert_eq!(json.status.code(), Some(0));
    let written = String::from_utf8_lossy(&json.written);
    assert!(written.contains("\"verdict\""), "{written}");
    let text = json.screen().contents();
    assert!(
        !text.contains("✓ P-"),
        "clause lines beside the JSON: {text}"
    );
}

/// A certification stopped from outside has already said what it found.
#[tokio::test(flavor = "multi_thread")]
async fn a_certification_stopped_from_outside_has_already_said_what_it_found() {
    let scratch = Scratch::new();
    // Rows enough that the kill clause, last, is still ahead when the signal comes.
    let mut running = certifying_on_a_terminal(
        &scratch,
        r#"{"seed": 11, "streams": [{"name": "events", "rows": 40000, "partitions": 1, "batch_rows": 50}]}"#,
        &[],
        STANDARD,
    );
    running
        .until("a clause is shown", |screen| {
            screen.contents().contains("✓ P-")
        })
        .await;
    signal::kill(pid(&running.child), Signal::SIGINT).expect("signalled");
    let finished = running.finished().await;
    assert_eq!(finished.status.code(), Some(130));
    let text = finished.screen().contents();
    assert!(text.contains("✓ P-"), "what it found stayed: {text}");
    assert!(text.contains('✗'), "the interruption is said: {text}");
    assert!(
        !text.contains("Certifying the source"),
        "the spinner is cleared: {text}"
    );
}

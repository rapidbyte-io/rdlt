//! `rdlt run`: a pipeline run with its progress shown, its events and report written where asked,
//! and the signals that stop it.

use std::io::IsTerminal as _;
use std::path::Path;
use std::time::Duration;

use rdlt::{Event, EventKind, Events, Interrupts, Lagged, StopMode};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::Exit;
use crate::args::HostArgs;
use crate::output::{self, FINISHING, Failure, Mode};

/// Lines that may wait for standard error to take them; further ones are dropped, as a run is
/// never held up by what it says.
const WAITING: usize = 32;

/// Where a run's events go.
enum Sink {
    Stdout,
    File(tokio::fs::File),
}

impl Sink {
    /// Writes one JSON line; a closed pipe stops the writing and leaves the run to finish.
    async fn line(&mut self, value: &serde_json::Value) -> Result<(), Failure> {
        let text = format!("{value}\n");
        match self {
            Self::Stdout => output::stdout(&text).await,
            Self::File(file) => {
                let written = async {
                    file.write_all(text.as_bytes()).await?;
                    file.flush().await
                };
                written
                    .await
                    .map_err(|error| Failure::io("writing the events", &error))
            }
        }
    }
}

/// The file at `path`, made if it is not there and left as it is until the run starts.
async fn opened(path: &Path, what: &str) -> Result<tokio::fs::File, Failure> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create(true).truncate(false);
    options.open(path).await.map_err(|error| {
        Failure::io(
            &format!("creating the {what} at {}", path.display()),
            &error,
        )
    })
}

/// What a run's output is written to, opened before anything is placed, so a wrong path costs
/// no load.
struct Written {
    sink: Option<Sink>,
    report: Option<tokio::fs::File>,
}

impl Written {
    async fn opened(events: Option<&Path>, report: Option<&Path>) -> Result<Self, Failure> {
        let sink = match events {
            Some(path) if path.as_os_str() == "-" => Some(Sink::Stdout),
            Some(path) => Some(Sink::File(opened(path, "events").await?)),
            None => None,
        };
        let report = match report {
            Some(path) => Some(opened(path, "report").await?),
            None => None,
        };
        Ok(Self { sink, report })
    }

    /// Empties each file written, now the pipeline is built: a build that fails loses no file a
    /// run wrote before it.
    async fn emptied(&mut self) -> Result<(), Failure> {
        for (file, what) in [
            (self.report.as_mut(), "report"),
            (
                match &mut self.sink {
                    Some(Sink::File(file)) => Some(file),
                    _ => None,
                },
                "events",
            ),
        ] {
            let Some(file) = file else { continue };
            let emptying = async {
                // A pipe or a terminal has nothing to empty, and refuses being told to.
                if file.metadata().await?.is_file() {
                    file.set_len(0).await?;
                }
                Ok::<(), std::io::Error>(())
            };
            emptying
                .await
                .map_err(|error| Failure::io(&format!("emptying the {what}"), &error))?;
        }
        Ok(())
    }
}

/// Runs the pipeline `document` describes, to its end or to the signals that stop it.
pub(crate) async fn run(
    document: &Path,
    events: Option<&Path>,
    report: Option<&Path>,
    host: &HostArgs,
    mode: Mode,
    interrupts: &mut Interrupts,
) -> Result<Exit, Failure> {
    let placing = async {
        let builder = crate::host::document(document).await?;
        let host = crate::host::host(host).await?;
        let written = Written::opened(events, report).await?;
        let pipeline = builder.build(&host).await?;
        Ok::<_, Failure>((pipeline, written))
    };
    let (pipeline, mut written) = tokio::select! {
        biased;
        // A signal before the run starts ends the verb; what was spawned is stopped.
        status = interrupts.heard() => return Err(Failure::signalled(status)),
        built = placing => built?,
    };
    written.emptied().await?;
    let to_stdout = matches!(written.sink, Some(Sink::Stdout));
    let to_file = matches!(written.sink, Some(Sink::File(_)));
    let terminal = std::io::stderr().is_terminal();
    let mut run = pipeline.start();
    let (lines, waiting) = mpsc::channel(WAITING);
    let writer = written
        .sink
        .map(|sink| tokio::spawn(writing(run.events(), sink)));
    let teller = tokio::spawn(telling(run.events(), terminal, waiting));
    let (outcome, first) = until_ended(&mut run, interrupts, &lines, terminal).await;
    if terminal {
        lines.try_send("\n".to_owned()).ok();
    }
    drop(lines);
    let done = tokio::time::Instant::now() + FINISHING;
    let unwritten = match writer {
        Some(writer) => match told(writer, done).await {
            Some(failure) => failure,
            None if to_file => {
                let late = std::io::Error::new(std::io::ErrorKind::TimedOut, "not done in time");
                Some(Failure::io("writing the events", &late))
            }
            // A pipe whose reader stopped reading loses only what its reader left.
            None => None,
        },
        None => None,
    };
    told(teller, done).await;
    let unwritten = reported(&outcome, written.report, unwritten, to_stdout, mode).await;
    Ok(match (first, &outcome.error, unwritten) {
        (Some(signal), _, _) => Exit::Signalled(signal),
        (None, Some(error), _) => Exit::of(error.kind()),
        (None, None, Some(failure)) => failure.exit,
        (None, None, None) => Exit::Success,
    })
}

/// The run's outcome, and the status of the first signal that came while it ran: the first stops
/// it after its next commit, a second at once; every note is told to `lines` and never waited
/// for, so nothing holds up the signals.
async fn until_ended(
    run: &mut rdlt::Run,
    interrupts: &mut Interrupts,
    lines: &mpsc::Sender<String>,
    terminal: bool,
) -> (rdlt::Outcome, Option<i32>) {
    let control = run.control();
    let mut showing = tokio::time::interval(Duration::from_secs(1));
    let mut first = None;
    loop {
        tokio::select! {
            biased;
            // A signal goes first, so one that comes with the run's end is still heard.
            heard = interrupts.heard() => {
                if first.is_none() {
                    first = Some(heard);
                    control.stop(StopMode::AfterCommit);
                    let note = "stopping after the next commit; interrupt again to stop now";
                    lines.try_send(whole_line(terminal, note)).ok();
                } else {
                    control.stop(StopMode::Now);
                }
            }
            outcome = &mut *run => return (outcome, first),
            _ = showing.tick(), if terminal => {
                lines.try_send(status_line(&run.status())).ok();
            }
        }
    }
}

/// Writes `outcome`'s report to `report` and prints it, each within [`FINISHING`], and answers
/// the first failure to write, `unwritten` among them.
async fn reported(
    outcome: &rdlt::Outcome,
    report: Option<tokio::fs::File>,
    mut unwritten: Option<Failure>,
    to_stdout: bool,
    mode: Mode,
) -> Option<Failure> {
    let report_json = serde_json::to_value(&outcome.report).expect("a report serializes");
    if let Some(mut file) = report {
        let written = written_within(&mut file, &format!("{report_json}\n")).await;
        unwritten = unwritten.or(written.err());
    }
    let error = outcome.error.as_ref().map(rdlt::Error::report);
    // One JSON value on standard output, holding a failure of the writing where the run has none.
    let failed = match (&error, &unwritten) {
        (Some(report), _) => serde_json::to_value(report).expect("a report serializes"),
        (None, Some(failure)) => output::failure_value(failure.exit, &failure.message),
        (None, None) => serde_json::Value::Null,
    };
    let ended = serde_json::json!({ "report": report_json, "error": failed });
    let shown = match (to_stdout, mode) {
        (true, _) => format!("{}\n", serde_json::json!({ "outcome": ended })),
        (false, Mode::Json) => format!("{ended}\n"),
        (false, Mode::Plain) => {
            let report = &outcome.report;
            format!(
                "{:?}: committed {} rows in {} commits\n",
                report.status, report.rows, report.commits
            )
        }
    };
    let printed = tokio::time::timeout(FINISHING, output::stdout(&shown)).await;
    unwritten = unwritten.or(printed.unwrap_or(Ok(())).err());
    if let (Some(error), Mode::Plain) = (&outcome.error, mode) {
        let text = output::with_causes(&error.to_string(), &error.report());
        say(&format!("rdlt: {}\n", output::shown(text))).await;
    }
    if let Some(failure) = &unwritten {
        say(&format!("rdlt: {}\n", output::shown(&failure.message))).await;
    }
    unwritten
}

/// Writes `text` to standard error within [`FINISHING`].
async fn say(text: &str) {
    tokio::time::timeout(FINISHING, output::stderr(text))
        .await
        .ok();
}

/// Writes the run's events to `sink`, on a task of its own, its subscriber's place in the run's
/// ring its own: a reader that stops reading holds up only this task, and misses events told as
/// [`Lagged`]; a sink that fails ends the writing, not the run.
async fn writing(mut events: Events, mut sink: Sink) -> Option<Failure> {
    while let Some(item) = events.next().await {
        if let Err(failure) = sink.line(&json_of(&item)).await {
            return Some(failure);
        }
    }
    None
}

/// Writes the `waiting` lines to standard error, and off a terminal a line for each event, on a
/// task of its own, so standard error stopping holds up nothing else.
async fn telling(events: Events, terminal: bool, mut waiting: mpsc::Receiver<String>) {
    let mut events = Some(events);
    let mut saying = true;
    while saying || events.is_some() {
        tokio::select! {
            biased;
            // A note goes first, ahead of however many events are waiting.
            line = waiting.recv(), if saying => match line {
                Some(line) => output::stderr(&line).await,
                None => saying = false,
            },
            item = next_of(&mut events), if events.is_some() => match item {
                Some(item) if !terminal => {
                    output::stderr(&format!("{}\n", output::shown(line(&item)))).await;
                }
                Some(_) => {}
                None => events = None,
            },
        }
    }
}

/// The next of `events`, which is there.
async fn next_of(events: &mut Option<Events>) -> Option<Result<Event, Lagged>> {
    match events {
        Some(events) => events.next().await,
        None => None,
    }
}

/// What `task` answered, once it ended or `done` passes: a reader that stopped reading is left
/// behind, and the task abandoned.
async fn told<T>(mut task: JoinHandle<T>, done: tokio::time::Instant) -> Option<T> {
    if let Ok(joined) = tokio::time::timeout_at(done, &mut task).await {
        return joined.ok();
    }
    task.abort();
    None
}

/// Writes `text` to `file` within `FINISHING`.
async fn written_within(file: &mut tokio::fs::File, text: &str) -> Result<(), Failure> {
    let writing = async {
        file.write_all(text.as_bytes()).await?;
        file.flush().await
    };
    let late = || std::io::Error::new(std::io::ErrorKind::TimedOut, "not written within its bound");
    let written = tokio::time::timeout(FINISHING, writing)
        .await
        .unwrap_or_else(|_| Err(late()));
    written.map_err(|error| Failure::io("writing the report", &error))
}

/// `item` as one line of JSON.
fn json_of(item: &Result<Event, Lagged>) -> serde_json::Value {
    match item {
        Ok(event) => serde_json::to_value(event).expect("an event serializes"),
        Err(lagged) => serde_json::json!({ "lagged": lagged }),
    }
}

/// One line a person reads for `item`.
fn line(item: &Result<Event, Lagged>) -> String {
    let event = match item {
        Ok(event) => event,
        Err(lagged) => return format!("missed {} events", lagged.dropped),
    };
    let what = match &event.kind {
        EventKind::AttemptStarted => "started".to_owned(),
        EventKind::Committed { rows, took, .. } => format!("committed {rows} rows in {took:?}"),
        EventKind::Credited { rows, .. } => {
            format!("found {rows} rows of an earlier attempt committed")
        }
        EventKind::SchemaChanged { table, version, .. } => {
            format!("table {table} changed to schema version {version}")
        }
        EventKind::RetentionReset { stream, partition } => {
            format!("stream {stream} partition {partition} read again from its earliest")
        }
        EventKind::Forgotten { stream, .. } => {
            format!("stream {stream} forgot partitions no plan names")
        }
        EventKind::Retrying {
            error, retry_in, ..
        } => format!("retrying in {retry_in:?}: {}", error.message),
        EventKind::RunEnded { status } => format!("ended {status:?}"),
    };
    format!("attempt {}: {what}", event.attempt)
}

/// `text`, shown, as a whole line of its own, past what the terminal's status line shows.
fn whole_line(terminal: bool, text: &str) -> String {
    let prefix = if terminal { CLEAR } else { "" };
    format!("{prefix}{}\n", output::shown(text))
}

/// The escape that returns to the start of a terminal's line and clears it.
const CLEAR: &str = "\r\x1b[2K";

/// The run's progress, redrawn on one terminal line.
fn status_line(status: &rdlt::Status) -> String {
    let text = format!(
        "{:?}: {} partitions reading, {} rows committed in {} commits",
        status.phase, status.partitions.reading, status.report.rows, status.report.commits
    );
    format!("{CLEAR}{}", output::shown(text))
}

#![forbid(unsafe_code)]
//! A bulk load watched as it runs: its events as they happen, its report and metrics once it has
//! ended, and a stop after its next commit, asked at Ctrl-C or once it has committed a given
//! number of times.

use std::process::ExitCode;

use metrics_exporter_prometheus::PrometheusBuilder;
use rdlt::{
    Commit, Connector, Event, EventKind, Events, Host, Interrupts, Pipeline, ReadMode, RunControl,
    Side, StopMode, Stream, json,
};
use rdlt_connector_reference::{LogSource, SqliteDestination};

/// Says what `event` tells, as the run does it.
fn tell(event: &Event) {
    match &event.kind {
        EventKind::AttemptStarted => println!("attempt {} started", event.attempt),
        EventKind::SchemaChanged { table, version, .. } => {
            println!("table {table} at schema version {version}");
        }
        EventKind::Committed {
            commit, rows, took, ..
        } => println!("commit {commit}: {rows} rows in {} ms", took.as_millis()),
        EventKind::Retrying {
            error, retry_in, ..
        } => println!("retrying in {} ms: {}", retry_in.as_millis(), error.message),
        EventKind::RunEnded { status } => println!("run ended: {status:?}"),
        other => println!("{other:?}"),
    }
}

/// The pipeline: four seeded offset logs loaded into the SQLite database at `database`, read on
/// from where the last run stopped, with a commit due after each fixed count of rows.
async fn pipeline(database: &str) -> Result<Pipeline, rdlt::Error> {
    // Compiled in and trusted: both connectors run in this process.
    let host = Host::new()
        .source::<LogSource>()
        .destination::<SqliteDestination>();
    let source = Side::new(Connector::new("io.rapidbyte.log")).config(json!({
        "seed": 7,
        "streams": [{
            "name": "events", "partitions": 4, "messages": 250_000, "bounded": true,
            "batch_rows": 1000
        }]
    }));
    let destination =
        Side::new(Connector::new("io.rapidbyte.sqlite")).config(json!({ "path": database }));
    Pipeline::builder("observing", source, destination)
        .stream(Stream::new("events").read(ReadMode::Incremental))
        .commit(Commit::new().rows(50_000))
        .build(&host)
        .await
}

/// The stops the program has asked of its run: the status of the first signal heard, and whether
/// a stop after the next commit was asked, by a signal or by the count.
#[derive(Debug, Default)]
struct Stops {
    heard: Option<i32>,
    asked: bool,
}

impl Stops {
    /// The stop a signal answering `status` asks for: after the next commit at the first signal,
    /// though the count asked for that stop already, and at once at a later one.
    fn signalled(&mut self, status: i32) -> StopMode {
        if self.heard.is_some() {
            return StopMode::Now;
        }
        self.heard = Some(status);
        self.asked = true;
        StopMode::AfterCommit
    }

    /// Whether the `committed`th commit asks for the stop after the next commit: where it is the
    /// commit `stop_after` names, and no stop was asked before it.
    fn counted(&mut self, committed: u64, stop_after: Option<u64>) -> bool {
        let due = Some(committed) == stop_after && !self.asked;
        self.asked |= due;
        due
    }
}

/// Tells each of a run's `events` as it comes, until the run has ended, and asks the run's
/// `control` to stop: at a signal `interrupts` hears, such as Ctrl-C, or once `stop_after` commits
/// have landed.
///
/// Answers the exit status of the first signal heard, where one was.
async fn watch(
    mut events: Events,
    control: RunControl,
    mut interrupts: Interrupts,
    stop_after: Option<u64>,
) -> Option<i32> {
    let (mut committed, mut stops) = (0, Stops::default());
    loop {
        tokio::select! {
            biased;
            // A signal goes first, so one that comes with the run's last event is still heard.
            status = interrupts.heard() => {
                let mode = stops.signalled(status);
                if mode == StopMode::AfterCommit {
                    eprintln!("stopping after the next commit; interrupt again to stop now");
                }
                control.stop(mode);
            }
            told = events.next() => match told {
                Some(Ok(event)) => {
                    tell(&event);
                    if let EventKind::Committed { .. } = event.kind {
                        committed += 1;
                        if stops.counted(committed, stop_after) {
                            println!("stopping after the next commit");
                            control.stop(StopMode::AfterCommit);
                        }
                    }
                }
                Some(Err(lagged)) => println!("missed {} events", lagged.dropped),
                None => return stops.heard,
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let database = args.next().unwrap_or_else(|| "observing.db".to_owned());
    // Given a count, the program asks for the stop itself once that many commits have landed.
    let stop_after: Option<u64> = args.next().map(|count| count.parse()).transpose()?;
    // The run's metrics go to whichever recorder is installed; Prometheus's renders them as a
    // scrape reads them.
    let metrics = PrometheusBuilder::new().install_recorder()?;
    // Heard from here on, so that Ctrl-C asks the run to stop rather than ending the process.
    let interrupts = Interrupts::listen()?;
    let run = pipeline(&database).await?.start();
    let watching = watch(run.events(), run.control(), interrupts, stop_after);
    let (outcome, heard) = tokio::join!(run, watching);
    let report = &outcome.report;
    println!(
        "{:?}: committed {} rows in {} commits",
        report.status, report.rows, report.commits
    );
    // The run's counters, by name, as a scrape reads them.
    let rendered = metrics.render();
    let mut counters: Vec<&str> = rendered
        .lines()
        .filter(|line| line.starts_with("rdlt_") && line.contains("_total"))
        .collect();
    counters.sort_unstable();
    for counter in counters {
        println!("{counter}");
    }
    // A signal's status comes first, as the command line's does: 130 for Ctrl-C, however the
    // run ended; then the run's error, if it failed.
    match (heard, outcome.error) {
        (Some(status), _) => Ok(ExitCode::from(u8::try_from(status)?)),
        (None, Some(error)) => Err(error.into()),
        (None, None) => Ok(ExitCode::SUCCESS),
    }
}

#[cfg(test)]
mod tests;

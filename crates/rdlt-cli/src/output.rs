//! What the command line prints: machine output on standard output, human output on standard
//! error, connector text shown as the host shows it, and a closed pipe ending quietly.

use std::io::{IsTerminal as _, Write as _};
use std::time::Duration;

use tokio::io::AsyncWriteExt as _;
use tracing_subscriber::fmt::writer::BoxMakeWriter;

use crate::Exit;
use crate::args::Output;
use crate::ui::{self, Std, Tone, Ui};

/// Bytes: the most of one line of human output.
const LINE_BYTES: usize = 4_096;

/// How long the last things a verb says, and a run's report, may take to reach where they go: a
/// reader that stopped reading does not hold the exit past it.
pub(crate) const FINISHING: Duration = Duration::from_secs(5);

/// The variables that name a collector to export to.
pub(crate) const COLLECTOR: [&str; 3] = [
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
];

/// How results print.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Plain,
    Json,
}

impl Mode {
    /// `output` resolved: `auto` is plain on a terminal and JSON elsewhere.
    pub(crate) fn of(output: Output) -> Self {
        match output {
            Output::Plain => Self::Plain,
            Output::Json => Self::Json,
            Output::Auto => {
                if std::io::stdout().is_terminal() {
                    Self::Plain
                } else {
                    Self::Json
                }
            }
        }
    }
}

/// Why a verb ended without doing what it was asked.
#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) exit: Exit,
    pub(crate) message: String,
    pub(crate) report: Option<Box<rdlt::ErrorReport>>,
    /// Whether what could not be read is not there.
    pub(crate) file_not_found: bool,
}

impl Failure {
    pub(crate) fn io(context: &str, error: &std::io::Error) -> Self {
        Self {
            exit: Exit::Io,
            message: format!("{context}: {error}"),
            report: None,
            file_not_found: false,
        }
    }

    /// The end of a verb a signal stopped, `status` being the exit status of a process so ended.
    pub(crate) fn signalled(status: i32) -> Self {
        Self {
            exit: Exit::Signalled(status),
            message: "interrupted by a signal".to_owned(),
            report: None,
            file_not_found: false,
        }
    }

    pub(crate) fn usage(message: impl Into<String>) -> Self {
        Self {
            exit: Exit::Usage,
            message: message.into(),
            report: None,
            file_not_found: false,
        }
    }
}

impl From<rdlt::Error> for Failure {
    fn from(error: rdlt::Error) -> Self {
        Self::of(&error)
    }
}

impl Failure {
    /// What `error`, which also ended a run, says of itself and how it exits.
    pub(crate) fn of(error: &rdlt::Error) -> Self {
        // A file the facade could not read is `Config` to it, and I/O to whoever runs this.
        let unread = [
            rdlt::codes::DOCUMENT_UNREADABLE,
            rdlt::codes::TLS_UNREADABLE,
        ];
        let exit = if error.code().is_some_and(|code| unread.contains(&code)) {
            Exit::Io
        } else {
            Exit::of(error.kind())
        };
        let report = Box::new(error.report());
        let file_not_found = std::error::Error::source(error)
            .and_then(|cause| cause.downcast_ref::<std::io::Error>())
            .is_some_and(|cause| cause.kind() == std::io::ErrorKind::NotFound);
        Self {
            exit,
            message: with_causes(&error.to_string(), &report),
            report: Some(report),
            file_not_found,
        }
    }
}

/// `message`, with what `report` gives as its causes after it, each once.
pub(crate) fn with_causes(message: &str, report: &rdlt::ErrorReport) -> String {
    let mut text = message.to_owned();
    for cause in &report.causes {
        if !text.contains(cause) {
            text.push_str(": ");
            text.push_str(cause);
        }
    }
    text
}

/// `text` as a terminal may show it: every control sequence escaped, bounded.
pub(crate) fn shown(text: impl std::fmt::Display) -> String {
    rdlt_connector::text::shown(text, LINE_BYTES)
}

/// Writes `text` to standard output, whoever holds the reading end; a closed pipe ends the
/// writing quietly.
pub(crate) async fn stdout(text: &str) -> Result<(), Failure> {
    let mut out = tokio::io::stdout();
    let written = async {
        out.write_all(text.as_bytes()).await?;
        out.flush().await
    };
    match written.await {
        Err(error) if error.kind() != std::io::ErrorKind::BrokenPipe => {
            Err(Failure::io("writing to standard output", &error))
        }
        _ => Ok(()),
    }
}

/// Writes a line for a person to standard error, shown and within [`written_within`]'s bound,
/// whether or not the command line was told to be quiet; a failure to write is no failure of
/// the verb.
pub(crate) fn note(text: &str) {
    written_within(Std::Err, format!("{}\n", shown(text))).ok();
}

/// Writes `text` to `to` on a thread of its own, waiting within [`FINISHING`], and answers what
/// the write answered: a terminal that stopped reading holds up only that thread, which the
/// process's end takes, and is no failure.
pub(crate) fn written_within(to: Std, text: String) -> std::io::Result<()> {
    let (answer, answered) = std::sync::mpsc::channel();
    let writing = move || {
        let wrote = match to {
            Std::Out => {
                let mut stdout = std::io::stdout().lock();
                stdout
                    .write_all(text.as_bytes())
                    .and_then(|()| stdout.flush())
            }
            Std::Err => std::io::stderr().lock().write_all(text.as_bytes()),
        };
        answer.send(wrote).ok();
    };
    let thread = std::thread::Builder::new().name("rdlt-writer".to_owned());
    if thread.spawn(writing).is_err() {
        return Ok(());
    }
    answered.recv_timeout(FINISHING).unwrap_or(Ok(()))
}

/// Writes `text` to standard error, whoever holds the reading end; a failure to write is no
/// failure of the verb.
pub(crate) async fn stderr(text: &str) {
    let mut err = tokio::io::stderr();
    let written = async {
        err.write_all(text.as_bytes()).await?;
        err.flush().await
    };
    written.await.ok();
}

/// Prints a verb's result, with what was animated cleared first: `rich` where standard output is
/// a terminal and `mode` is plain, `plain`'s lines each shown and in `tone` elsewhere plain, or
/// `json`.
pub(crate) async fn result(
    ui: &Ui,
    mode: Mode,
    tone: Option<Tone>,
    plain: impl FnOnce() -> Vec<String>,
    rich: impl FnOnce() -> String,
    json: &serde_json::Value,
) -> Result<(), Failure> {
    ui.cleared().await;
    let text = match mode {
        Mode::Json => format!("{json}\n"),
        Mode::Plain if ui.rich_results() => rich(),
        Mode::Plain => {
            let mut lines = String::new();
            for line in plain() {
                lines.push_str(&ui::plain_line(ui, tone, &line));
                lines.push('\n');
            }
            lines
        }
    };
    stdout(&text).await
}

/// Prints why a verb failed, on standard error as text, in its full layout where standard error
/// is a terminal, or on standard output as JSON, within [`FINISHING`].
pub(crate) async fn failed(ui: &Ui, mode: Mode, failure: &Failure) {
    let printing = async {
        match rendered(ui, mode, failure) {
            Rendered::Output(text) => {
                stdout(&text).await.ok();
            }
            Rendered::Error(text) => stderr(&text).await,
        }
    };
    tokio::time::timeout(FINISHING, printing).await.ok();
}

/// As [`failed`], with no runtime to print through, as the start of the process has none.
pub(crate) fn failed_at_start(mode: Mode, failure: &Failure) {
    match rendered_off_a_terminal(mode, failure, None) {
        Rendered::Output(text) => write!(std::io::stdout().lock(), "{text}").ok(),
        Rendered::Error(text) => write!(std::io::stderr().lock(), "{text}").ok(),
    };
}

/// A failure of the command line's own, which is no error of a pipeline's, as JSON says one.
pub(crate) fn failure_value(exit: Exit, message: &str) -> serde_json::Value {
    let kind = match exit {
        Exit::Usage => "usage",
        Exit::Signalled(_) => "signal",
        _ => "io",
    };
    serde_json::json!({ "kind": kind, "message": shown(message) })
}

/// The line standard output holds for such a failure, under `--output json`.
pub(crate) fn failure_json(exit: Exit, message: &str) -> String {
    format!(
        "{}\n",
        serde_json::json!({ "error": failure_value(exit, message) })
    )
}

/// `failure` as `mode` prints it, and on which stream.
fn rendered(ui: &Ui, mode: Mode, failure: &Failure) -> Rendered {
    if mode == Mode::Plain && ui.rich_failures() {
        return Rendered::Error(ui::failure(failure, ui));
    }
    rendered_off_a_terminal(mode, failure, Some(ui))
}

/// `failure` as `mode` prints it without its full layout, its `rdlt:` in red where `ui` colours
/// standard error.
fn rendered_off_a_terminal(mode: Mode, failure: &Failure, ui: Option<&Ui>) -> Rendered {
    match (mode, &failure.report) {
        (Mode::Json, Some(report)) => {
            Rendered::Output(format!("{}\n", serde_json::json!({ "error": report })))
        }
        (Mode::Json, None) => Rendered::Output(failure_json(failure.exit, &failure.message)),
        (Mode::Plain, _) => Rendered::Error(failure_line(&failure.message, ui)),
    }
}

/// `message` as the single line said off a terminal.
pub(crate) fn failure_line(message: &str, ui: Option<&Ui>) -> String {
    let lead = ui.map_or_else(
        || "rdlt:".to_owned(),
        |ui| ui.looks().err().paint(Tone::Bad, "rdlt:"),
    );
    format!("{lead} {}\n", shown(message))
}

/// A failure, printed: JSON on standard output, or text on standard error.
enum Rendered {
    Output(String),
    Error(String),
}

/// Logs to standard error at `level`, or as `RUST_LOG` says; and to a collector too where the
/// `OTEL_*` variables name one, which the returned export flushes when it is dropped.
pub(crate) fn logging(level: &str, ui: &Ui) -> Option<rdlt::telemetry::Telemetry> {
    if COLLECTOR
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        match rdlt::telemetry::otlp(level) {
            Ok(telemetry) => return Some(telemetry),
            Err(error) if !ui.quiet() => note(&format!("rdlt: {error}")),
            Err(_) => {}
        }
    }
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    let writer = match ui.log_lines() {
        Some(lines) => BoxMakeWriter::new(lines),
        None => BoxMakeWriter::new(std::io::stderr),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(ui.looks().err_coloured())
        .try_init()
        .ok();
    None
}

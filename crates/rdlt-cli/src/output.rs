//! What the command line prints: machine output on standard output, human output on standard
//! error, connector text shown as the host shows it, and a closed pipe ending quietly.

use std::io::{IsTerminal as _, Write as _};
use std::time::Duration;

use tokio::io::AsyncWriteExt as _;

use crate::Exit;
use crate::args::Output;

/// Bytes: the most of one line of human output.
const LINE_BYTES: usize = 4_096;

/// How long the last things a verb says, and a run's report, may take to reach where they go: a
/// reader that stopped reading does not hold the exit past it.
pub(crate) const FINISHING: Duration = Duration::from_secs(5);

/// The variables that name a collector to export to.
const COLLECTOR: [&str; 3] = [
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
}

impl Failure {
    pub(crate) fn io(context: &str, error: &std::io::Error) -> Self {
        Self {
            exit: Exit::Io,
            message: format!("{context}: {error}"),
            report: None,
        }
    }

    /// The end of a verb a signal stopped, `status` being the exit status of a process so ended.
    pub(crate) fn signalled(status: i32) -> Self {
        Self {
            exit: Exit::Signalled(status),
            message: "interrupted by a signal".to_owned(),
            report: None,
        }
    }

    pub(crate) fn usage(message: impl Into<String>) -> Self {
        Self {
            exit: Exit::Usage,
            message: message.into(),
            report: None,
        }
    }
}

impl From<rdlt::Error> for Failure {
    fn from(error: rdlt::Error) -> Self {
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
        Self {
            exit,
            message: with_causes(&error.to_string(), &report),
            report: Some(report),
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

/// Writes a line for a person to standard error, shown; a failure to write is no failure of the
/// verb.
pub(crate) fn note(text: &str) {
    writeln!(std::io::stderr().lock(), "{}", shown(text)).ok();
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

/// Prints a verb's result: `plain`'s lines, each shown, or `json`, as `mode` says.
pub(crate) async fn result(
    mode: Mode,
    plain: impl FnOnce() -> Vec<String>,
    json: &serde_json::Value,
) -> Result<(), Failure> {
    let text = match mode {
        Mode::Plain => {
            let mut lines = String::new();
            for line in plain() {
                lines.push_str(&shown(line));
                lines.push('\n');
            }
            lines
        }
        Mode::Json => format!("{json}\n"),
    };
    stdout(&text).await
}

/// Prints why a verb failed, on standard error as text or on standard output as JSON, within
/// [`FINISHING`].
pub(crate) async fn failed(mode: Mode, failure: &Failure) {
    let printing = async {
        match rendered(mode, failure) {
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
    match rendered(mode, failure) {
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
fn rendered(mode: Mode, failure: &Failure) -> Rendered {
    match (mode, &failure.report) {
        (Mode::Json, Some(report)) => {
            Rendered::Output(format!("{}\n", serde_json::json!({ "error": report })))
        }
        (Mode::Json, None) => Rendered::Output(failure_json(failure.exit, &failure.message)),
        (Mode::Plain, _) => Rendered::Error(format!("rdlt: {}\n", shown(&failure.message))),
    }
}

/// A failure, printed: JSON on standard output, or text on standard error.
enum Rendered {
    Output(String),
    Error(String),
}

/// Logs to standard error at `level`, or as `RUST_LOG` says; and to a collector too where the
/// `OTEL_*` variables name one, which the returned export flushes when it is dropped.
pub(crate) fn logging(level: &str) -> Option<rdlt::telemetry::Telemetry> {
    if COLLECTOR
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        match rdlt::telemetry::otlp(level) {
            Ok(telemetry) => return Some(telemetry),
            Err(error) => note(&format!("rdlt: {error}")),
        }
    }
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .try_init()
        .ok();
    None
}

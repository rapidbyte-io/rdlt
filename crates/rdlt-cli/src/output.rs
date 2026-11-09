//! What the command line prints: machine output on standard output, human output on standard
//! error, connector text shown as the host shows it, and a closed pipe ending quietly.

use std::io::{IsTerminal as _, Write as _};

use tokio::io::AsyncWriteExt as _;

use crate::Exit;
use crate::args::Output;

/// Bytes: the most of one line of human output.
const LINE_BYTES: usize = 4_096;

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
        Self {
            exit,
            message: error.to_string(),
            report: Some(Box::new(error.report())),
        }
    }
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

/// Prints why a verb failed: on standard error as text, or on standard output as JSON.
pub(crate) fn failed(mode: Mode, failure: &Failure) {
    let mut stdout = std::io::stdout().lock();
    match (mode, &failure.report) {
        (Mode::Json, Some(report)) => {
            writeln!(stdout, "{}", serde_json::json!({ "error": report })).ok();
        }
        (Mode::Json, None) => {
            let message = shown(&failure.message);
            writeln!(
                stdout,
                "{}",
                serde_json::json!({ "error": { "message": message } })
            )
            .ok();
        }
        (Mode::Plain, _) => note(&format!("rdlt: {}", failure.message)),
    }
}

/// Logs to standard error at `level`, or as `RUST_LOG` says; and to a collector too where the
/// `OTEL_*` variables name one, which the returned export flushes when it is dropped.
pub(crate) fn logging(level: &str) -> Option<rdlt::telemetry::Telemetry> {
    if COLLECTOR
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        match rdlt::telemetry::otlp() {
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

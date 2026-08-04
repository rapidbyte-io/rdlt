//! The `run` subcommand: parse the document, surface the CDC
//! composition advisories, build through the shared model, drive the
//! event feed, emit the report. The library does all the work — this
//! file is plumbing and rendering.

use std::path::PathBuf;

use rdlt::pipeline_spec::{self, Spec};

use crate::args::Verbosity;
use crate::ui::RendererKind;
use crate::{CliError, cdc, ui};

pub(crate) async fn run(
    spec_path: PathBuf,
    report_path: Option<PathBuf>,
    events_path: Option<PathBuf>,
    verbosity: Verbosity,
    renderer: RendererKind,
) -> Result<(), CliError> {
    // `--events -` claims stdout, which is the report's channel: the
    // two machine outputs must never interleave, so the report has to
    // be redirected first.
    let events_to_stdout = events_path.as_deref() == Some(std::path::Path::new("-"));
    if events_to_stdout && report_path.is_none() {
        return Err(CliError::Usage(
            "`--events -` writes NDJSON to stdout, where the report would go —              add `--report <path>` to give the report its own destination"
                .into(),
        ));
    }

    let (spec, pipeline_name) = load_spec(&spec_path)?;
    let pipeline = pipeline_spec::build_pipeline(&spec)?;
    let events_sink = match events_path {
        None => None,
        Some(_) if events_to_stdout => Some(EventSink::Stdout),
        Some(path) => Some(EventSink::File(std::io::BufWriter::new(
            std::fs::File::create(&path)
                .map_err(|e| CliError::Io(format!("creating {}: {e}", path.display())))?,
        ))),
    };
    drive(
        pipeline,
        pipeline_name,
        report_path,
        events_sink,
        verbosity,
        renderer,
    )
    .await
}

/// Parse the document and surface the CDC composition advisories —
/// shared by `run` and `validate`, so the two can never disagree
/// about what a valid document is.
fn load_spec(spec_path: &std::path::Path) -> Result<(Spec, String), CliError> {
    let raw = std::fs::read_to_string(spec_path)
        .map_err(|e| CliError::Io(format!("reading {}: {e}", spec_path.display())))?;
    let spec: Spec =
        serde_yaml::from_str(&raw).map_err(|e| CliError::Usage(format!("parsing spec: {e}")))?;

    // The exactly-once CDC composition advisories need the resolved postgres
    // SOURCE config; `pg_source_config` is `None` for other source kinds.
    if let Some(config) = spec.pg_source_config() {
        let config = config?;
        for warning in cdc::cdc_composition_warnings(&spec, &config) {
            eprintln!("warning: {warning}");
        }
    }
    let name = spec.pipeline.clone();
    Ok((spec, name))
}

/// `rdlt validate` — the same gates a run passes on its way to the
/// first byte, and nothing after them.
pub(crate) async fn validate(spec_path: PathBuf, verbosity: Verbosity) -> Result<(), CliError> {
    let (spec, pipeline_name) = load_spec(&spec_path)?;
    let _pipeline = pipeline_spec::build_pipeline(&spec)?;
    if verbosity != Verbosity::Quiet {
        eprintln!("ok: pipeline {pipeline_name} is valid");
    }
    Ok(())
}

/// Where `--events` NDJSON goes.
enum EventSink {
    Stdout,
    File(std::io::BufWriter<std::fs::File>),
}

impl EventSink {
    fn write(&mut self, event: &rdlt::prelude::PipelineEvent) {
        use std::io::Write as _;
        // Advisory, like the feed itself: a sink failure warns once at
        // flush, never fails the run.
        if let Ok(line) = serde_json::to_string(event) {
            match self {
                // Not `println!`: a closed consumer must degrade, not
                // panic the feed task.
                EventSink::Stdout => {
                    let mut stdout = std::io::stdout().lock();
                    let _ = writeln!(stdout, "{line}");
                }
                EventSink::File(w) => {
                    let _ = writeln!(w, "{line}");
                }
            }
        }
    }

    fn finish(self) {
        use std::io::Write as _;
        if let EventSink::File(mut w) = self
            && let Err(e) = w.flush()
        {
            eprintln!("warning: flushing --events file: {e}");
        }
    }
}

/// Event feed + run + report emission (shared tail after the pipeline
/// is built). Contract, pinned by tests/cli_contract.rs: human text on
/// stderr, the report JSON on stdout (or `--report`), and a failed
/// renderer never fails the run.
async fn drive(
    pipeline: rdlt::Pipeline,
    pipeline_name: String,
    report_path: Option<PathBuf>,
    mut events_sink: Option<EventSink>,
    verbosity: Verbosity,
    renderer: RendererKind,
) -> Result<(), CliError> {
    let mut events = pipeline.events();
    let feed = tokio::spawn(async move {
        match renderer {
            RendererKind::Quiet => {
                while let Some(event) = events.recv().await {
                    if let Some(sink) = &mut events_sink {
                        sink.write(&event);
                    }
                }
            }
            RendererKind::Plain => {
                while let Some(event) = events.recv().await {
                    if let Some(sink) = &mut events_sink {
                        sink.write(&event);
                    }
                    if let Some(line) = ui::plain::line(&event, verbosity) {
                        eprintln!("{line}");
                    }
                }
            }
            RendererKind::Pretty => {
                let mut display = ui::pretty::Pretty::new(&pipeline_name);
                let mut tick = tokio::time::interval(ui::pretty::Pretty::redraw_every());
                loop {
                    tokio::select! {
                        event = events.recv() => match event {
                            Some(event) => {
                                if let Some(sink) = &mut events_sink {
                                    sink.write(&event);
                                }
                                display.apply(&event);
                            }
                            None => break,
                        },
                        _ = tick.tick() => display.redraw(),
                    }
                }
                // The live rows come down; the summary that follows
                // carries the durable numbers.
                display.clear();
            }
        }
        if let Some(sink) = events_sink {
            sink.finish();
        }
    });

    let report = pipeline.run().await?;
    // A failed renderer is reported but never fails the run: by this point the
    // load has succeeded and the report is in hand, so exiting non-zero would
    // misreport the outcome.
    if let Err(e) = feed.await {
        eprintln!("warning: event feed stopped: {e}");
    }
    // The FINAL numbers, from the exactly-once report — never the live
    // fold. Quiet suppresses it; both other renderers end with it.
    // Best-effort BY HAND: `eprint!` panics on a closed stderr, which
    // would turn a finished run into exit 101 — the human channel
    // failing must never misreport the outcome.
    if renderer != RendererKind::Quiet {
        use std::io::Write as _;
        let _ = std::io::stderr().write_all(ui::summary::render(&report).as_bytes());
    }

    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| CliError::Usage(format!("encoding report: {e}")))?;
    match report_path {
        Some(path) => std::fs::write(&path, json)
            .map_err(|e| CliError::Io(format!("writing {}: {e}", path.display())))?,
        // Not `println!`: a closed stdout (the consumer exited) would
        // PANIC and exit 101, outside the documented code set. A
        // failed report write is an IO failure like any other — 74.
        None => {
            use std::io::Write as _;
            let mut stdout = std::io::stdout().lock();
            stdout
                .write_all(json.as_bytes())
                .and_then(|()| stdout.write_all(b"\n"))
                .map_err(|e| CliError::Io(format!("writing report to stdout: {e}")))?;
        }
    }
    Ok(())
}

//! The `run` subcommand: build the pipeline from the document, drive
//! the event feed, emit the report. The library does all the work —
//! this file is plumbing and rendering.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use rdlt::pipeline::Pipeline;

use crate::args::{Output, Verbosity};
use crate::render::mode::Mode;
use crate::{events, exit, render};

pub(crate) async fn run(
    spec: PathBuf,
    report: Option<PathBuf>,
    events: Option<PathBuf>,
    stdout_is_tty: bool,
    verbosity: Verbosity,
    mode: Mode,
    output: Output,
) -> Result<(), exit::Error> {
    let target = events::Target::resolve(events, report.is_some())?;
    let (pipeline, name) = build(&spec).await?;
    // NDJSON streaming to a terminal stdout shares the screen with the
    // live display; the display yields.
    let mode = match target {
        Some(events::Target::Stdout) if stdout_is_tty => mode.sharing_terminal(),
        _ => mode,
    };
    let sink = target.map(events::Target::open).transpose()?;
    let feed = feed(&pipeline, name, mode, sink, verbosity);
    let run_result = pipeline.run().await;
    // The feed is awaited BEFORE the outcome, on both paths: under
    // Pretty the display owns stderr until it clears, so anything
    // written earlier races its redraws and can be overdrawn or
    // scrolled away. A renderer failure is reported but never changes
    // the exit code — the run's outcome is already decided by here.
    if let Err(e) = feed.await {
        render::stderr::line(&format!("warning: event feed stopped: {e}"));
    }
    let report_run = run_result?;
    // The FINAL numbers, from the exactly-once report — never the live
    // fold. Best-effort BY HAND: `eprint!` panics on a closed stderr,
    // which would turn a finished run into exit 101.
    if mode != Mode::Quiet {
        render::stderr::frame("", &render::summary::render(&report_run));
    }

    let json = serde_json::to_string_pretty(&report_run)
        .map_err(|e| exit::Error::Usage(format!("encoding report: {e}")))?;
    match report {
        // The report carries source-defined cursors: born owner-only,
        // and never written through a link planted at the name.
        Some(path) => rdlt_core::fs::replace_private(&path)
            .and_then(|mut file| file.write_all(json.as_bytes()))
            .map_err(|e| exit::Error::Io(format!("writing {}: {e}", path.display())))?,
        // An INTERACTIVE stdout gets the summary above and a hint, not
        // the JSON dump that would bury it — unless `--output json`
        // asks for machine output regardless. Redirected stdout keeps
        // the byte-identical contract every script depends on.
        None if stdout_is_tty && output != Output::Json => {
            if mode != Mode::Quiet {
                render::stderr::line("  (full report: redirect stdout, or pass --report <path>)");
            }
        }
        // Not `println!`: a closed stdout (the consumer exited) would
        // PANIC and exit 101, outside the documented code set. A
        // failed report write is an IO failure like any other — 74.
        None => {
            let mut stdout = std::io::stdout().lock();
            stdout
                .write_all(json.as_bytes())
                .and_then(|()| stdout.write_all(b"\n"))
                .map_err(|e| exit::Error::Io(format!("writing report to stdout: {e}")))?;
        }
    }
    Ok(())
}

/// Build the document through the facade's ONE chain —
/// [`Pipeline::from_file_with_name`] — so the CLI and any embedder can
/// never disagree about what a valid document is; what lives here is
/// only the exit-code mapping. A filesystem refusal (`Error::Io`) is
/// exit 74, a config refusal of the config class renders its message
/// bare (`error: <the provider's or connector's own wording>`) as exit
/// 2, the same line the parse refusals print; every other engine error
/// keeps its own rendering and code.
pub(crate) async fn build(spec_path: &Path) -> Result<(Pipeline, String), exit::Error> {
    Pipeline::from_file_with_name(spec_path)
        .await
        .map_err(|e| match e {
            rdlt::error::Error::Io { message } => exit::Error::Io(message),
            rdlt::error::Error::Config { message } => exit::Error::Usage(message),
            other => exit::Error::Run(other),
        })
}

/// Spawn the task that drains the event feed into the chosen renderer
/// and the `--events` sink; it ends when the pipeline closes the
/// channel, clearing the live display first so the summary or the
/// error is the next thing on stderr.
fn feed(
    pipeline: &Pipeline,
    pipeline_name: String,
    mode: Mode,
    mut sink: Option<events::Sink>,
    verbosity: Verbosity,
) -> tokio::task::JoinHandle<()> {
    let mut events = pipeline.events();
    tokio::spawn(async move {
        match mode {
            Mode::Quiet => {
                while let Some(event) = events.recv().await {
                    if let Some(sink) = &mut sink {
                        sink.write(&event);
                    }
                }
            }
            Mode::Plain => {
                while let Some(event) = events.recv().await {
                    if let Some(sink) = &mut sink {
                        sink.write(&event);
                    }
                    if let Some(line) = render::plain::line(&event, verbosity) {
                        render::stderr::line(&line);
                    }
                }
            }
            Mode::Pretty => {
                let mut display = render::pretty::Pretty::new(&pipeline_name);
                // `interval_at`, not `interval`: a plain interval fires
                // its first tick immediately, which would reveal the
                // display before the run's rows exist.
                let mut tick = tokio::time::interval_at(
                    tokio::time::Instant::now() + render::pretty::REDRAW_EVERY,
                    render::pretty::REDRAW_EVERY,
                );
                loop {
                    tokio::select! {
                        event = events.recv() => match event {
                            Some(event) => {
                                if let Some(sink) = &mut sink {
                                    sink.write(&event);
                                }
                                display.apply(&event);
                            }
                            None => break,
                        },
                        // The first tick also reveals the display: the
                        // initial rows exist by then, so it appears at
                        // full height instead of growing on screen.
                        _ = tick.tick() => {
                            display.reveal();
                            display.redraw();
                        }
                    }
                }
                display.clear();
            }
        }
        if let Some(sink) = sink {
            sink.finish();
        }
    })
}

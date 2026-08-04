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
    verbosity: Verbosity,
    renderer: RendererKind,
) -> Result<(), CliError> {
    let raw = std::fs::read_to_string(&spec_path)
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

    let pipeline_name = spec.pipeline.clone();
    let pipeline = pipeline_spec::build_pipeline(&spec)?;
    drive(pipeline, pipeline_name, report_path, verbosity, renderer).await
}

/// Event feed + run + report emission (shared tail after the pipeline
/// is built). Contract, pinned by tests/cli_contract.rs: human text on
/// stderr, the report JSON on stdout (or `--report`), and a failed
/// renderer never fails the run.
async fn drive(
    pipeline: rdlt::Pipeline,
    pipeline_name: String,
    report_path: Option<PathBuf>,
    verbosity: Verbosity,
    renderer: RendererKind,
) -> Result<(), CliError> {
    let mut events = pipeline.events();
    let feed = tokio::spawn(async move {
        match renderer {
            RendererKind::Quiet => while events.recv().await.is_some() {},
            RendererKind::Plain => {
                while let Some(event) = events.recv().await {
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
                            Some(event) => display.apply(&event),
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
    if renderer != RendererKind::Quiet {
        eprint!("{}", ui::summary::render(&report));
    }

    let json = serde_json::to_string_pretty(&report)
        .map_err(|e| CliError::Usage(format!("encoding report: {e}")))?;
    match report_path {
        Some(path) => std::fs::write(&path, json)
            .map_err(|e| CliError::Io(format!("writing {}: {e}", path.display())))?,
        None => println!("{json}"),
    }
    Ok(())
}

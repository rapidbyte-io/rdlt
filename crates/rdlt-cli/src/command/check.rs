//! `rdlt check` — connectivity, discovery and plan checks without a
//! load session. The build gates run for real first (for a
//! `connector:` requirement that is the REAL spawn and handshake — the
//! connector's own config validation runs and refuses here, typed,
//! exit 2), then the library's check: both connectors' reachability
//! probes, stream discovery, and the run's plan validation. Nothing is
//! created or written anywhere — no workdir, no WAL, no session; the
//! checked pipeline is discarded, which kills the spawned processes
//! with it.

use std::path::PathBuf;

use crate::args::Verbosity;
use crate::command::run;
use crate::{exit, render};

pub(crate) async fn check(spec: PathBuf, verbosity: Verbosity) -> Result<(), exit::Error> {
    let (pipeline, name) = run::build(&spec).await?;
    let summary = pipeline.check().await?;
    if verbosity != Verbosity::Quiet {
        // The pipeline name is document text — render it through the
        // identifier escape like every other name seat.
        render::stderr::line(&format!(
            "ok: pipeline {} — connectors reachable, {} streams discovered, plan valid",
            render::stderr::sanitize_identifier(&name),
            summary.streams
        ));
    }
    Ok(())
}

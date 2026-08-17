//! `rdlt validate` — the same gates a run passes on its way to the
//! first byte, and nothing after them. For a `connector:` requirement
//! those gates are the REAL spawn and handshake — the connector's own
//! config validation runs and refuses here, typed, exit 2 — and the
//! built pipeline is then discarded, which kills the spawned processes
//! with it.

use std::path::PathBuf;

use crate::args::Verbosity;
use crate::command::run;
use crate::{exit, render};

pub(crate) async fn validate(spec: PathBuf, verbosity: Verbosity) -> Result<(), exit::Error> {
    let (_pipeline, name) = run::build(&spec).await?;
    if verbosity != Verbosity::Quiet {
        // The pipeline name is document text — render it through the
        // identifier escape like every other name seat.
        render::stderr::line(&format!(
            "ok: pipeline {} is valid",
            render::stderr::sanitize_identifier(&name)
        ));
    }
    Ok(())
}

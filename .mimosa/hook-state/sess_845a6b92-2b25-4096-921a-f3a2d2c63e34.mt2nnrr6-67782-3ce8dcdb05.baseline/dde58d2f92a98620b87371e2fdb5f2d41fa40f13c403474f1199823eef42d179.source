//! Checking a pipeline without running it: connectivity, discovery and
//! plan validation — [`Summary`] is what a clean check reports.
//!
//! The choreography is the front of a run and nothing after it: the
//! source's probe, the destination's probe, stream discovery under the
//! stream cap, then the same plan validation a run performs. The ENGINE
//! creates nothing — no workdir lock, no WAL, no load session, no reads;
//! what a connector's own probe or construction does is that
//! connector's behavior (a destination that materializes its target at
//! construction does so here exactly as a run would).

use std::sync::Arc;

use rdlt_connector::destination::Destination;
use rdlt_connector::source::Source;
use rdlt_core::error::Error;
use rdlt_core::id::StreamName;

use crate::classify::{classify_dest_error, classify_source_error};
use crate::config::Config;
use crate::run::validate;

/// What a clean check found. At least the discovered stream count;
/// `#[non_exhaustive]` so a later finding is not a breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Summary {
    /// How many streams discovery declared (already under the
    /// per-source stream cap — an over-cap discovery refuses instead).
    pub streams: usize,
}

/// The check choreography behind [`crate::engine::Engine::check`].
/// Probe failures classify exactly as a run's would: a source refusal
/// surfaces as [`Error::Source`] (under the pseudo-stream `<check>`,
/// the `<discovery>` convention), a destination refusal as
/// [`Error::Destination`], a plan problem as [`Error::Config`].
pub(crate) async fn check(
    config: &Config,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
) -> Result<Summary, Error> {
    source
        .check()
        .await
        .map_err(|e| classify_source_error(StreamName::new("<check>"), &e))?;
    destination
        .check()
        .await
        .map_err(|e| classify_dest_error(&e))?;
    let capabilities = destination.capabilities();
    let streams = validate::discover_and_validate(
        config,
        source.as_ref(),
        capabilities,
        destination.as_ref(),
    )
    .await?;
    Ok(Summary {
        streams: streams.len(),
    })
}

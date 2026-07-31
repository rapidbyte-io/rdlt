//! Change data capture via logical replication — UNDER CONSTRUCTION.
//!
//! The configuration vocabulary parses (the `cdc:` block), but reads
//! dispatching here fail typed until the CDC runtime lands. This placeholder
//! keeps the crate honest while it is built bottom-up: nothing pretends to
//! capture changes.

use rdlt_connector::SourceError;

use crate::source::errors::{self, Phase};

/// Run-scoped CDC state placeholder — the typestate runtime replaces this.
#[derive(Debug)]
pub(crate) struct Runtime;

impl Runtime {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn not_yet_implemented(&self, stream: &str) -> SourceError {
        errors::fatal(
            Phase::Slot,
            Some(stream),
            "CDC is not yet implemented in this crate generation",
        )
    }
}

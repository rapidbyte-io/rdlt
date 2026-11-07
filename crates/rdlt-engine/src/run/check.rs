//! Checking a plan against its connectors, as a run's start checks it, opening no session.

use std::sync::Arc;

use rdlt_connector::{Catalog, Destination, Source};

use super::Engine;
use crate::attempt::streams::checked;
use crate::attempt::within_calls;
use crate::error::{Error, Side};
use crate::naming::Naming;
use crate::plan::PipelinePlan;

impl Engine {
    /// Checks `plan` against `source` and `destination` before anything is read, and answers the
    /// catalog once every check passes.
    ///
    /// Opens no session, so no run of the pipeline is fenced. The checks, in order: the calls
    /// each connector carries at once, each connector's own `check`, the destination's identifier
    /// rules, the source's catalog, and each stream against the catalog and the destination's
    /// capabilities, its transforms and its stage included, as a run's start checks it.
    ///
    /// A run's start does not call the connectors' own checks; they are this check's alone. A
    /// connector that panics fails the check as an internal error.
    ///
    /// # Errors
    ///
    /// The first check that fails, in the order above; a `Config` error for a plan the connectors
    /// cannot run.
    pub async fn check(
        &self,
        plan: &PipelinePlan,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> Result<Catalog, Error> {
        let checking = |source, destination| checking(plan, source, destination);
        self.guarded("check", source, destination, checking).await
    }
}

/// Checks as [`Engine::check`] says, a connector's panic unwinding out of it.
async fn checking(
    plan: &PipelinePlan,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
) -> Result<Catalog, Error> {
    within_calls(source.as_ref(), destination.as_ref())?;
    source
        .check()
        .await
        .map_err(|error| Error::connector(Side::Source, "checking the source", error))?;
    destination
        .check()
        .await
        .map_err(|error| Error::connector(Side::Destination, "checking the destination", error))?;
    let capabilities = destination.capabilities();
    Naming::checked(&capabilities.identifiers)?;
    let catalog = source
        .discover()
        .await
        .map_err(|error| Error::connector(Side::Source, "discovering the catalog", error))?;
    for stream in plan.streams() {
        checked(capabilities, plan, stream, &catalog)?;
    }
    Ok(catalog)
}

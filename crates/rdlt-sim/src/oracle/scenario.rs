//! How one run of a phase goes: undisturbed, crashed, stopped or raced by a second run, for each
//! pipeline sharing the destination.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectorId, Destination, DestinationConnector as _, Source, SourceConnector as _,
};
use rdlt_engine::{Engine, PipelinePlan, Report, RunHandle, RunStatus, StopMode};
use rdlt_host::{ConnectorRef, Provider as _};

use super::refusals::Failure;
use crate::canary::Canary;
use crate::changes::SimChangeSource;
use crate::destination::SimDestination;
use crate::network::Placing;
use crate::rng::SplitMix64;
use crate::source::SimSource;
use crate::world::World;

/// The longest a single run may take in virtual time before the oracle calls it hung.
pub(super) const RUN_LIMIT: Duration = Duration::from_secs(3600);

/// How one run of a phase goes.
#[derive(Clone, Copy, Debug)]
pub(super) enum Scenario {
    /// The run proceeds undisturbed.
    Plain,
    /// The run is dropped after the given time, as if its worker crashed.
    Crash(Duration),
    /// The run is asked to stop after committing, after the given time.
    Stop(Duration),
    /// The run is asked to stop at once, after the given time.
    StopNow(Duration),
    /// A second run of the same pipeline starts after the given time.
    Concurrent(Duration),
}

/// A disruption for a run, drawn from `rng`.
pub(super) fn pick(rng: &mut SplitMix64) -> Scenario {
    let after = Duration::from_millis(rng.below(2000));
    match rng.below(5) {
        0 => Scenario::Plain,
        1 => Scenario::Crash(after),
        2 => Scenario::Stop(after),
        3 => Scenario::StopNow(after),
        _ => Scenario::Concurrent(after / 2),
    }
}

/// Starts a run of `plan` against the world registered as `world`, its connectors placed by
/// `placing` on a simulated network where there is one, and in this process otherwise.
pub(super) async fn start(
    engine: &Engine,
    plan: &PipelinePlan,
    world: &str,
    placing: Option<&Placing>,
) -> RunHandle {
    if let Some(placing) = placing {
        let config = Canary::of(world).config();
        // Where faults are on, a connector is now and then placed on the peer that stalls.
        let stalls = || World::named(world).is_some_and(|world| world.stalls());
        let source = placing.source(&config, stalls()).await;
        let destination = placing.destination(&config, stalls()).await;
        return engine.run(plan.clone(), source, destination);
    }
    let (source, destination) = in_process(world).await;
    engine.run(plan.clone(), source, destination)
}

/// The source and destination of the world registered as `world`, placed in this process through
/// the host's registry, as an embedder places connectors it trusts, their configurations'
/// secrets resolved by the world's vault.
pub(super) async fn in_process(world: &str) -> (Arc<dyn Source>, Arc<dyn Destination>) {
    let canary = Canary::of(world);
    let (registry, config) = (canary.registry(), canary.config());
    // A world serving changes has the change source serve them.
    let changes = World::named(world).is_some_and(|world| !world.changes.streams.is_empty());
    let source = if changes {
        SimChangeSource::ID
    } else {
        SimSource::ID
    };
    let reference = |id: &str| {
        ConnectorRef::new(ConnectorId::parse(id).expect("the simulated connectors' ids are valid"))
    };
    let source = registry
        .source(&reference(source), &config)
        .await
        .expect("the simulated source connects")
        .connector;
    let destination = registry
        .destination(&reference(SimDestination::ID), &config)
        .await
        .expect("the simulated destination connects")
        .connector;
    (Arc::from(source), Arc::from(destination))
}

/// How one pipeline's runs of a scenario went.
pub(super) struct Executed {
    /// Whether one of them succeeded.
    pub(super) succeeded: bool,
    /// The failures of those that failed.
    pub(super) failures: Vec<Failure>,
    /// The reports of those that ended.
    pub(super) reports: Vec<Report>,
    /// How many runs it started.
    pub(super) started: u64,
}

/// Runs `scenario` for each of `plans`, the pipelines sharing the destination, at once.
pub(super) async fn execute_all(
    engine: &Engine,
    plans: &[PipelinePlan],
    world: &str,
    placing: Option<&Placing>,
    scenario: Scenario,
) -> Vec<Executed> {
    let first = execute(engine, &plans[0], world, placing, scenario);
    let second = async {
        match plans.get(1) {
            Some(plan) => Some(execute(engine, plan, world, placing, scenario).await),
            None => None,
        }
    };
    let (first, second) = tokio::join!(first, second);
    std::iter::once(first).chain(second).collect()
}

/// Runs `scenario` for the pipeline of `plan`.
async fn execute(
    engine: &Engine,
    plan: &PipelinePlan,
    world: &str,
    placing: Option<&Placing>,
    scenario: Scenario,
) -> Executed {
    let (ended, dropped, started) = match scenario {
        Scenario::Plain => (
            vec![bounded(start(engine, plan, world, placing).await).await],
            false,
            1,
        ),
        Scenario::Crash(after) => {
            let run = bounded(start(engine, plan, world, placing).await);
            tokio::select! {
                biased;
                ended = run => (vec![ended], false, 1),
                // Dropping the run is the crash, which takes what its disk had not made durable.
                () = tokio::time::sleep(after) => {
                    if let Some(world) = World::named(world) {
                        world.crash_logs(plan.pipeline());
                    }
                    (Vec::new(), true, 1)
                }
            }
        }
        Scenario::Stop(after) | Scenario::StopNow(after) => {
            let handle = start(engine, plan, world, placing).await;
            let control = handle.control();
            let stop = async {
                tokio::time::sleep(after).await;
                control.stop(match scenario {
                    Scenario::StopNow(_) => StopMode::Now,
                    _ => StopMode::AfterCommit,
                });
            };
            let (ended, ()) = tokio::join!(bounded(handle), stop);
            (vec![ended], false, 1)
        }
        Scenario::Concurrent(delay) => {
            let first = bounded(start(engine, plan, world, placing).await);
            let second = async {
                tokio::time::sleep(delay).await;
                bounded(start(engine, plan, world, placing).await).await
            };
            let (first, second) = tokio::join!(first, second);
            (vec![first, second], false, 2)
        }
    };
    let succeeded = ended
        .iter()
        .any(|(report, _)| report.status == RunStatus::Succeeded);
    let (reports, failures): (Vec<Report>, Vec<Option<Failure>>) = ended.into_iter().unzip();
    Executed {
        succeeded: succeeded && !dropped,
        failures: failures.into_iter().flatten().collect(),
        reports,
        started,
    }
}

/// Awaits `run`, panicking if it takes longer than [`RUN_LIMIT`]; its report, and its failure.
async fn bounded(run: RunHandle) -> (Report, Option<Failure>) {
    let outcome = tokio::time::timeout(RUN_LIMIT, run)
        .await
        .expect("every run ends within the limit of virtual time");
    let failure = outcome.error.as_ref().map(Failure::of);
    (outcome.report, failure)
}

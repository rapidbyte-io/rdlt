//! How one run of a phase goes: undisturbed, crashed, stopped or raced by a second run, for each
//! pipeline sharing the destination.

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{ConnectContext, destination_factory, source_factory};
use rdlt_engine::{Engine, PipelinePlan, Report, RunHandle, RunStatus, StopMode};
use serde_json::json;

use super::refusals::Failure;
use crate::destination::SimDestination;
use crate::rng::SplitMix64;
use crate::source::SimSource;

/// The longest a single run may take in virtual time before the oracle calls it hung.
const RUN_LIMIT: Duration = Duration::from_secs(3600);

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

/// Starts a run of `plan` against the world registered as `world`.
async fn start(engine: &Engine, plan: &PipelinePlan, world: &str) -> RunHandle {
    let config = json!({ "world": world });
    let source = source_factory::<SimSource>()
        .connect(config.clone(), ConnectContext::new())
        .await
        .expect("the simulated source connects");
    let destination = destination_factory::<SimDestination>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the simulated destination connects");
    engine.run(plan.clone(), Arc::from(source), Arc::from(destination))
}

/// How one pipeline's runs of a scenario went.
pub(super) struct Executed {
    /// Whether one of them succeeded.
    pub(super) succeeded: bool,
    /// The failures of those that failed.
    pub(super) failures: Vec<Failure>,
    /// The reports of those that ended.
    pub(super) reports: Vec<Report>,
}

/// Runs `scenario` for each of `plans`, the pipelines sharing the destination, at once.
pub(super) async fn execute_all(
    engine: &Engine,
    plans: &[PipelinePlan],
    world: &str,
    scenario: Scenario,
) -> Vec<Executed> {
    let first = execute(engine, &plans[0], world, scenario);
    let second = async {
        match plans.get(1) {
            Some(plan) => Some(execute(engine, plan, world, scenario).await),
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
    scenario: Scenario,
) -> Executed {
    let (ended, dropped) = match scenario {
        Scenario::Plain => (vec![bounded(start(engine, plan, world).await).await], false),
        Scenario::Crash(after) => {
            let run = bounded(start(engine, plan, world).await);
            tokio::select! {
                biased;
                ended = run => (vec![ended], false),
                // Dropping the run is the crash.
                () = tokio::time::sleep(after) => (Vec::new(), true),
            }
        }
        Scenario::Stop(after) | Scenario::StopNow(after) => {
            let handle = start(engine, plan, world).await;
            let control = handle.control();
            let stop = async {
                tokio::time::sleep(after).await;
                control.stop(match scenario {
                    Scenario::StopNow(_) => StopMode::Now,
                    _ => StopMode::AfterCommit,
                });
            };
            let (ended, ()) = tokio::join!(bounded(handle), stop);
            (vec![ended], false)
        }
        Scenario::Concurrent(delay) => {
            let first = bounded(start(engine, plan, world).await);
            let second = async {
                tokio::time::sleep(delay).await;
                bounded(start(engine, plan, world).await).await
            };
            let (first, second) = tokio::join!(first, second);
            (vec![first, second], false)
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

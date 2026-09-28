//! The kill clauses (`K`): a connector killed at random points of a load, placed and supervised
//! as an engine's placement is, and the engine, retrying, converging exactly-once.
//!
//! A spawned connector is killed outright; one reached otherwise has its connections cut, which
//! is all a host can do to it. The clauses run only in a build with the `kill` feature, which
//! brings the engine; another build skips them.

#[cfg(feature = "kill")]
mod destination;
#[cfg(feature = "kill")]
mod killing;
#[cfg(feature = "kill")]
mod rows;
#[cfg(feature = "kill")]
mod source;
#[cfg(all(test, feature = "kill"))]
mod tests;

use rdlt_connector::Role;
#[cfg(not(feature = "kill"))]
use rdlt_connector::testing::Outcome;
use rdlt_connector::testing::{Clause, ClauseResult, Probe};

use crate::target::Target;

/// The clauses [`certify_source`](crate::certify_source) and
/// [`certify_destination`](crate::certify_destination) check last: the source's, then the
/// destination's.
pub const KILL_CLAUSES: &[Clause] = &[
    Clause {
        id: "K-SOURCE",
        statement: "a source killed at random points of a load after it commits is started \
                    again and resumes from what was committed, so the engine converges on \
                    exactly the tables a load never killed publishes",
    },
    Clause {
        id: "K-DESTINATION",
        statement: "a destination killed at random points of a load, as it writes, before a \
                    commit, or after a commit before its answer, is started again and \
                    publishes every row exactly once when the engine converges",
    },
];

/// The kill clauses of `role`.
pub(crate) fn clauses(role: Role) -> &'static [Clause] {
    match role {
        Role::Source => &KILL_CLAUSES[..1],
        Role::Destination => &KILL_CLAUSES[1..],
    }
}

/// `K-SOURCE`'s result for the source `target` reaches, which answers to `id`, with `config`.
#[cfg_attr(
    not(feature = "kill"),
    expect(
        clippy::unused_async,
        reason = "it awaits the clause in a build with it"
    )
)]
pub(crate) async fn source(
    target: &Target,
    id: &rdlt_connector::ConnectorId,
    config: &serde_json::Value,
) -> ClauseResult {
    #[cfg(feature = "kill")]
    let outcome = within(source::resumed(target, id, config)).await;
    #[cfg(not(feature = "kill"))]
    let outcome = {
        let _ = (target, id, config);
        Outcome::Skipped(UNBUILT.to_owned())
    };
    ClauseResult {
        clause: KILL_CLAUSES[0],
        outcome,
    }
}

/// `K-DESTINATION`'s result for the destination `target` reaches, which answers to `id`, with
/// `config`, reading what it published through `probe`.
#[cfg_attr(
    not(feature = "kill"),
    expect(
        clippy::unused_async,
        reason = "it awaits the clause in a build with it"
    )
)]
pub(crate) async fn destination(
    target: &Target,
    id: &rdlt_connector::ConnectorId,
    config: &serde_json::Value,
    probe: &dyn Probe,
) -> ClauseResult {
    #[cfg(feature = "kill")]
    let outcome = within(destination::exactly_once(target, id, config, probe)).await;
    #[cfg(not(feature = "kill"))]
    let outcome = {
        let _ = (target, id, config, probe);
        Outcome::Skipped(UNBUILT.to_owned())
    };
    ClauseResult {
        clause: KILL_CLAUSES[1],
        outcome,
    }
}

/// Why a build without the `kill` feature skips the kill clauses.
#[cfg(not(feature = "kill"))]
const UNBUILT: &str = "rdlt-certify was built without its `kill` feature";

#[cfg(feature = "kill")]
pub(crate) use running::{Loaded, converged, run, unproven, within};

#[cfg(feature = "kill")]
mod running {
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use rdlt_connector::testing::Outcome;
    use rdlt_connector::{Destination, Source};

    use crate::protocol::Violation;
    use rdlt_engine::{
        BatchPolicy, CommitPolicy, Engine, EngineConfig, PipelinePlan, RayonPool, RetryPolicy,
        RunStatus, SystemEnv,
    };
    use rdlt_host::Kills;

    /// The longest a kill clause takes, all its loads together.
    const KILL_TIME: Duration = Duration::from_secs(300);

    /// The most loads a clause runs until one succeeds: a kill that lands as a load opens, or as
    /// it ends, can fail a load its retries do not save.
    const LOADS: usize = 3;

    /// The rows a load commits at a time: few, so a load commits often and a kill finds work in
    /// flight.
    const COMMIT_ROWS: u64 = 16;

    /// The bytes a load holds in flight, which bound what a source sends ahead of commits.
    const MEMORY: u64 = 1 << 20;

    /// The rows a batch the engine writes holds, at most.
    const BATCH_ROWS: u64 = 8;

    /// The attempts a load makes: each kill costs one.
    const ATTEMPTS: u32 = 20;

    /// What a clause found.
    pub(crate) enum Loaded {
        /// The connector kept the clause.
        Kept,
        /// It broke the clause, for the stated reason.
        Broken(String),
        /// The clause proves nothing of the connector, for the stated reason: no kill interrupted
        /// the load, or nothing reads back what it published.
        Inapplicable(String),
    }

    /// `clause`'s outcome, or a failure once it takes longer than [`KILL_TIME`].
    pub(crate) async fn within(clause: impl Future<Output = Loaded>) -> Outcome {
        match tokio::time::timeout(KILL_TIME, clause).await {
            Ok(Loaded::Kept) => Outcome::Passed,
            Ok(Loaded::Broken(reason)) => Outcome::Failed(reason),
            Ok(Loaded::Inapplicable(reason)) => Outcome::Skipped(reason),
            Err(_) => Outcome::Failed(format!("the clause took longer than {KILL_TIME:?}")),
        }
    }

    /// This run of a clause: the time, which names its pipelines, tables and stores apart from
    /// every other run's, and, unless one is chosen, seeds the points it kills at.
    pub(crate) fn run() -> u64 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        u64::try_from(nanos).unwrap_or(u64::MAX)
    }

    /// Why a load proves nothing of the connector, drawn from `seed`, when `kills` killed nothing
    /// or no kill interrupted it; `None` when one did.
    pub(crate) fn unproven(kills: &Kills, interrupted: bool, seed: u64) -> Option<Loaded> {
        (kills.count() == 0 || !interrupted).then(|| {
            let reason = format!("no kill interrupted the load (kill seed {seed}): it ended first");
            Loaded::Inapplicable(reason)
        })
    }

    /// An engine that commits every [`COMMIT_ROWS`] rows and retries quickly.
    fn engine() -> Result<Engine, Violation> {
        let commit = CommitPolicy::new(None, Some(COMMIT_ROWS), None).map_err(Violation::of)?;
        let retry = RetryPolicy::default()
            .max_attempts(ATTEMPTS)
            .initial(Duration::from_millis(10))
            .max_delay(Duration::from_millis(200))
            .reset_after_progress(true);
        // Small batches and little room ahead of the destination, so commits come often and a
        // source is still reading when the kills land.
        let batch = BatchPolicy::new(1 << 20, BATCH_ROWS, Duration::from_millis(10), 1 << 20)
            .map_err(Violation::of)?;
        let config = EngineConfig::builder()
            .memory(MEMORY)
            .partition_buffer(1)
            .lane_window(1)
            .batch(batch)
            .commit(commit)
            .retry(retry)
            .build()
            .map_err(Violation::of)?;
        let threads = NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN);
        let pool = RayonPool::new(threads).map_err(Violation::of)?;
        Ok(Engine::new(config, Arc::new(SystemEnv::new(pool))))
    }

    /// Loads `plan` from `source` into `destination` until a load succeeds, at most [`LOADS`]
    /// loads; whether any attempt of them failed, as a kill fails one.
    pub(crate) async fn converged(
        plan: &PipelinePlan,
        source: &Arc<dyn Source>,
        destination: &Arc<dyn Destination>,
    ) -> Result<bool, Violation> {
        let engine = engine()?;
        let mut interrupted = false;
        let mut failed = String::new();
        for _ in 0..LOADS {
            let outcome = engine
                .run(plan.clone(), Arc::clone(source), Arc::clone(destination))
                .await;
            interrupted |= outcome
                .report
                .attempts
                .iter()
                .any(|attempt| attempt.error.is_some());
            if outcome.report.status == RunStatus::Succeeded {
                return Ok(interrupted);
            }
            interrupted = true;
            failed = outcome.error.map_or_else(
                || format!("{:?}", outcome.report.status),
                |error| error.to_string(),
            );
        }
        Err(Violation(format!(
            "the load did not succeed in {LOADS} loads: {failed}"
        )))
    }
}

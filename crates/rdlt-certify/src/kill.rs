//! The kill clauses (`K`): a connector killed at random points of a load, placed and supervised
//! as an engine's placement is, and the engine, retrying, converging exactly-once.
//!
//! A spawned connector is killed outright; one reached otherwise has its connections cut, which
//! is all a host can do to it. The clauses run only in a build with the `kill` feature, which
//! brings the engine; in another build they are not observed.

#[cfg(feature = "kill")]
mod bounded;
#[cfg(feature = "kill")]
mod destination;
#[cfg(feature = "kill")]
mod killing;
#[cfg(feature = "kill")]
mod rows;
#[cfg(feature = "kill")]
mod source;
#[cfg(feature = "kill")]
#[cfg(test)]
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
        unless: "the source has no stream that is read in any mode",
    },
    Clause {
        id: "K-DESTINATION",
        statement: "a destination killed at random points of a load, as it writes, before a \
                    commit, or after a commit before its answer, is started again and \
                    publishes every row exactly once when the engine converges",
        unless: "the destination declares no write mode",
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
    let (outcome, note) =
        within(source::resumed(target, id, config), target.chosen_timeout()).await;
    #[cfg(not(feature = "kill"))]
    let (outcome, note) = {
        let _ = (target, id, config);
        (Outcome::Unobserved(UNBUILT.into()), None)
    };
    ClauseResult {
        clause: KILL_CLAUSES[0],
        outcome,
        note,
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
    let (outcome, note) = {
        let checking = destination::exactly_once(target, id, config, probe);
        within(checking, target.chosen_timeout()).await
    };
    #[cfg(not(feature = "kill"))]
    let (outcome, note) = {
        let _ = (target, id, config, probe);
        (Outcome::Unobserved(UNBUILT.into()), None)
    };
    ClauseResult {
        clause: KILL_CLAUSES[1],
        outcome,
        note,
    }
}

/// Why a build without the `kill` feature leaves the kill clauses unobserved.
#[cfg(not(feature = "kill"))]
const UNBUILT: &str = "rdlt-certify was built without its `kill` feature";

#[cfg(all(feature = "kill", test))]
use running::{DRAWS, drawn};
#[cfg(feature = "kill")]
pub(crate) use running::{Loaded, Proof, converged, proven, run, unproven, within};

#[cfg(feature = "kill")]
mod running {
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use rdlt_connector::testing::{Outcome, Reason};
    use rdlt_connector::{Destination, Source};

    use crate::protocol::Violation;
    use rdlt_engine::{
        BatchPolicy, CommitPolicy, Engine, EngineConfig, PipelinePlan, RayonPool, RetryPolicy,
        RunStatus, SystemEnv,
    };
    use rdlt_host::Kills;

    /// The longest a kill clause takes, all its loads together, unless the target chooses.
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

    /// What a kill that landed was seen to end.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Proof {
        /// A connection to the connector ended after a kill that did not cut it: the process this
        /// host started was killed, and its end of the connection closed with it.
        Ended,
        /// Every connection a kill ended this host cut itself, which is all a kill does to a
        /// connector it did not start: the connector was not seen to stop.
        Cut,
    }

    impl Proof {
        /// What `kills` were seen to end.
        pub(crate) fn of(kills: &Kills) -> Self {
            Self::seen(kills.landed(), kills.cut())
        }

        /// What kills were seen to end, of which `landed` ended a connection and `cut` of those
        /// by cutting it.
        pub(crate) fn seen(landed: u64, cut: u64) -> Self {
            if landed > cut { Self::Ended } else { Self::Cut }
        }

        /// What a report says beside a clause passed on this.
        pub(crate) fn note(self) -> &'static str {
            match self {
                Self::Ended => "killed: its connection ended with a process a kill ended",
                Self::Cut => "cut: kills cut its connection, and no process was seen to stop",
            }
        }
    }

    /// What a clause found.
    pub(crate) enum Loaded {
        /// The connector kept the clause, on the stated proof that kills reached it.
        Kept(Proof),
        /// It broke the clause, for the stated reason.
        Broken(String),
        /// The clause does not apply to what the connector declares, for the stated reason:
        /// nothing it serves loads as the clause loads.
        Inapplicable(String),
        /// What the clause requires was not seen, for the stated reason: no kill reached the
        /// connector, or nothing reads back what it published.
        Unobserved(String),
        /// No kill interrupted the load, for the stated reason: another draw of kill points may.
        Unseen(String),
    }

    /// The loads a clause runs until a kill interrupts one: its kill points count commits, and a
    /// load on a busy machine makes fewer, larger ones, and may end first.
    pub(crate) const DRAWS: u64 = 4;

    /// The outcome of `load`, given the run naming its pipelines and stores and the seed of its
    /// kill points, drawn again with another run and seed while no kill interrupted it, at most
    /// [`DRAWS`] times; a `chosen` seed loads once, so its run can be repeated.
    pub(crate) async fn proven<F, Loading>(chosen: Option<u64>, run: u64, mut load: F) -> Loaded
    where
        F: FnMut(u64, u64) -> Loading,
        Loading: Future<Output = Loaded>,
    {
        let mut seed = drawn(chosen, run);
        let mut draw = 0;
        loop {
            let loaded = load(run.wrapping_add(draw), seed).await;
            draw += 1;
            if !matches!(loaded, Loaded::Unseen(_)) || chosen.is_some() || draw == DRAWS {
                return loaded;
            }
            seed = mixed(seed);
        }
    }

    /// `clause`'s outcome, or a failure once it takes longer than `chosen`, or [`KILL_TIME`].
    pub(crate) async fn within(
        clause: impl Future<Output = Loaded>,
        chosen: Option<Duration>,
    ) -> (Outcome, Option<Reason>) {
        let bound = chosen.unwrap_or(KILL_TIME);
        let outcome = match tokio::time::timeout(bound, clause).await {
            Ok(Loaded::Kept(proof)) => return (Outcome::Passed, Some(proof.note().into())),
            Ok(Loaded::Broken(reason)) => Outcome::Failed(reason.into()),
            Ok(Loaded::Inapplicable(reason)) => Outcome::Inapplicable(reason.into()),
            Ok(Loaded::Unobserved(reason) | Loaded::Unseen(reason)) => {
                Outcome::Unobserved(reason.into())
            }
            Err(_) => Outcome::Failed(Reason::new(format_args!(
                "the loads took longer than {bound:?}; a connector this slow needs a longer \
                 --kill-timeout"
            ))),
        };
        (outcome, None)
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

    /// The seed a clause draws its kill points from: `chosen`, or else `run` mixed, so a clock
    /// that ticks coarser than nanoseconds still draws every point.
    pub(crate) fn drawn(chosen: Option<u64>, run: u64) -> u64 {
        chosen.unwrap_or_else(|| mixed(run))
    }

    /// `value` mixed as `SplitMix64` mixes its state, so every bit of the result follows every bit
    /// of `value`.
    fn mixed(value: u64) -> u64 {
        let mut mixed = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    /// Why a load proves nothing of the connector, drawn from `seed`: `kills` killed nothing, no
    /// connection was seen to end after a kill, or no attempt failed; `None` when a kill landed
    /// and an attempt failed.
    ///
    /// An attempt that failed is no evidence by itself: a clause that loses an answer fails one
    /// whatever became of the connector.
    pub(crate) fn unproven(kills: &Kills, interrupted: bool, seed: u64) -> Option<Loaded> {
        let reason = if kills.count() == 0 {
            "no kill interrupted the load: it ended first"
        } else if kills.landed() == 0 {
            "no kill reached the connector: its connection outlived each"
        } else if !interrupted {
            "no kill interrupted the load: no attempt of it failed"
        } else {
            return None;
        };
        Some(Loaded::Unseen(format!("{reason} (kill seed {seed})")))
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

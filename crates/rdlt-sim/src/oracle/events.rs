//! The events oracle: what each run tells of itself agrees with how it ended.
//!
//! Its commits and credits count the report's rows and bytes, its end comes last and once, each
//! failed attempt the report keeps was retried or ended the run, and its status once ended
//! carries its report.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use rdlt_connector::{Destination, Source};
use rdlt_engine::{
    ComputePool, Engine, EngineConfig, Env, Event, EventKind, Phase, PipelinePlan, Report,
    RunControl, RunHandle, RunOutcome, RunStatus, Scope, Sleep, Status,
};

use super::memory::aside;
use crate::env::SimEnv;

/// What a simulation's runs share: the engine's configuration and the simulated environment.
pub(super) struct Runs {
    config: EngineConfig,
    env: Arc<SimEnv>,
}

impl Runs {
    /// Runs under `config` on `env`.
    pub(super) fn new(config: EngineConfig, env: Arc<SimEnv>) -> Self {
        Self { config, env }
    }

    /// An engine of the simulation's, for what is not a run: a reset.
    pub(super) fn engine(&self) -> Engine {
        Engine::new(self.config.clone(), Arc::clone(&self.env) as Arc<dyn Env>)
    }

    /// Starts a run of `plan` from `source` to `destination` whose events are kept for the
    /// oracle.
    pub(super) fn run(
        &self,
        plan: PipelinePlan,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> Observed {
        let told = Arc::new(Mutex::new(Told::default()));
        let env = Telling {
            env: Arc::clone(&self.env),
            told: Arc::clone(&told),
        };
        let engine = Engine::new(self.config.clone(), Arc::new(env));
        Observed {
            handle: engine.run(plan, source, destination),
            told,
        }
    }
}

/// A run whose events are kept.
pub(super) struct Observed {
    handle: RunHandle,
    told: Arc<Mutex<Told>>,
}

impl Observed {
    /// A control that stops the run.
    pub(super) fn control(&self) -> RunControl {
        self.handle.control()
    }

    /// Awaits the run for at most `limit` of virtual time: its outcome, the trace of its events,
    /// and what its events got wrong.
    ///
    /// # Panics
    ///
    /// Panics where the run takes longer than `limit`.
    pub(super) async fn ended(mut self, limit: Duration) -> (RunOutcome, u64, Vec<String>) {
        let outcome = tokio::time::timeout(limit, &mut self.handle)
            .await
            .expect("every run ends within the limit of virtual time");
        let status = self.handle.status();
        let told = self.told.lock();
        let wrong = disagreements(&told, &outcome.report, &status);
        let trace = told.trace.finish();
        drop(told);
        (outcome, trace, wrong)
    }
}

/// What `told`, the events of a run that ended with `report`, gets wrong, and where its `status`
/// once it ended does not carry the report.
fn disagreements(told: &Told, report: &Report, status: &Status) -> Vec<String> {
    let mut wrong = Vec::new();
    let counted = (u128::from(report.rows), u128::from(report.bytes));
    if (told.rows, told.bytes) != counted {
        wrong.push(format!(
            "a run's commits and credits told {:?} rows and bytes, its report {counted:?}",
            (told.rows, told.bytes)
        ));
    }
    if told.ended != Some(report.status) || told.late > 0 {
        wrong.push(format!(
            "a run that ended {:?} told it ended {:?}, and {} events after",
            report.status, told.ended, told.late
        ));
    }
    let listed = u64::try_from(report.attempts.len()).unwrap_or(u64::MAX);
    let first = report.attempted.saturating_sub(listed) + 1;
    for (number, attempt) in (first..).zip(&report.attempts) {
        let retried = told.retried.contains(&number);
        if attempt.error.is_some() && number != report.attempted && !retried {
            wrong.push(format!(
                "attempt {number} of a run failed with {:?}, told no retry, and ended no run",
                attempt.error
            ));
        }
    }
    let ended = Phase::Ended {
        status: report.status,
    };
    if status.report != *report || status.phase != ended {
        wrong.push(format!(
            "a run's status once it ended is {:?} with {:?}, not its outcome's {report:?}",
            status.phase, status.report
        ));
    }
    wrong
}

/// What a run told, folded as it is told: no event is kept.
#[derive(Default)]
struct Told {
    /// The rows and bytes its commits and credits counted.
    rows: u128,
    bytes: u128,
    /// How it told it ended, where it did.
    ended: Option<RunStatus>,
    /// Events told after its end.
    late: u64,
    /// The attempts it told it retries.
    retried: BTreeSet<u64>,
    /// Every event, hashed in order: alike on every run of a seed.
    trace: DefaultHasher,
}

impl Told {
    fn tell(&mut self, event: &Event) {
        serde_json::to_string(event)
            .expect("an event serializes")
            .hash(&mut self.trace);
        if self.ended.is_some() {
            self.late += 1;
        }
        match &event.kind {
            EventKind::Committed { rows, bytes, .. } | EventKind::Credited { rows, bytes } => {
                self.rows += u128::from(*rows);
                self.bytes += u128::from(*bytes);
            }
            EventKind::Retrying {
                scope: Scope::Attempt,
                ..
            } => {
                self.retried.insert(event.attempt);
            }
            EventKind::RunEnded { status } => self.ended = Some(*status),
            EventKind::AttemptStarted
            | EventKind::Retrying {
                scope: Scope::Commit,
                ..
            }
            | EventKind::SchemaChanged { .. }
            | EventKind::RetentionReset { .. }
            | EventKind::Forgotten { .. } => {}
        }
    }
}

/// The simulated environment, keeping what a run tells for the oracle.
struct Telling {
    env: Arc<SimEnv>,
    told: Arc<Mutex<Told>>,
}

impl Env for Telling {
    fn now(&self) -> SystemTime {
        self.env.now()
    }

    fn instant(&self) -> Instant {
        self.env.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        self.env.sleep(duration)
    }

    fn random(&self) -> u64 {
        self.env.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        self.env.compute()
    }

    fn cores(&self) -> NonZeroUsize {
        self.env.cores()
    }

    fn observe(&self, event: &Event) {
        // The simulation's own work: what it keeps is not the engine's to hold.
        aside(|| self.told.lock().tell(event));
    }

    fn observes(&self) -> bool {
        true
    }

    fn charged(&self, change: i64) {
        self.env.charged(change);
    }
}

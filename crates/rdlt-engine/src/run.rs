//! Runs: attempts, retries, stopping, and the handle an embedder awaits.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{Destination, LoadId, Source};
use tokio_util::sync::CancellationToken;

use crate::attempt::{self, RunContext};
use crate::budget::MemoryBudget;
use crate::config::{EngineConfig, RetryPolicy};
use crate::env::Env;
use crate::error::Error;
use crate::plan::PipelinePlan;
use crate::report::{AttemptEnd, AttemptLog, AttemptRecord, CommitRecord, Report, RunStatus};
use crate::scope::contained;

/// Moves data from sources to destinations, exactly once.
///
/// ```
/// use std::num::NonZeroUsize;
/// use std::sync::Arc;
///
/// use rdlt_engine::{Engine, EngineConfig, RayonPool, SystemEnv};
///
/// let env = Arc::new(SystemEnv::new(RayonPool::new(NonZeroUsize::MIN)?));
/// let engine = Engine::new(EngineConfig::builder().build()?, env);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Engine {
    config: Arc<EngineConfig>,
    env: Arc<dyn Env>,
}

impl Engine {
    /// An engine with `config`, taking time, randomness and compute from `env`.
    pub fn new(config: EngineConfig, env: Arc<dyn Env>) -> Self {
        Self {
            config: Arc::new(config),
            env,
        }
    }

    /// Starts a run of `plan` from `source` to `destination`; nothing happens until the handle is
    /// polled.
    ///
    /// The run retries failed attempts as the retry policy allows. Each attempt opens the
    /// destination, which fences every older attempt and discards their unpublished staging, and
    /// resumes from committed state.
    pub fn run(
        &self,
        plan: PipelinePlan,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> RunHandle {
        let control = RunControl {
            after_commit: CancellationToken::new(),
            now: CancellationToken::new(),
        };
        let context = RunContext {
            env: Arc::clone(&self.env),
            config: Arc::clone(&self.config),
            plan: Arc::new(plan),
            source,
            destination,
            budget: MemoryBudget::new(self.config.memory().get()),
            stop: control.after_commit.clone(),
            cycles: Mutex::new(BTreeMap::new()),
        };
        RunHandle {
            future: Box::pin(drive(context, control.clone())),
            control,
        }
    }
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// How to stop a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopMode {
    /// Stop reading, commit what is sealed, and end cleanly.
    AfterCommit,
    /// End the current attempt at once; its unpublished staging is discarded at the next open.
    Now,
}

/// Stops a run; clones stop the same run.
#[derive(Clone, Debug)]
pub struct RunControl {
    after_commit: CancellationToken,
    now: CancellationToken,
}

impl RunControl {
    /// Asks the run to stop as `mode` says.
    pub fn stop(&self, mode: StopMode) {
        match mode {
            StopMode::AfterCommit => self.after_commit.cancel(),
            StopMode::Now => self.now.cancel(),
        }
    }
}

/// How a run ended: its report, and the error that ended it, if any.
#[derive(Debug)]
pub struct RunOutcome {
    /// What the run committed, attempt by attempt.
    pub report: Report,
    /// The error that ended the run; `None` when it succeeded.
    ///
    /// A run stopped on request carries the error of the attempt that failed just before the
    /// stop, if one did.
    pub error: Option<Error>,
}

/// A run in progress; await it for the [`RunOutcome`].
///
/// Dropping the handle before it completes cancels the run: every task it started ends, and
/// unpublished staging is discarded at the next open.
#[must_use = "a run does nothing unless it is awaited"]
pub struct RunHandle {
    future: Pin<Box<dyn Future<Output = RunOutcome> + Send>>,
    control: RunControl,
}

impl RunHandle {
    /// A control that stops this run.
    pub fn control(&self) -> RunControl {
        self.control.clone()
    }
}

impl fmt::Debug for RunHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle").finish_non_exhaustive()
    }
}

impl Future for RunHandle {
    type Output = RunOutcome;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<RunOutcome> {
        self.future.as_mut().poll(cx)
    }
}

/// How long to wait before the next attempt: as long as the failure asked, or the policy's backoff
/// after `failures` consecutive failures.
fn backoff(retry: &RetryPolicy, error: &Error, failures: u32, env: &dyn Env) -> Duration {
    let failed = NonZeroU32::new(failures).unwrap_or(NonZeroU32::MIN);
    error
        .retry_after()
        .unwrap_or_else(|| retry.delay(failed, env.random()))
}

/// Credits a failed attempt's commit in flight to it once `log`'s attempt opened and found it landed,
/// and keeps `log`'s own commit in flight when its attempt `failed`.
///
/// An attempt that never opened read nothing back, so the commit stays in flight for the next one.
fn credit(
    attempts: &mut [AttemptRecord],
    unresolved: &mut Option<(usize, CommitRecord)>,
    log: &mut AttemptLog,
    failed: bool,
) {
    if let Some(opened) = log.opened
        && let Some((index, pending)) = unresolved.take()
        && opened == (pending.receipt.load_id, pending.receipt.commit_seq)
    {
        attempts[index].log.commits.push(pending);
    }
    if failed && let Some(pending) = log.pending.take() {
        *unresolved = Some((attempts.len(), pending));
    }
}

/// Runs one attempt, which `log` records, unless the run is stopped now; dropping the attempt
/// ends every task it started.
///
/// A connector that panics fails its attempt, not the caller awaiting the run.
async fn attempted(
    context: &RunContext,
    control: &RunControl,
    load_id: LoadId,
    log: Arc<Mutex<AttemptLog>>,
) -> Result<AttemptEnd, Error> {
    tokio::select! {
        biased;
        () = control.now.cancelled() => Err(Error::cancelled("the run was stopped")),
        result = contained(attempt::run(context, load_id, log)) => result.unwrap_or_else(|panic| {
            Err(Error::internal(format!("an attempt panicked: {panic}")))
        }),
    }
}

/// Runs attempts until one finishes, the retry policy gives up, or the run is stopped.
async fn drive(context: RunContext, control: RunControl) -> RunOutcome {
    let started = context.env.instant();
    let retry = *context.config.retry();
    let mut attempts: Vec<AttemptRecord> = Vec::new();
    // A failed attempt's commit in flight, credited to it once a later attempt reads it back.
    let mut unresolved: Option<(usize, CommitRecord)> = None;
    let mut failures = 0;
    let (status, error) = loop {
        let load_id = context.env.load_id();
        let log = Arc::new(Mutex::new(AttemptLog::default()));
        let started_at = context.env.now();
        let result = attempted(&context, &control, load_id, Arc::clone(&log)).await;
        let mut log = std::mem::take(&mut *log.lock());
        credit(&mut attempts, &mut unresolved, &mut log, result.is_err());
        let progressed = !log.commits.is_empty();
        attempts.push(AttemptRecord {
            load_id,
            started_at,
            ended_at: context.env.now(),
            log,
            error: result.as_ref().err().map(Error::report),
        });
        let error = match result {
            Ok(AttemptEnd::Exhausted) => break (RunStatus::Succeeded, None),
            Ok(AttemptEnd::Stopped) => break (RunStatus::Stopped, None),
            Err(error) => error,
        };
        if control.now.is_cancelled() {
            break (RunStatus::Cancelled, Some(error));
        }
        if progressed && retry.resets_after_progress() {
            failures = 0;
        }
        failures += 1;
        if !error.is_retryable() || failures >= retry.attempts().get() {
            break (RunStatus::Failed, Some(error));
        }
        let delay = backoff(&retry, &error, failures, context.env.as_ref());
        tokio::select! {
            biased;
            () = control.now.cancelled() => break (RunStatus::Cancelled, Some(error)),
            () = control.after_commit.cancelled() => break (RunStatus::Stopped, Some(error)),
            () = context.env.sleep(delay) => {}
        }
    };
    let elapsed = context.env.instant().saturating_duration_since(started);
    let report = Report::fold(
        context.plan.pipeline().clone(),
        status,
        attempts,
        elapsed,
        context.budget.peak(),
    );
    RunOutcome { report, error }
}

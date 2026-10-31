//! Runs: attempts, retries, stopping, and the handle an embedder awaits.

mod reset;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{Destination, Source};
use tokio_util::sync::CancellationToken;

use crate::attempt::{self, RunContext};
use crate::budget::MemoryBudget;
use crate::config::{EngineConfig, RetryPolicy};
use crate::deadline::Waits;
use crate::env::Env;
use crate::error::Error;
use crate::event::{Announcer, EventKind, Scope};
use crate::plan::PipelinePlan;
use crate::report::{AttemptEnd, AttemptLog, Credit, Report, RunStatus, Tally};
use crate::scope::contained;
use crate::status::{Ended, Live, Status};

pub use reset::{ResetReport, ResetScope};

/// Moves data from sources to destinations, exactly once.
///
/// ```
/// use std::num::NonZeroUsize;
/// use std::sync::Arc;
///
/// use rdlt_engine::{Cores, Engine, EngineConfig, SystemEnv};
///
/// let cores = Cores::new(NonZeroUsize::new(2).expect("2 is non-zero"));
/// let env = Arc::new(SystemEnv::try_new(cores)?);
/// let engine = Engine::new(EngineConfig::builder().build()?, env);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
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
        let budget = MemoryBudget::new(
            self.config.memory().get(),
            Arc::clone(&self.env),
            self.config.budget_wait(),
        )
        .read_by(self.config.partitions().get())
        .limited(self.config.limits());
        let waits =
            Waits::new(Arc::clone(&self.env), self.config.connector_wait()).charging(&budget);
        let tally = Arc::new(Tally::default());
        let live = Arc::new(Live::new(
            Arc::clone(&self.env),
            plan.pipeline().clone(),
            budget.clone(),
            Arc::clone(&tally),
        ));
        let context = RunContext {
            env: Arc::clone(&self.env),
            config: Arc::clone(&self.config),
            plan: Arc::new(plan),
            source: waits.source(source),
            destination: waits.destination(destination),
            budget,
            tally,
            stop: control.after_commit.clone(),
            cycles: Mutex::new(BTreeMap::new()),
            live: Arc::clone(&live),
            telling: Arc::default(),
        };
        RunHandle {
            future: Box::pin(ran(context, control.clone())),
            control,
            live,
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
    ///
    /// A commit of it that landed unheard is not in the run's report. An attempt that fails
    /// otherwise waits up to [`EngineConfig::close_wait`](crate::EngineConfig::close_wait) for its
    /// commit in flight, so a landed commit goes uncounted only under this mode, or, where no
    /// later attempt opens to find it landed, where it answered past the close wait, where its
    /// last try was answered transiently though it landed, or where its answer was lost.
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
    live: Arc<Live>,
}

impl RunHandle {
    /// A control that stops this run.
    pub fn control(&self) -> RunControl {
        self.control.clone()
    }

    /// What the run is doing now, and its report so far.
    ///
    /// It never waits, and no decision of the run reads what it returns. Awaited through a
    /// reference, the handle still answers once the run ended: the status then carries the
    /// outcome's report.
    pub fn status(&self) -> Status {
        self.live.status()
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

/// How long to wait before trying again, after an attempt or a try of a commit failed: as long as
/// the failure asked, held to the policy's delays, or the policy's backoff after `failures`
/// consecutive failures.
pub(crate) fn backoff(
    retry: &RetryPolicy,
    error: &Error,
    failures: u32,
    env: &dyn Env,
) -> Duration {
    let failed = NonZeroU32::new(failures).unwrap_or(NonZeroU32::MIN);
    error.retry_after().map_or_else(
        || retry.delay(failed, env.random()),
        |asked| retry.within(asked),
    )
}

/// Tells, through `announcer`, what an attempt that `ended` left to tell: a commit whose report it
/// ended before, and a commit of an earlier attempt it found landed; whether it progressed.
fn told(announcer: &Announcer, ended: Ended) -> bool {
    if let Some(unreported) = ended.unreported {
        announcer.announce(|| unreported.cut_short(announcer.env().instant()));
    }
    if let Some(Credit { rows, bytes }) = ended.credited {
        announcer.announce(|| EventKind::Credited { rows, bytes });
    }
    ended.progressed
}

/// Runs one attempt, which `log` records and `announcer` tells of, unless the run is stopped
/// now; dropping the attempt ends every task it started.
///
/// A connector that panics fails its attempt, not the caller awaiting the run, and so does an
/// observer that panics at an event of the attempt.
async fn attempted(
    context: &RunContext,
    control: &RunControl,
    log: Arc<Mutex<AttemptLog>>,
    announcer: &Announcer,
) -> Result<AttemptEnd, Error> {
    let attempt = async {
        announcer.announce(|| EventKind::AttemptStarted);
        attempt::run(context, log, announcer).await
    };
    tokio::select! {
        biased;
        () = control.now.cancelled() => Err(Error::cancelled("the run was stopped")),
        result = contained(attempt) => result.unwrap_or_else(|panic| {
            Err(Error::internal(format!("an attempt panicked: {panic}")))
        }),
    }
}

/// Runs the run as [`deadlined`] does, then ends it: its report, which every status from now on
/// carries, and the event that tells it ended.
async fn ran(context: RunContext, control: RunControl) -> RunOutcome {
    context.live.begin();
    let (status, error) = deadlined(&context, control).await;
    // A task the attempt's end left running may still run: from here on it changes nothing a
    // status shows and tells nothing.
    let report = context.live.end(status);
    let pipeline = context.plan.pipeline().clone();
    let telling = Arc::clone(&context.telling);
    Announcer::new(
        Arc::clone(&context.env),
        pipeline,
        report.attempted,
        telling,
    )
    .end(status);
    RunOutcome { report, error }
}

/// Runs attempts as `drive` does, and at the plan's deadline, where it has one, stops the run as
/// a stop after a commit does: how the run ended, and the error that ended it.
///
/// A run the deadline stops read for as long as it was asked, so it succeeded; where its last
/// attempt had failed and was waiting to be retried, it failed.
async fn deadlined(context: &RunContext, control: RunControl) -> (RunStatus, Option<Error>) {
    let Some(deadline) = context.plan.until().deadline() else {
        return drive(context, control).await;
    };
    let reached = AtomicBool::new(false);
    let ended = CancellationToken::new();
    let sleep = context.env.sleep(deadline);
    let timer = async {
        tokio::select! {
            biased;
            () = ended.cancelled() => {}
            () = sleep => {
                reached.store(true, Ordering::SeqCst);
                control.after_commit.cancel();
            }
        }
    };
    let run = async {
        let ran = drive(context, control.clone()).await;
        ended.cancel();
        ran
    };
    let ((status, error), ()) = tokio::join!(run, timer);
    if reached.load(Ordering::SeqCst) && status == RunStatus::Stopped {
        let status = if error.is_some() {
            RunStatus::Failed
        } else {
            RunStatus::Succeeded
        };
        return (status, error);
    }
    (status, error)
}

/// Runs attempts until one finishes, the retry policy gives up, or the run is stopped: how the
/// run ended, and the error that ended it.
async fn drive(context: &RunContext, control: RunControl) -> (RunStatus, Option<Error>) {
    let retry = *context.config.retry();
    let mut failures = 0;
    loop {
        let pipeline = context.plan.pipeline().clone();
        let attempt = context.live.attempted() + 1;
        let telling = Arc::clone(&context.telling);
        let announcer = Announcer::new(Arc::clone(&context.env), pipeline, attempt, telling);
        let log = Arc::new(Mutex::new(AttemptLog::default()));
        context.live.started(Arc::clone(&log), context.env.now());
        let result = attempted(context, &control, log, &announcer).await;
        let ended = context
            .live
            .ended(result.as_ref().err().map(Error::report), result.is_err());
        let progressed = match ended {
            Ok(ended) => told(&announcer, ended),
            Err(error) => return (RunStatus::Failed, Some(error)),
        };
        let error = match result {
            Ok(AttemptEnd::Exhausted) => return (RunStatus::Succeeded, None),
            Ok(AttemptEnd::Stopped) => return (RunStatus::Stopped, None),
            Err(error) => error,
        };
        if control.now.is_cancelled() {
            return (RunStatus::Cancelled, Some(error));
        }
        if progressed && retry.resets_after_progress() {
            failures = 0;
        }
        failures += 1;
        if !error.is_retryable() || failures >= retry.attempts().get() {
            return (RunStatus::Failed, Some(error));
        }
        let delay = backoff(&retry, &error, failures, context.env.as_ref());
        let now = context.env.instant();
        context
            .live
            .backing_off(now.checked_add(delay).unwrap_or(now), now);
        announcer.announce(|| EventKind::Retrying {
            scope: Scope::Attempt,
            error: error.report(),
            retry_in: delay,
        });
        tokio::select! {
            biased;
            () = control.now.cancelled() => return (RunStatus::Cancelled, Some(error)),
            () = control.after_commit.cancelled() => return (RunStatus::Stopped, Some(error)),
            () = context.env.sleep(delay) => {}
        }
    }
}

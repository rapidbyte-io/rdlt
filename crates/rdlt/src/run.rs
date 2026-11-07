//! A run of a pipeline: a future of its outcome, with its events and its status.

#[cfg(test)]
mod tests;

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use rdlt_engine::{
    ComputePool, Env, Event, Report, RunControl, RunHandle, RunOutcome, RunStatus, Sleep, Status,
    StopMode, SystemEnv,
};
use tokio::sync::broadcast;

use crate::{Error, Events};

/// A run in progress: await it for its [`Outcome`]; nothing happens until it is polled, and
/// dropping it cancels the run.
///
/// ```no_run
/// # async fn example(pipeline: rdlt::Pipeline) {
/// let run = pipeline.start();
/// let mut events = run.events();
/// let watching = tokio::spawn(async move {
///     while let Some(event) = events.next().await {
///         println!("{event:?}");
///     }
/// });
/// let outcome = run.await;
/// watching.await.ok();
/// println!("{:?}", outcome.status());
/// # }
/// ```
#[must_use = "a run does nothing unless it is awaited"]
pub struct Run {
    handle: RunHandle,
    events: broadcast::Sender<Event>,
}

impl Run {
    pub(crate) fn new(handle: RunHandle, events: broadcast::Sender<Event>) -> Self {
        Self { handle, events }
    }

    /// A new subscriber of the run's events, from the next one on.
    pub fn events(&self) -> Events {
        Events::new(self.events.subscribe())
    }

    /// What the run is doing now, and its report so far; it never waits.
    pub fn status(&self) -> Status {
        self.handle.status()
    }

    /// Asks the run to stop as `mode` says, from the task that holds the run.
    pub fn stop(&self, mode: StopMode) {
        self.handle.control().stop(mode);
    }

    /// A handle that stops the run from elsewhere: another task, or a signal handler, holds a
    /// clone while this task awaits the run.
    pub fn control(&self) -> RunControl {
        self.handle.control()
    }
}

impl fmt::Debug for Run {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Run")
            .field("subscribers", &self.events.receiver_count())
            .finish_non_exhaustive()
    }
}

impl Future for Run {
    type Output = Outcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Outcome> {
        Pin::new(&mut self.handle).poll(context).map(Outcome::of)
    }
}

/// How a run ended: its report, and the error that ended it, if one did.
///
/// ```no_run
/// # async fn example(pipeline: rdlt::Pipeline) {
/// let outcome = pipeline.start().await;
/// match &outcome.error {
///     None => println!("{:?}: {} rows", outcome.status(), outcome.report.rows),
///     Some(error) => eprintln!("{:?} failed: {error}", error.kind()),
/// }
/// # }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub struct Outcome {
    /// What the run committed, attempt by attempt.
    pub report: Report,
    /// The error that ended the run; none where it succeeded.
    pub error: Option<Error>,
}

impl Outcome {
    /// How the run ended.
    pub fn status(&self) -> RunStatus {
        self.report.status
    }

    /// The engine's `outcome`, its error as the facade reports one.
    fn of(outcome: RunOutcome) -> Self {
        Self {
            report: outcome.report,
            error: outcome.error.map(Error::engine),
        }
    }
}

/// The host's environment, telling one run's events to its subscribers: copying an event into
/// the ring is all it does on the engine's task.
pub(crate) struct Observed {
    pub(crate) base: Arc<SystemEnv>,
    pub(crate) events: broadcast::Sender<Event>,
}

impl Env for Observed {
    fn now(&self) -> SystemTime {
        self.base.now()
    }

    fn instant(&self) -> Instant {
        self.base.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        self.base.sleep(duration)
    }

    fn random(&self) -> u64 {
        self.base.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        self.base.compute()
    }

    fn cores(&self) -> NonZeroUsize {
        self.base.cores()
    }

    fn observe(&self, event: &Event) {
        // A run with no subscriber left is no failure of the run.
        self.events.send(event.clone()).ok();
    }

    fn observes(&self) -> bool {
        true
    }
}

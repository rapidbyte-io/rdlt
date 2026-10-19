//! The pool that runs CPU-bound work off the async runtime.
//!
//! The runtime's thread waits on the pool in one place: a caller let go while one of its jobs runs
//! waits for that job, bounded CPU work, as it is dropped, so no job outlives what its caller
//! charged for it.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::env::Env;
use crate::report::Tally;

/// A unit of CPU-bound work.
pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// Runs CPU-bound jobs so they never block the async runtime.
pub trait ComputePool: Send + Sync {
    /// Runs `job` to completion, on the pool's threads or inline.
    fn execute(&self, job: Job);
}

/// A [`ComputePool`] backed by a dedicated rayon thread pool.
#[derive(Debug)]
pub struct RayonPool {
    pool: rayon::ThreadPool,
}

/// The compute pool's threads failed to start, or the host's count of cores to size it by is
/// unknown.
#[derive(Debug, thiserror::Error)]
#[error("compute pool failed to start")]
pub struct ComputePoolError(#[source] pub(crate) Box<dyn std::error::Error + Send + Sync>);

/// The cores a run may use: the engine gives each a lane and a compute thread, and a process that
/// builds its own tokio runtime gives it [`runtime_workers`](Self::runtime_workers).
///
/// The runtime's workers run the engine's per-row work and its destination writes, and the
/// compute pool its CPU-bound jobs. The pool has a thread a core beside the workers, so CPU-bound
/// work can take every core the workers leave idle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cores {
    count: NonZeroUsize,
}

impl Cores {
    /// `count` cores.
    pub const fn new(count: NonZeroUsize) -> Self {
        Self { count }
    }

    /// The cores this process may run on.
    ///
    /// The count honours the process's CPU affinity mask and, on Linux, its cgroup's CPU quota.
    ///
    /// # Errors
    ///
    /// When the host does not say how many cores the process may run on.
    pub fn try_from_host() -> std::io::Result<Self> {
        Ok(Self::new(host_cores()?))
    }

    /// The cores a run may use.
    pub const fn count(self) -> NonZeroUsize {
        self.count
    }

    /// The worker threads a process that builds its own tokio runtime gives it: half the cores,
    /// rounded down, and two at least where there are two.
    ///
    /// One worker runs the engine's per-row work and every lane's writes alone, and more workers
    /// than half the cores slow a run of one partition.
    pub fn runtime_workers(self) -> NonZeroUsize {
        let count = self.count.get();
        NonZeroUsize::new(count.min(2).max(count / 2)).unwrap_or(NonZeroUsize::MIN)
    }
}

/// The cores this process may run on: the host's, less what its CPU affinity mask and, on
/// Linux, its cgroup's CPU quota leave it.
#[expect(
    clippy::disallowed_methods,
    reason = "the engine reads the host's cores here alone; a simulation declares its own"
)]
fn host_cores() -> std::io::Result<NonZeroUsize> {
    std::thread::available_parallelism()
}

impl RayonPool {
    /// Starts a pool of a thread for each of `cores`, named `rdlt-compute-<n>`, each with 8 MiB
    /// of stack.
    ///
    /// Shredding a JSON value at the nesting limit walks it once per level; the stack lets every
    /// job run without growing it, in every build.
    pub fn try_new(cores: Cores) -> Result<Self, ComputePoolError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(cores.count().get())
            .stack_size(8_388_608)
            .thread_name(|index| format!("rdlt-compute-{index}"))
            .build()
            .map(|pool| Self { pool })
            .map_err(|error| ComputePoolError(Box::new(error)))
    }
}

impl ComputePool for RayonPool {
    fn execute(&self, job: Job) {
        self.pool.spawn(job);
    }
}

/// A [`ComputePool`] that runs each job at once, on the calling thread.
#[cfg(any(test, feature = "bench"))]
pub(crate) struct Inline;

#[cfg(any(test, feature = "bench"))]
impl ComputePool for Inline {
    fn execute(&self, job: Job) {
        job();
    }
}

/// The output of `future`, whose compute jobs all run on an [`Inline`] pool, so it is ready at
/// its first poll.
#[cfg(any(test, feature = "bench"))]
pub(crate) fn ready<F: Future>(future: F) -> F::Output {
    match std::pin::pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("work on an inline pool finishes at once"),
    }
}

/// A run's compute pool, each job's wait to start and its run timed on the run's clock.
#[derive(Clone)]
pub(crate) struct Pool {
    env: Arc<dyn Env>,
    tally: Arc<Tally>,
}

impl Pool {
    /// The pool of `env`, counting into `tally`.
    pub(crate) fn new(env: Arc<dyn Env>, tally: Arc<Tally>) -> Self {
        Self { env, tally }
    }

    /// A pool that runs each job at once, on the calling thread, counting into a tally of its
    /// own.
    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn inline() -> Self {
        Self::new(Arc::new(crate::env::InlineEnv), Arc::default())
    }

    /// Runs every job of `work` on the pool, all at once, and returns their results in `work`'s
    /// order.
    ///
    /// A panic inside a job resumes in the caller once the jobs before it have finished. No job
    /// outlives its caller holding what the caller's charges cover: a caller that stops waiting
    /// drops its jobs not yet started, and its drop waits, on the thread dropping it, for those
    /// running to end, bounded CPU work that never waits on the runtime.
    pub(crate) async fn run_all<T, F>(&self, work: impl IntoIterator<Item = F>) -> Vec<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let slots = work
            .into_iter()
            .map(|job| {
                let env = Arc::clone(&self.env);
                let queued = env.instant();
                let slot = Arc::new(Slot::new(job));
                let running = Arc::clone(&slot);
                self.env.compute().execute(Box::new(move || {
                    let Some(job) = running.start() else {
                        return;
                    };
                    // Ends the slot however the run ends, handing the result back.
                    let mut ending = Ending(&running, None);
                    let started = env.instant();
                    let done = panic::catch_unwind(AssertUnwindSafe(job));
                    let ran = Ran {
                        queued: started.saturating_duration_since(queued),
                        running: env.instant().saturating_duration_since(started),
                    };
                    ending.1 = Some((done, ran));
                }));
                (queued, slot)
            })
            .collect();
        let mut waited = Waited { slots, taken: 0 };
        let mut values = Vec::with_capacity(waited.slots.len());
        while let Some((queued, slot)) = waited.slots.get(waited.taken) {
            let (queued, slot) = (*queued, Arc::clone(slot));
            let (done, ran) = std::future::poll_fn(|cx| slot.poll(cx))
                .await
                .expect("compute pools run every job they accept");
            waited.taken += 1;
            let ended = queued + ran.queued + ran.running;
            let returned = self.env.instant().saturating_duration_since(ended);
            self.tally.add(|counters| {
                let pool = &mut counters.pool;
                pool.jobs = pool.jobs.saturating_add(1);
                pool.queued = pool.queued.saturating_add(ran.queued);
                pool.running = pool.running.saturating_add(ran.running);
                pool.returned = pool.returned.saturating_add(returned);
            });
            match done {
                Ok(value) => values.push(value),
                Err(payload) => panic::resume_unwind(payload),
            }
        }
        values
    }
}

/// What a job ran to, and how long it waited and ran.
type Done<T> = (std::thread::Result<T>, Ran);

/// A job handed to the pool and, once it ran, its result: the pool takes the job to run it, and
/// a caller that stops waiting takes it to drop it unrun, or waits for it to end where it runs and
/// drops what it made.
struct Slot<F, T> {
    state: parking_lot::Mutex<State<F, T>>,
    ended: parking_lot::Condvar,
}

struct State<F, T> {
    stage: Stage<F, T>,
    /// The caller waiting for the result.
    waker: Option<Waker>,
}

/// Where a job handed to the pool is.
enum Stage<F, T> {
    Waiting(F),
    Running,
    /// Ended, with its result until the caller takes it.
    Ended(Option<Done<T>>),
}

impl<F, T> Slot<F, T> {
    fn new(job: F) -> Self {
        let state = State {
            stage: Stage::Waiting(job),
            waker: None,
        };
        Self {
            state: parking_lot::Mutex::new(state),
            ended: parking_lot::Condvar::new(),
        }
    }

    /// The job, to run now; none where its caller let it go.
    fn start(&self) -> Option<F> {
        let mut state = self.state.lock();
        match std::mem::replace(&mut state.stage, Stage::Running) {
            Stage::Waiting(job) => Some(job),
            other => {
                state.stage = other;
                None
            }
        }
    }

    /// Ends the job with `done`, what it ran to, for its caller.
    fn end(&self, done: Option<Done<T>>) {
        let waker = {
            let mut state = self.state.lock();
            state.stage = Stage::Ended(done);
            state.waker.take()
        };
        self.ended.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// The job's result once it ended; none where it ended without one.
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Option<Done<T>>> {
        let mut state = self.state.lock();
        if let Stage::Ended(done) = &mut state.stage {
            return Poll::Ready(done.take());
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    /// Lets the job go: dropped unrun where it has not started, and where it runs, waited for and
    /// what it made dropped.
    fn abandon(&self) {
        let mut state = self.state.lock();
        if let Stage::Waiting(_) = &state.stage {
            let unrun = std::mem::replace(&mut state.stage, Stage::Ended(None));
            drop(state);
            drop(unrun);
            return;
        }
        while let Stage::Running = &state.stage {
            self.ended.wait(&mut state);
        }
        let made = match &mut state.stage {
            Stage::Ended(done) => done.take(),
            _ => None,
        };
        drop(state);
        drop(made);
    }
}

/// Ends a running job's slot when dropped, with the result it holds, however the run ends.
struct Ending<'a, F, T>(&'a Slot<F, T>, Option<Done<T>>);

impl<F, T> Drop for Ending<'_, F, T> {
    fn drop(&mut self) {
        self.0.end(self.1.take());
    }
}

/// The jobs of a call, the first `taken` of whose results its caller took; dropped, it lets the
/// rest go.
struct Waited<F, T> {
    slots: Vec<(Instant, Arc<Slot<F, T>>)>,
    taken: usize,
}

impl<F, T> Drop for Waited<F, T> {
    fn drop(&mut self) {
        for (_, slot) in self.slots.iter().skip(self.taken) {
            slot.abandon();
        }
    }
}

/// How long a job waited to start, and ran.
struct Ran {
    queued: Duration,
    running: Duration,
}

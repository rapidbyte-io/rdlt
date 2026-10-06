//! The pool that runs CPU-bound work off the async runtime.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
#[cfg(any(test, feature = "bench"))]
use std::task::{Context, Poll, Waker};

use tokio::sync::oneshot;

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
pub struct ComputePoolError(#[source] Box<dyn std::error::Error + Send + Sync>);

/// The cores a run may use, and how many of them the embedder's tokio runtime takes for its
/// worker threads.
///
/// The runtime's workers run the engine's per-row work and its destination writes, and the
/// compute pool its CPU-bound jobs; sized apart, the two contend for the same cores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cores {
    count: NonZeroUsize,
    workers: NonZeroUsize,
}

impl Cores {
    /// `count` cores, `workers` of them the runtime's worker threads: the count the runtime was
    /// built with, which `tokio::runtime::Handle::current().metrics().num_workers()` reads.
    pub const fn new(count: NonZeroUsize, workers: NonZeroUsize) -> Self {
        Self { count, workers }
    }

    /// `count` cores, half of them, rounded down and one at least, the runtime's workers.
    ///
    /// `docs/perf/passthrough.md` records the layouts measured and why this one is chosen.
    pub const fn from_count(count: NonZeroUsize) -> Self {
        let workers = match NonZeroUsize::new(count.get() / 2) {
            Some(workers) => workers,
            None => NonZeroUsize::MIN,
        };
        Self::new(count, workers)
    }

    /// The cores this process may run on, split as [`from_count`](Self::from_count) splits them:
    /// what a process that builds its own runtime gives it.
    ///
    /// The count honours the process's CPU affinity mask and, on Linux, its cgroup's CPU quota.
    ///
    /// # Errors
    ///
    /// When the host does not say how many cores the process may run on.
    pub fn try_from_host() -> Result<Self, ComputePoolError> {
        Ok(Self::from_count(host_cores()?))
    }

    /// The cores a run may use.
    pub const fn count(self) -> NonZeroUsize {
        self.count
    }

    /// The runtime's worker threads.
    pub const fn workers(self) -> NonZeroUsize {
        self.workers
    }

    /// The compute pool's threads: the cores the runtime's workers leave, and one at least, so
    /// CPU-bound jobs always have a thread.
    pub const fn compute_threads(self) -> NonZeroUsize {
        match NonZeroUsize::new(self.count.get().saturating_sub(self.workers.get())) {
            Some(threads) => threads,
            None => NonZeroUsize::MIN,
        }
    }

    /// The host's cores, beside `runtime`'s workers.
    pub(crate) fn try_beside(runtime: &tokio::runtime::Handle) -> Result<Self, ComputePoolError> {
        let workers =
            NonZeroUsize::new(runtime.metrics().num_workers()).expect("a runtime has a worker");
        Ok(Self::new(host_cores()?, workers))
    }
}

/// The cores this process may run on: the host's, less what its CPU affinity mask and, on
/// Linux, its cgroup's CPU quota leave it.
#[expect(
    clippy::disallowed_methods,
    reason = "the engine reads the host's cores here alone; a simulation declares its own"
)]
fn host_cores() -> Result<NonZeroUsize, ComputePoolError> {
    std::thread::available_parallelism().map_err(|error| ComputePoolError(Box::new(error)))
}

impl RayonPool {
    /// Starts a pool of [`Cores::compute_threads`] threads named `rdlt-compute-<n>`, each with
    /// 8 MiB of stack.
    ///
    /// Shredding a JSON value at the nesting limit walks it once per level; the stack lets every
    /// job run without growing it, in every build.
    pub fn try_new(cores: Cores) -> Result<Self, ComputePoolError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(cores.compute_threads().get())
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

/// Runs every job of `work` on `pool`, all at once, and returns their results in `work`'s order.
///
/// A panic inside a job resumes in the caller once the jobs before it have finished.
pub(crate) async fn run_all<T, F>(
    pool: &dyn ComputePool,
    work: impl IntoIterator<Item = F>,
) -> Vec<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let receivers: Vec<_> = work
        .into_iter()
        .map(|job| {
            let (sender, receiver) = oneshot::channel();
            pool.execute(Box::new(move || {
                // The caller may have stopped waiting; its result is then not needed.
                drop(sender.send(panic::catch_unwind(AssertUnwindSafe(job))));
            }));
            receiver
        })
        .collect();
    let mut values = Vec::with_capacity(receivers.len());
    for receiver in receivers {
        match receiver
            .await
            .expect("compute pools run every job they accept")
        {
            Ok(value) => values.push(value),
            Err(payload) => panic::resume_unwind(payload),
        }
    }
    values
}

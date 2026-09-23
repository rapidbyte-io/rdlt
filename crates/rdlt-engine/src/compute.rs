//! The pool that runs CPU-bound work off the async runtime.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};

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

/// The compute pool's threads failed to start.
#[derive(Debug, thiserror::Error)]
#[error("compute pool failed to start")]
pub struct ComputePoolError(#[source] rayon::ThreadPoolBuildError);

impl RayonPool {
    /// Starts a pool of `threads` worker threads named `rdlt-compute-<n>`.
    pub fn new(threads: NonZeroUsize) -> Result<Self, ComputePoolError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads.get())
            .thread_name(|index| format!("rdlt-compute-{index}"))
            .build()
            .map(|pool| Self { pool })
            .map_err(ComputePoolError)
    }
}

impl ComputePool for RayonPool {
    fn execute(&self, job: Job) {
        self.pool.spawn(job);
    }
}

/// Runs `work` on `pool` and returns its result.
///
/// A panic inside `work` resumes in the caller, so it surfaces in the task that asked for the work.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the engine stages call this from M2 on")
)]
pub(crate) async fn run<T, F>(pool: &dyn ComputePool, work: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    pool.execute(Box::new(move || {
        // The caller may have stopped waiting; its result is then not needed.
        drop(sender.send(panic::catch_unwind(AssertUnwindSafe(work))));
    }));
    match receiver
        .await
        .expect("compute pools run every job they accept")
    {
        Ok(value) => value,
        Err(payload) => panic::resume_unwind(payload),
    }
}

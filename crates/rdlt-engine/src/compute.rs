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

/// The compute pool's threads failed to start.
#[derive(Debug, thiserror::Error)]
#[error("compute pool failed to start")]
pub struct ComputePoolError(#[source] rayon::ThreadPoolBuildError);

impl RayonPool {
    /// Starts a pool of `threads` worker threads named `rdlt-compute-<n>`, each with 8 MiB of
    /// stack.
    ///
    /// Shredding a JSON value at the nesting limit walks it once per level; the stack lets every
    /// job run without growing it, in every build.
    pub fn new(threads: NonZeroUsize) -> Result<Self, ComputePoolError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads.get())
            .stack_size(8_388_608)
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

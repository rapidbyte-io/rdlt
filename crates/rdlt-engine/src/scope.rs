//! Structured concurrency: tasks owned by a scope that cancels and aborts them together.

#[cfg(test)]
mod tests;

use std::any::Any;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::task::Poll;

use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

/// An error a [`TaskScope`] can collect from its tasks.
pub(crate) trait ScopeError: Send + 'static {
    /// Whether the error only reports that its task observed cancellation.
    fn is_cancelled(&self) -> bool;

    /// The error reported for a task that panicked or was aborted.
    fn panicked(message: String) -> Self;
}

/// Owns a set of tasks; dropping the scope cancels its token and aborts every task.
pub(crate) struct TaskScope<E: ScopeError> {
    tasks: JoinSet<Result<(), E>>,
    cancel: CancellationToken,
}

impl<E: ScopeError> TaskScope<E> {
    /// Creates a scope whose token is a child of `parent`.
    pub(crate) fn new(parent: &CancellationToken) -> Self {
        Self {
            tasks: JoinSet::new(),
            cancel: parent.child_token(),
        }
    }

    /// The scope's cancellation token; tasks watch it to stop early.
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.cancel
    }

    /// Starts `task` inside the scope.
    #[expect(
        clippy::disallowed_methods,
        reason = "TaskScope is the owner every task needs"
    )]
    pub(crate) fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
    {
        self.tasks.spawn(task);
    }

    /// Waits for every task.
    ///
    /// The first error cancels the scope. The result is the first error that is not a
    /// cancellation or, when every error is a cancellation, the first cancellation.
    pub(crate) async fn join(mut self) -> Result<(), E> {
        let mut first: Option<E> = None;
        while let Some(joined) = self.tasks.join_next().await {
            let outcome = joined.unwrap_or_else(|error| Err(E::panicked(describe(error))));
            if let Err(error) = outcome {
                self.cancel.cancel();
                first = Some(match first {
                    Some(current) if !current.is_cancelled() || error.is_cancelled() => current,
                    _ => error,
                });
            }
        }
        first.map_or(Ok(()), Err)
    }
}

impl<E: ScopeError> Drop for TaskScope<E> {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.tasks.abort_all();
    }
}

/// The panic message of a task that panicked, or a fixed text when there is none.
fn describe(error: JoinError) -> String {
    match error.try_into_panic() {
        Ok(payload) => message(payload.as_ref()),
        Err(_) => "task was aborted".to_owned(),
    }
}

/// The message a panic carried, or a fixed text when it carried none.
fn message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "task panicked".to_owned())
}

/// A future panicked; the panic's message.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct Panicked(String);

/// Awaits `future` on the calling task, containing a panic it raises: its output, or the panic.
///
/// A future that panicked is dropped, never polled again.
pub(crate) async fn contained<F: Future>(future: F) -> Result<F::Output, Panicked> {
    let mut future = pin!(future);
    poll_fn(
        |context| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(Panicked(message(payload.as_ref())))),
        },
    )
    .await
}

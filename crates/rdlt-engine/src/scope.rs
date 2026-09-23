//! Structured concurrency: tasks owned by a scope that cancels and aborts them together.

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "the run orchestrator uses scopes from M2 on")
)]

#[cfg(test)]
mod tests;

use std::future::Future;

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
    let Ok(payload) = error.try_into_panic() else {
        return "task was aborted".to_owned();
    };
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "task panicked".to_owned())
}

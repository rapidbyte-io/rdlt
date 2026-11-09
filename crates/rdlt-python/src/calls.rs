//! One Python function called from Rust, on a daemon thread of Python's own.
//!
//! A user's function never runs with Rust frames beneath it: an interpreter that exits ends the
//! thread that runs it, and on `CPython` 3.11, 3.12 and early 3.13 that unwind through Rust frames
//! aborts the process (ADR 0055). Rust only starts the call and reads its answer.

#[cfg(test)]
mod tests;

use parking_lot::Mutex;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAnyMethods as _, PyModule, PyTuple};
use pyo3::{Bound, Py, PyAny, PyErr, Python, intern, pyclass, pymethods};
use tokio::sync::oneshot;
use tokio::task::JoinError;

/// What a call answered: what it returned, or what it raised.
pub(crate) type Answered = Result<Py<PyAny>, PyErr>;

/// Where a call's thread says how it ended.
#[pyclass(module = "rdlt._native", name = "CallAnswer", frozen)]
struct CallAnswer(Mutex<Option<oneshot::Sender<Answered>>>);

impl CallAnswer {
    fn give(&self, answered: Answered) {
        let sender = self.0.lock().take();
        if let Some(sender) = sender {
            sender.send(answered).ok();
        }
    }
}

#[pymethods]
impl CallAnswer {
    /// Says the call returned `value`.
    fn returned(&self, value: Py<PyAny>) {
        self.give(Ok(value));
    }

    /// Says the call raised `error`.
    fn raised(&self, error: &Bound<'_, PyAny>) {
        self.give(Err(PyErr::from_value(error.clone())));
    }
}

/// The package's module of thread bodies, imported once: the package imports it as it loads.
pub(crate) fn reading(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    static READING: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    let reading =
        READING.get_or_try_init(py, || Ok::<_, PyErr>(py.import("rdlt._reading")?.unbind()))?;
    Ok(reading.bind(py).clone())
}

/// Starts `function(*arguments)` on a daemon thread of its own; its receiver answers what the
/// function returned or raised, and none once its thread ended without answering.
///
/// # Errors
///
/// What starting the thread raises.
pub(crate) fn start(
    py: Python<'_>,
    function: &Bound<'_, PyAny>,
    arguments: &Bound<'_, PyTuple>,
) -> Result<oneshot::Receiver<Answered>, PyErr> {
    let (sender, received) = oneshot::channel();
    let answer = Py::new(py, CallAnswer(Mutex::new(Some(sender))))?;
    reading(py)?.call_method1(intern!(py, "call"), (function, arguments, answer))?;
    Ok(received)
}

/// Why a call made from async code has no answer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Unanswered {
    /// The call's thread ended without answering.
    #[error("the call's thread ended without answering")]
    Thread,
    /// Starting the call or reading its answer panicked.
    #[error("the call was not started or read to its end")]
    Joined(#[from] JoinError),
}

/// Calls the function `prepare` names, with the arguments it builds, and answers `finish` of
/// what it returned or raised.
///
/// The runtime owns the call's thread: a caller that stops waiting leaves the call to end, its
/// answer dropped, since Python code cannot be interrupted.
///
/// # Errors
///
/// [`Unanswered`] where the call's thread ended without answering, or a step panicked.
pub(crate) async fn call<T, P, F>(prepare: P, finish: F) -> Result<T, Unanswered>
where
    T: Send + 'static,
    P: for<'py> FnOnce(Python<'py>) -> Result<(Bound<'py, PyAny>, Bound<'py, PyTuple>), PyErr>
        + Send
        + 'static,
    F: for<'py> FnOnce(Python<'py>, Result<Bound<'py, PyAny>, PyErr>) -> T + Send + 'static,
{
    let started = tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            let (function, arguments) = prepare(py)?;
            start(py, &function, &arguments)
        })
    })
    .await?;
    let answered = match started {
        Ok(received) => received.await.map_err(|_| Unanswered::Thread)?,
        Err(error) => Err(error),
    };
    let finished = tokio::task::spawn_blocking(move || {
        Python::attach(|py| finish(py, answered.map(|value| value.into_bound(py))))
    })
    .await?;
    Ok(finished)
}

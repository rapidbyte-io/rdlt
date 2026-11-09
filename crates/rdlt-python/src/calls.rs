//! One Python function called from Rust, on a daemon thread of Python's own.
//!
//! A user's function never runs with Rust frames beneath it: an interpreter that exits ends the
//! thread that runs it, and on `CPython` 3.11, 3.12 and early 3.13 that unwind through Rust frames
//! aborts the process (ADR 0055). The caller holds no GIL and touches nothing of Python's: it
//! hands the call over as closures, and its arguments are built and its answer read on the
//! daemon thread. A call has a daemon thread of its own, since a runtime's worker runs many tasks
//! and one that never returns must not hold up the rest.

#[cfg(test)]
mod tests;

use std::sync::mpsc;

use parking_lot::Mutex;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAnyMethods as _, PyModule, PyTuple};
use pyo3::{Bound, Py, PyAny, PyErr, Python, intern, pyclass, pymethods};
use tokio::sync::oneshot;
use tokio::task::JoinError;

/// What a call answered: what it returned, or what it raised.
pub(crate) type Answered<'py> = Result<Bound<'py, PyAny>, PyErr>;

/// What to call and its arguments, built on the daemon thread.
type Prepare = Box<
    dyn for<'py> FnOnce(Python<'py>) -> Result<(Bound<'py, PyAny>, Bound<'py, PyTuple>), PyErr>
        + Send,
>;

/// Where a call's answer goes, read on the daemon thread with the GIL held.
type Give = Box<dyn for<'py> FnOnce(Python<'py>, Answered<'py>) + Send>;

/// A call as `rdlt._reading` unpacks it: the function, its arguments, and where to answer.
type Offered<'py> = (Bound<'py, PyAny>, Bound<'py, PyTuple>, Py<CallAnswer>);

/// A call for a daemon thread to make.
struct Call {
    prepare: Prepare,
    give: Give,
}

/// Where a call's thread says how it ended.
#[pyclass(module = "rdlt._native", name = "CallAnswer", frozen)]
struct CallAnswer(Mutex<Option<Give>>);

impl CallAnswer {
    fn give<'py>(&self, py: Python<'py>, answered: Answered<'py>) {
        let give = self.0.lock().take();
        if let Some(give) = give {
            give(py, answered);
        }
    }
}

#[pymethods]
impl CallAnswer {
    /// Says the call returned `value`.
    fn returned<'py>(&self, py: Python<'py>, value: Bound<'py, PyAny>) {
        self.give(py, Ok(value));
    }

    /// Says the call raised `error`.
    fn raised<'py>(&self, py: Python<'py>, error: &Bound<'py, PyAny>) {
        self.give(py, Err(PyErr::from_value(error.clone())));
    }
}

/// The calls one thread of Rust hands its daemon thread, which ends with them once that thread
/// has ended.
#[pyclass(module = "rdlt._native", name = "Calls", frozen)]
struct Calls(Mutex<mpsc::Receiver<Call>>);

#[pymethods]
impl Calls {
    /// The next call to make, the GIL released while it waits; none once its thread has ended.
    ///
    /// A call whose arguments cannot be built is answered with why, here.
    fn next<'py>(&self, py: Python<'py>) -> Result<Option<Offered<'py>>, PyErr> {
        loop {
            let Some(call) = py.detach(|| self.0.lock().recv().ok()) else {
                return Ok(None);
            };
            match (call.prepare)(py) {
                Ok((function, arguments)) => {
                    let answer = Py::new(py, CallAnswer(Mutex::new(Some(call.give))))?;
                    return Ok(Some((function, arguments, answer)));
                }
                Err(error) => (call.give)(py, Err(error)),
            }
        }
    }
}

/// The package's module of thread bodies, imported once: the package imports it as it loads.
pub(crate) fn reading(py: Python<'_>) -> Result<Bound<'_, PyModule>, PyErr> {
    static READING: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    let reading =
        READING.get_or_try_init(py, || Ok::<_, PyErr>(py.import("rdlt._reading")?.unbind()))?;
    Ok(reading.bind(py).clone())
}

/// Starts a daemon thread that makes each call its sender is handed, and ends once that sender is
/// dropped.
fn serving(py: Python<'_>) -> Result<mpsc::Sender<Call>, PyErr> {
    let (sender, calls) = mpsc::channel();
    let calls = Py::new(py, Calls(Mutex::new(calls)))?;
    reading(py)?.call_method1(intern!(py, "serve"), (calls,))?;
    Ok(sender)
}

/// Why a call has no answer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Unanswered {
    /// The call's thread ended without answering.
    #[error("the call's thread ended without answering")]
    Thread,
    /// The call's thread did not start.
    #[error("the call's thread did not start")]
    Started(#[source] PyErr),
    /// Starting the call panicked.
    #[error("the call was not started")]
    Joined(#[from] JoinError),
}

/// Calls the function `prepare` names, with the arguments it builds, on a daemon thread of its
/// own, and answers `finish` of what it returned or raised on that thread.
///
/// A caller that stops waiting leaves the call to end, its answer dropped, since Python code
/// cannot be interrupted.
///
/// # Errors
///
/// [`Unanswered`] where the call's thread ended without answering or did not start.
pub(crate) async fn call<T, P, F>(prepare: P, finish: F) -> Result<T, Unanswered>
where
    T: Send + 'static,
    P: for<'py> FnOnce(Python<'py>) -> Result<(Bound<'py, PyAny>, Bound<'py, PyTuple>), PyErr>
        + Send
        + 'static,
    F: for<'py> FnOnce(Python<'py>, Answered<'py>) -> T + Send + 'static,
{
    let (sender, received) = oneshot::channel();
    let call = Call {
        prepare: Box::new(prepare),
        give: Box::new(move |py, answered| {
            sender.send(finish(py, answered)).ok();
        }),
    };
    tokio::task::spawn_blocking(move || {
        Python::attach(|py| {
            // Handed over and its sender dropped, so the thread ends with this call.
            serving(py)?.send(call).ok();
            Ok::<(), PyErr>(())
        })
    })
    .await?
    .map_err(Unanswered::Started)?;
    received.await.map_err(|_| Unanswered::Thread)
}

//! An asyncio awaitable of a future the runtime runs, whose answer a daemon thread hands to the
//! event loop awaiting it, so no thread of Rust's takes the GIL for it.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, IntoPyObject as _, Py, PyAny, PyErr, Python, intern, pyclass, pymethods};
use tokio::task::{AbortHandle, JoinError};

use crate::calls::{self, Unanswered};
use crate::{errors, runtime};

/// How often an answer that found no daemon thread idle is offered again.
const AGAIN: Duration = Duration::from_millis(50);

/// What a future ended with and how its awaitable answers it, kept until a daemon thread takes
/// it.
struct Ended<T, A> {
    outcome: Result<T, JoinError>,
    answer: A,
    event_loop: Py<PyAny>,
    awaited: Py<PyAny>,
}

/// An asyncio future, of the event loop running on this thread, of what `future` ends with as
/// `answer` reads it on a daemon thread; cancelling it, or its loop's ending, drops `future`'s
/// answer.
///
/// # Errors
///
/// What `runtime::get` refuses, and a `RuntimeError` where no event loop runs on this thread.
pub(crate) fn awaitable<'py, T, F, A>(
    py: Python<'py>,
    future: F,
    answer: A,
) -> Result<Bound<'py, PyAny>, PyErr>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
    A: for<'a> FnOnce(Python<'a>, T) -> Result<Bound<'a, PyAny>, PyErr> + Send + 'static,
{
    let runtime = runtime::get(py)?;
    let event_loop = py
        .import("asyncio")?
        .call_method0(intern!(py, "get_running_loop"))?;
    let awaited = event_loop.call_method0(intern!(py, "create_future"))?;
    let task = runtime.spawn(future);
    awaited.call_method1(
        intern!(py, "add_done_callback"),
        (Cancel(task.abort_handle()),),
    )?;
    let (event_loop, target) = (event_loop.unbind(), awaited.clone().unbind());
    drop(runtime.spawn(async move {
        let ended = task.await;
        if matches!(&ended, Err(joined) if joined.is_cancelled()) {
            return;
        }
        let kept = Arc::new(Mutex::new(Some(Ended {
            outcome: ended,
            answer,
            event_loop,
            awaited: target,
        })));
        // A daemon thread that did not start leaves the answer to the next that goes idle.
        while let Err(Unanswered::Started(_)) = handed(&kept) {
            tokio::time::sleep(AGAIN).await;
        }
    }));
    Ok(awaited)
}

/// Hands what `kept` holds to the daemon thread that went idle last, which builds the answer and
/// gives it to `_reading.awaited`.
fn handed<T, A>(kept: &Arc<Mutex<Option<Ended<T, A>>>>) -> Result<(), Unanswered>
where
    T: Send + 'static,
    A: for<'a> FnOnce(Python<'a>, T) -> Result<Bound<'a, PyAny>, PyErr> + Send + 'static,
{
    let kept = Arc::clone(kept);
    calls::start(
        move |py| {
            let Ended {
                outcome,
                answer,
                event_loop,
                awaited,
            } = kept
                .lock()
                .take()
                .expect("an answer is handed to one daemon thread");
            let answered = outcome
                .map_err(|joined| {
                    errors::internal(
                        py,
                        &format!("an awaited task ended without its answer: {joined}"),
                    )
                })
                .and_then(|value| answer(py, value));
            let (value, error) = match answered {
                Ok(value) => (value, None),
                Err(error) => (py.None().into_bound(py), Some(error.into_value(py))),
            };
            let arguments = (event_loop, awaited, value, error).into_pyobject(py)?;
            Ok((
                calls::reading(py)?.getattr(intern!(py, "awaited"))?,
                arguments,
            ))
        },
        |_, _| Box::new(|| ()),
    )
}

/// Lets go of the future an awaitable waits on, once that awaitable is done or cancelled.
#[pyclass(module = "rdlt._native", name = "Cancel", frozen)]
struct Cancel(AbortHandle);

#[pymethods]
impl Cancel {
    fn __call__(&self, _awaited: &Bound<'_, PyAny>) {
        self.0.abort();
    }
}

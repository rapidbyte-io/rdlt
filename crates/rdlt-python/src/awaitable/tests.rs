use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods as _, PyCFunction, PyDict, PyDictMethods as _, PyTypeMethods as _};
use pyo3::{Bound, IntoPyObjectExt as _, Py, PyAny, PyErr, Python};

use super::awaitable;
use crate::calls::tests::Doomed;
use crate::embedded;

/// What `asyncio.run` makes of `code`, a coroutine function's body, run with `make` named: a
/// function giving the awaitable under test, called on the event loop's thread.
fn awaited<'py>(
    py: Python<'py>,
    code: &std::ffi::CStr,
    make: impl Fn(Python<'_>) -> Result<Py<PyAny>, PyErr> + Send + Sync + 'static,
) -> Result<Bound<'py, PyAny>, PyErr> {
    let make = PyCFunction::new_closure(py, None, None, move |arguments, _| make(arguments.py()))?;
    let globals = PyDict::new(py);
    globals.set_item("make", make)?;
    py.run(code, Some(&globals), None)?;
    Ok(globals.get_item("answer")?.expect("the coroutine answered"))
}

const AWAITED: &std::ffi::CStr = c"
import asyncio
async def main():
    return await asyncio.wait_for(make(), 30)
answer = asyncio.run(main())
";

#[test]
fn an_await_answers_what_its_future_ends_with() {
    embedded::with(|py| {
        let answer = awaited(py, AWAITED, |py| {
            awaitable(py, async { 7_i64 }, |py, n| n.into_bound_py_any(py)).map(Bound::unbind)
        })
        .unwrap();
        assert_eq!(answer.extract::<i64>().unwrap(), 7);
    });
}

#[test]
fn an_await_raises_what_reading_its_answer_raised() {
    embedded::with(|py| {
        let raised = awaited(py, AWAITED, |py| {
            awaitable(py, async {}, |_, ()| Err(PyValueError::new_err("refused")))
                .map(Bound::unbind)
        })
        .expect_err("the await raised");
        assert!(raised.is_instance_of::<PyValueError>(py), "{raised}");
    });
}

#[test]
fn an_await_whose_future_panicked_raises_an_internal_error() {
    embedded::with(|py| {
        let raised = awaited(py, AWAITED, |py| {
            awaitable(
                py,
                async { panic!("a bug in an awaited future") },
                |py, ()| py.None().into_bound_py_any(py),
            )
            .map(Bound::unbind)
        })
        .expect_err("the await raised");
        assert_eq!(
            raised.get_type(py).name().unwrap().to_string(),
            "InternalError"
        );
    });
}

/// Says it was dropped.
struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[test]
fn an_await_whose_answer_was_queued_on_a_spare_that_did_not_start_is_answered() {
    let doomed = Doomed::holding();
    // The answer is queued on that spare, which does not start, then offered again until the
    // held daemon thread is let go and takes it.
    let releasing = std::thread::spawn(move || {
        doomed.refused_once_queued();
        doomed.released();
    });
    let answer = embedded::with(|py| {
        let held_on = c"
import asyncio
async def main():
    return await asyncio.wait_for(make(), 30)
answer = asyncio.run(main())
";
        awaited(py, held_on, |py| {
            awaitable(py, async { 7_i64 }, |py, n| n.into_bound_py_any(py)).map(Bound::unbind)
        })
        .map(|answer| answer.extract::<i64>().unwrap())
    });
    assert_eq!(answer.unwrap(), 7);
    releasing.join().unwrap();
}

#[test]
fn a_cancelled_await_lets_go_of_its_future() {
    let dropped = Arc::new(AtomicBool::new(false));
    let held = Arc::clone(&dropped);
    embedded::with(|py| {
        // An answer is handed over within moments where one is: none is to an await cancelled.
        let cancelled = c"
import asyncio, rdlt._reading as reading
handed = []
reading.awaited = lambda *answered: handed.append(answered)
async def main():
    awaited = make()
    await asyncio.sleep(0)
    awaited.cancel()
    await asyncio.sleep(0.3)
    return awaited.cancelled() and not handed
answer = asyncio.run(main())
";
        let answer = awaited(py, cancelled, move |py| {
            let guard = Dropped(Arc::clone(&held));
            let pending = async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            };
            awaitable(py, pending, |py, ()| py.None().into_bound_py_any(py)).map(Bound::unbind)
        })
        .unwrap();
        assert!(
            answer.extract::<bool>().unwrap(),
            "the await was cancelled, and nothing handed to it"
        );
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    while !dropped.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the cancelled await's future was let go"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

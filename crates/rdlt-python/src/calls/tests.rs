use std::time::Duration;

use pyo3::types::{PyAnyMethods as _, PyTuple};
use pyo3::{Py, PyAny, PyErr, Python};

use super::{Unanswered, call};
use crate::{embedded, runtime};

/// A call of `code`, a callable expression, with no arguments, as `finish` reads its answer.
fn calling<T: Send + 'static>(
    py: Python<'_>,
    code: &'static std::ffi::CStr,
    finish: impl for<'py> FnOnce(Python<'py>, Result<pyo3::Bound<'py, PyAny>, PyErr>) -> T
    + Send
    + 'static,
) -> Result<T, Unanswered> {
    let runtime = runtime::get(py).unwrap();
    py.detach(|| {
        runtime.block_on(async move {
            tokio::time::timeout(
                Duration::from_secs(30),
                call(
                    move |py| Ok((py.eval(code, None, None)?, PyTuple::empty(py))),
                    finish,
                ),
            )
            .await
            .expect("within 30 s")
        })
    })
}

#[test]
fn a_function_runs_on_a_daemon_thread_of_python_s_own() {
    embedded::with(|py| {
        let answered = calling(
            py,
            c"lambda: (__import__('threading').current_thread().name, __import__('threading').current_thread().daemon)",
            |_, returned| {
                returned
                    .and_then(|value| value.extract::<(String, bool)>())
                    .unwrap()
            },
        )
        .unwrap();
        assert_eq!(answered, ("rdlt-call".to_owned(), true));
    });
}

#[test]
fn what_a_function_raises_is_its_answer() {
    embedded::with(|py| {
        let raised = calling(py, c"lambda: 1 / 0", |py, returned| {
            let error = returned.expect_err("it raised");
            error.is_instance_of::<pyo3::exceptions::PyZeroDivisionError>(py)
        })
        .unwrap();
        assert!(raised);
    });
}

#[test]
fn a_call_goes_on_to_its_end_though_nothing_waits_for_it() {
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.finished = threading.Event()",
            None,
            None,
        )
        .unwrap();
        let runtime = runtime::get(py).unwrap();
        // Polled once, then dropped: the call has been handed over.
        let dropped = runtime.block_on(async {
            let call = called(c"lambda: __import__('builtins').finished.set()");
            tokio::time::timeout(Duration::ZERO, call).await
        });
        assert!(dropped.is_err(), "the wait was let go before the answer");
        let finished = py.detach(|| {
            Python::attach(|py| {
                py.eval(c"__import__('builtins').finished.wait(10)", None, None)
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            })
        });
        assert!(finished);
    });
}

/// A call of `code`, a callable expression, with no arguments, awaited on `runtime`.
async fn called(code: &'static std::ffi::CStr) -> Result<Result<Py<PyAny>, PyErr>, Unanswered> {
    call(
        move |py| Ok((py.eval(code, None, None)?, PyTuple::empty(py))),
        |_, answered| answered.map(pyo3::Bound::unbind),
    )
    .await
}

#[test]
fn a_blocked_call_does_not_delay_another_on_the_same_worker() {
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.entered = threading.Event(); builtins.leave = threading.Event()",
            None,
            None,
        )
        .unwrap();
        py.detach(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let first = runtime.spawn(called(
                c"lambda: (__import__('builtins').entered.set(), __import__('builtins').leave.wait(30))[1]",
            ));
            let entered = Python::attach(|py| {
                py.eval(c"__import__('builtins').entered.wait(30)", None, None)
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            });
            assert!(entered, "the first call started");
            let quick = runtime.spawn(called(c"lambda: 7"));
            let answered = runtime
                .block_on(async { tokio::time::timeout(Duration::from_secs(20), quick).await })
                .expect("the second call was not held up by the first")
                .unwrap()
                .unwrap()
                .unwrap();
            Python::attach(|py| {
                assert_eq!(answered.extract::<i64>(py).unwrap(), 7);
                py.eval(c"__import__('builtins').leave.set()", None, None)
                    .unwrap();
            });
            runtime
                .block_on(async { tokio::time::timeout(Duration::from_secs(30), first).await })
                .expect("the first call ended")
                .unwrap()
                .unwrap()
                .unwrap();
        });
    });
}

use std::time::Duration;

use pyo3::Python;
use pyo3::types::{PyAnyMethods as _, PyTuple};

use super::{Unanswered, call};
use crate::{embedded, runtime};

/// A call of `code`, a callable expression, with no arguments, as `finish` reads its answer.
fn calling<T: Send + 'static>(
    py: Python<'_>,
    code: &'static std::ffi::CStr,
    finish: impl for<'py> FnOnce(Python<'py>, Result<pyo3::Bound<'py, pyo3::PyAny>, pyo3::PyErr>) -> T
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
        let function = py
            .eval(c"lambda: __import__('builtins').finished.set()", None, None)
            .unwrap();
        let arguments = PyTuple::empty(py);
        let received = super::start(py, &function, &arguments).unwrap();
        drop(received);
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

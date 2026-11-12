#![expect(
    clippy::disallowed_methods,
    reason = "a test's own threads wait for answers attached, as an embedder's do"
)]

use std::time::Duration;

use pyo3::types::{
    IntoPyDict as _, PyAnyMethods as _, PyDictMethods as _, PyTuple, PyTypeMethods as _,
};
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

/// A call of `code`, a callable expression, with no arguments, its answer waited for here.
fn blocked(code: &'static std::ffi::CStr) -> Result<Result<Py<PyAny>, PyErr>, Unanswered> {
    super::blocking(
        move |py| Ok((py.eval(code, None, None)?, PyTuple::empty(py))),
        |_, answered| answered.map(pyo3::Bound::unbind),
    )
}

#[test]
fn a_blocking_call_waits_for_its_answer_without_holding_the_gil() {
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.entered = threading.Event(); builtins.leave = threading.Event()",
            None,
            None,
        )
        .unwrap();
        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| {
                blocked(c"lambda: (__import__('builtins').entered.set(), __import__('builtins').leave.wait(10), 7)[-1]")
            });
            // The answer needs this interpreter, so reaching it proves the wait released it.
            py.detach(|| {
                Python::attach(|py| {
                    let entered: bool = py
                        .eval(c"__import__('builtins').entered.wait(10)", None, None)
                        .unwrap()
                        .extract()
                        .unwrap();
                    assert!(entered, "the call started");
                    py.eval(c"__import__('builtins').leave.set()", None, None)
                        .unwrap();
                });
                let answered = waiting.join().unwrap().expect("an answer");
                Python::attach(|py| {
                    assert_eq!(answered.unwrap().extract::<i64>(py).unwrap(), 7);
                });
            });
        });
    });
}

#[test]
fn a_blocking_call_answers_what_was_raised() {
    embedded::with(|py| {
        py.detach(|| {
            let answered = blocked(c"lambda: 1 / 0").unwrap();
            Python::attach(|py| {
                assert!(
                    answered
                        .unwrap_err()
                        .is_instance_of::<pyo3::exceptions::PyZeroDivisionError>(py)
                );
            });
        });
    });
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
fn one_thread_s_calls_share_one_daemon_thread() {
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.local = threading.local()",
            None,
            None,
        )
        .unwrap();
        // Set on the first call's thread, a value seen only there: a thread's id is reused
        // once the thread has ended, so a fresh id proves nothing.
        let first = c"lambda: setattr(__import__('builtins').local, 'mark', 1)";
        let second = c"lambda: getattr(__import__('builtins').local, 'mark', 0)";
        let answered = py.detach(|| {
            blocked(first).unwrap().unwrap();
            blocked(second).unwrap().unwrap()
        });
        assert_eq!(answered.extract::<i64>(py).unwrap(), 1);
    });
}

/// Marks the thread a call is made on, a mark seen only there, and answers its id: a thread's id
/// is reused once the thread has ended, so a fresh id proves nothing.
const MARKED: &std::ffi::CStr = c"lambda: (setattr(__import__('builtins').local, 'mark', 1), __import__('threading').get_ident())[1]";

/// Answers the mark of the thread a call is made on, 0 where it has none.
const MARK: &std::ffi::CStr = c"lambda: getattr(__import__('builtins').local, 'mark', 0)";

#[test]
fn a_daemon_thread_outlives_its_thread_of_rust_and_makes_another_s_call() {
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.local = threading.local()",
            None,
            None,
        )
        .unwrap();
        let first = py.detach(|| {
            std::thread::spawn(|| blocked(MARKED).unwrap().unwrap())
                .join()
                .unwrap()
        });
        let watched = c"
import sys, time
deadline = time.monotonic() + 1
while ident in sys._current_frames() and time.monotonic() < deadline:
    time.sleep(0.01)
alive = ident in sys._current_frames()
";
        let locals = [("ident", first)].into_py_dict(py).unwrap();
        py.run(watched, None, Some(&locals)).unwrap();
        let alive: bool = locals
            .get_item("alive")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert!(
            alive,
            "the daemon thread lived on for a second once its thread of Rust had ended"
        );
        let marked = py.detach(|| {
            std::thread::spawn(|| blocked(MARK).unwrap().unwrap())
                .join()
                .unwrap()
        });
        assert_eq!(
            marked.extract::<i64>(py).unwrap(),
            1,
            "a later thread's call is made on the daemon thread the first one's was"
        );
    });
}

#[test]
fn an_async_call_s_daemon_thread_makes_the_next_async_call() {
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.local = threading.local()",
            None,
            None,
        )
        .unwrap();
        calling(py, MARKED, |_, returned| {
            returned.unwrap();
        })
        .unwrap();
        let marked = calling(py, MARK, |_, returned| {
            returned.and_then(|value| value.extract::<i64>()).unwrap()
        })
        .unwrap();
        assert_eq!(
            marked, 1,
            "the second call is made on the daemon thread the first one's was"
        );
    });
}

#[test]
fn a_forked_child_s_first_call_is_answered() {
    embedded::with(|py| {
        // The parent's daemon threads are not in the child, which starts its own.
        let first = py.detach(|| blocked(c"lambda: 1"));
        assert!(matches!(first, Ok(Ok(_))), "the parent's call is answered");
        py.run(
            c"import warnings; warnings.simplefilter('ignore', DeprecationWarning)",
            None,
            None,
        )
        .unwrap();
        let os = py.import("os").unwrap();
        let child: i64 = os.call_method0("fork").unwrap().extract().unwrap();
        if child == 0 {
            let answered = runtime::get(py).is_ok()
                && py.detach(|| blocked(c"lambda: 7")).is_ok_and(|answered| {
                    answered.is_ok_and(|value| value.extract::<i64>(py).is_ok_and(|n| n == 7))
                });
            os.call_method1("_exit", (if answered { 0 } else { 3 },))
                .unwrap();
        }
        let waited = c"
import os, signal, time
deadline = time.monotonic() + 30
while not (ended := os.waitpid(child, os.WNOHANG))[0] and time.monotonic() < deadline:
    time.sleep(0.05)
if not ended[0]:
    os.kill(child, signal.SIGKILL)
    os.waitpid(child, 0)
status = os.waitstatus_to_exitcode(ended[1]) if ended[0] else None
";
        let locals = [("child", child)].into_py_dict(py).unwrap();
        py.run(waited, None, Some(&locals)).unwrap();
        let status: Option<i64> = locals
            .get_item("status")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(status, Some(0), "the child's call is answered within 30 s");
    });
}

#[test]
fn a_call_whose_arguments_cannot_be_built_is_answered_with_why() {
    embedded::with(|py| {
        py.detach(|| {
            let answered: Result<Py<PyAny>, PyErr> = super::blocking(
                |_| Err(pyo3::exceptions::PyValueError::new_err("no arguments")),
                |_, answered| answered.map(pyo3::Bound::unbind),
            )
            .unwrap();
            Python::attach(|py| {
                assert!(
                    answered
                        .unwrap_err()
                        .is_instance_of::<pyo3::exceptions::PyValueError>(py)
                );
            });
        });
    });
}

#[test]
fn a_daemon_thread_whose_answer_panicked_waits_for_good_and_another_makes_the_next_call() {
    embedded::with(|py| {
        let parked = py.detach(|| {
            // A panic while the answer is read is a bug of rdlt's own: the daemon thread that
            // read it leaves the pool.
            let (ident, read) = std::sync::mpsc::channel();
            let lost = super::blocking(
                |py| {
                    let ident =
                        py.eval(c"lambda: __import__('threading').get_ident()", None, None)?;
                    Ok((ident, PyTuple::empty(py)))
                },
                move |_, answered| -> () {
                    ident
                        .send(answered.unwrap().extract::<u64>().unwrap())
                        .unwrap();
                    panic!("a bug in the reader of an answer")
                },
            );
            assert!(matches!(lost, Err(Unanswered::Thread)));
            let answered = blocked(c"lambda: 7").expect("another daemon thread answers");
            Python::attach(|py| {
                assert_eq!(answered.unwrap().extract::<i64>(py).unwrap(), 7);
            });
            read.recv().unwrap()
        });
        let watched = c"
import sys, time
deadline = time.monotonic() + 10
while (frame := sys._current_frames().get(ident)) is not None and frame.f_code.co_name != '_parked' and time.monotonic() < deadline:
    time.sleep(0.01)
waits = frame is not None and frame.f_code.co_name == '_parked'
";
        let locals = [("ident", parked)].into_py_dict(py).unwrap();
        py.run(watched, None, Some(&locals)).unwrap();
        let waits: bool = locals
            .get_item("waits")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert!(
            waits,
            "the daemon thread whose answer panicked waits for good, never ending"
        );
    });
}

#[test]
fn a_function_that_raises_to_end_its_thread_or_process_is_answered_and_its_thread_called_again() {
    let raising = [
        (c"lambda: (setattr(__import__('builtins').local, 'mark', 1), __import__('sys').exit(3))", "SystemExit"),
        (c"lambda: (setattr(__import__('builtins').local, 'mark', 1), __import__('_thread').exit())", "SystemExit"),
        (c"lambda: (setattr(__import__('builtins').local, 'mark', 1), (_ for _ in ()).throw(KeyboardInterrupt))", "KeyboardInterrupt"),
        (c"lambda: (setattr(__import__('builtins').local, 'mark', 1), (_ for _ in ()).throw(GeneratorExit))", "GeneratorExit"),
    ];
    embedded::with(|py| {
        for (code, raised) in raising {
            py.run(
                c"import builtins, threading; builtins.local = threading.local()",
                None,
                None,
            )
            .unwrap();
            let (answered, marked) = py.detach(|| {
                let answered = blocked(code).expect("the call is answered");
                let marked = blocked(MARK).unwrap().unwrap();
                (answered, marked)
            });
            let error = answered.expect_err("it raised");
            assert_eq!(
                error.get_type(py).name().unwrap().to_string(),
                raised,
                "{code:?} answers what it raised"
            );
            assert_eq!(
                marked.extract::<i64>(py).unwrap(),
                1,
                "{code:?}: the next call is made on the daemon thread that raised"
            );
        }
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

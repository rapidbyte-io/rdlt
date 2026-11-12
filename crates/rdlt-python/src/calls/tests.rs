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

/// A one-shot a test waits on within a bound, through `parking_lot`, as a forked child can.
pub(crate) struct Within<T>(std::sync::Arc<(parking_lot::Mutex<Option<T>>, parking_lot::Condvar)>);

impl<T> Clone for Within<T> {
    fn clone(&self) -> Self {
        Self(std::sync::Arc::clone(&self.0))
    }
}

impl<T> Within<T> {
    pub(crate) fn new() -> Self {
        Self(std::sync::Arc::default())
    }

    pub(crate) fn give(&self, value: T) {
        *self.0.0.lock() = Some(value);
        self.0.1.notify_all();
    }

    /// What was given, within `bound`; none where nothing was.
    pub(crate) fn wait(&self, bound: Duration) -> Option<T> {
        let deadline = std::time::Instant::now() + bound;
        let mut given = self.0.0.lock();
        while given.is_none() && !self.0.1.wait_until(&mut given, deadline).timed_out() {}
        given.take()
    }
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
                embedded::with(|py| {
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
                embedded::with(|py| {
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
            embedded::with(|py| {
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

#[test]
fn each_call_starts_in_a_context_of_its_own() {
    embedded::with(|py| {
        py.run(
            c"import builtins, contextvars, threading\nbuiltins.local = threading.local()\nbuiltins.held = contextvars.ContextVar('held', default=0)",
            None,
            None,
        )
        .unwrap();
        // The first call marks its thread and sets a variable it does not reset; the second, on
        // that thread, finds the mark and the variable's default.
        let first = c"lambda: (setattr(__import__('builtins').local, 'mark', 1), __import__('builtins').held.set(1))";
        let second = c"lambda: (getattr(__import__('builtins').local, 'mark', 0), __import__('builtins').held.get())";
        let answered = py.detach(|| {
            blocked(first).unwrap().unwrap();
            blocked(second).unwrap().unwrap()
        });
        assert_eq!(answered.extract::<(i64, i64)>(py).unwrap(), (1, 0));
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

/// Whether the forked child this runs in has two calls at once answered, the second taking a
/// daemon thread beneath the first's, where a copy of the parent's would lie.
fn a_forked_child_s_calls_answered(py: Python<'_>) -> bool {
    // Each step said before it is taken, so a child that dies says where.
    let step = |step: &str| eprintln!("forked child: {step}");
    step("forked; importing threading");
    py.run(
        c"import builtins, threading; builtins.both = threading.Event()",
        None,
        None,
    )
    .unwrap();
    step("getting the runtime");
    let answered = runtime::get(py).is_ok()
        && py.detach(|| {
            step("starting the first call's thread");
            let first =
                std::thread::spawn(|| blocked(c"lambda: __import__('builtins').both.wait(10)"));
            step("making the second call");
            let second = blocked(c"lambda: __import__('builtins').both.set()");
            step("joining the first call");
            matches!(second, Ok(Ok(_)))
                && first.join().is_ok_and(|first| {
                    first.is_ok_and(|waited| {
                        waited.is_ok_and(|waited| {
                            embedded::with(|py| waited.extract::<bool>(py).unwrap_or(false))
                        })
                    })
                })
        });
    step(if answered { "answered" } else { "unanswered" });
    answered
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
            let answered = a_forked_child_s_calls_answered(py);
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
            embedded::with(|py| {
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
            let ident = Within::new();
            let read = ident.clone();
            let lost = super::blocking(
                |py| {
                    let ident =
                        py.eval(c"lambda: __import__('threading').get_ident()", None, None)?;
                    Ok((ident, PyTuple::empty(py)))
                },
                move |_, answered| -> () {
                    ident.give(answered.unwrap().extract::<u64>().unwrap());
                    panic!("a bug in the reader of an answer")
                },
            );
            assert!(matches!(lost, Err(Unanswered::Dropped)));
            let answered = blocked(c"lambda: 7").expect("another daemon thread answers");
            embedded::with(|py| {
                assert_eq!(answered.unwrap().extract::<i64>(py).unwrap(), 7);
            });
            read.wait(Duration::from_secs(10))
                .expect("the answer was read")
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
            embedded::with(|py| {
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
            let entered = embedded::with(|py| {
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
            embedded::with(|py| {
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

#[test]
fn a_closed_daemon_thread_leaves_the_pool_with_the_spares_queued_on_it_and_the_rest_stay() {
    let (kept, _kept_waits) = tokio::sync::mpsc::unbounded_channel();
    let (doomed, mut doomed_waits) = tokio::sync::mpsc::unbounded_channel();
    let (carried, carried_waits) = tokio::sync::mpsc::unbounded_channel();
    super::put_back(kept.clone());
    super::put_back(carried.clone());
    super::put_back(doomed.clone());
    // Queued on the doomed spare, a call that carries a spare of its own, whose sender is on the
    // pool.
    let call = super::Call {
        prepare: Box::new(|_| Err(pyo3::exceptions::PyValueError::new_err("unmade"))),
        daemon: doomed.clone(),
        spare: Some(carried_waits),
        give: Box::new(|_, _| Box::new(|| ())),
    };
    assert!(doomed.send(call).is_ok());
    let queued = super::closed(
        &mut doomed_waits,
        "RuntimeError: no threads left".to_owned(),
    );
    assert_eq!(queued.len(), 1, "the call queued is given back");
    let idle = super::IDLE.lock();
    assert_eq!(
        idle.daemons.len(),
        1,
        "the doomed spare and the one it carried left"
    );
    assert!(idle.daemons[0].same_channel(&kept));
    assert!(doomed.is_closed() && carried.is_closed());
    assert_eq!(
        idle.unstarted.as_deref(),
        Some("RuntimeError: no threads left")
    );
}

#[test]
fn closing_a_daemon_thread_off_the_pool_keeps_why_the_pool_last_lost_one() {
    let (doomed, mut doomed_waits) = tokio::sync::mpsc::unbounded_channel::<super::Call>();
    super::put_back(doomed.clone());
    drop(super::closed(
        &mut doomed_waits,
        "RuntimeError: no threads left".to_owned(),
    ));
    // A thread parked off the pool, whose sender went with the call it held.
    let (_parked, mut parked_waits) = tokio::sync::mpsc::unbounded_channel::<super::Call>();
    drop(super::closed(
        &mut parked_waits,
        super::ABANDONED.to_owned(),
    ));
    assert_eq!(
        super::IDLE.lock().unstarted.as_deref(),
        Some("RuntimeError: no threads left"),
        "a closing that took no sender off the pool says nothing of why it is empty"
    );
}

/// Runs its code as it is dropped, putting back what a test replaced of a module's, whether the
/// test passed or failed.
pub(crate) struct Unpatch(pub(crate) &'static std::ffi::CStr);

impl Drop for Unpatch {
    fn drop(&mut self) {
        let put_back = embedded::with(|py| py.run(self.0, None, None));
        // A test that failed is unwinding already, with its own panic saying why.
        if !std::thread::panicking() {
            put_back.expect("what the test replaced is put back");
        }
    }
}

/// Gives daemon threads' starts back, as `builtins.started_by` kept them, and lets go the call
/// that waits for `builtins.leave`.
pub(crate) const GIVEN_BACK: &std::ffi::CStr = c"import _thread, builtins\n_thread.start_new_thread = builtins.started_by\nbuiltins.leave.set()";

/// Refuses every daemon thread's start once `builtins.queued` is set, as a process out of
/// threads does, and keeps the start for [`GIVEN_BACK`].
const REFUSAL: &std::ffi::CStr = c"
import _thread, builtins, threading
builtins.starting = threading.Event()
builtins.queued = threading.Event()
builtins.leave = threading.Event()
builtins.started_by = _thread.start_new_thread
def refused(*_):
    builtins.starting.set()
    builtins.queued.wait(10)
    raise RuntimeError('no threads left')
_thread.start_new_thread = refused
";

/// The only daemon thread, held by a call, starting the spare it was asked for, whose start is
/// refused once a call is queued on it.
pub(crate) struct Doomed {
    held: std::thread::JoinHandle<Result<Result<Py<PyAny>, PyErr>, Unanswered>>,
    spare: super::Daemon,
    given_back: Unpatch,
}

impl Doomed {
    /// Holds the only daemon thread once it has begun to start its spare.
    pub(crate) fn holding() -> Self {
        embedded::with(|py| py.run(REFUSAL, None, None)).unwrap();
        let given_back = Unpatch(GIVEN_BACK);
        let held = std::thread::spawn(|| blocked(c"lambda: __import__('builtins').leave.wait(30)"));
        let starting: bool = embedded::with(|py| {
            py.eval(c"__import__('builtins').starting.wait(10)", None, None)
                .unwrap()
                .extract()
                .unwrap()
        });
        assert!(starting, "the only daemon thread starts its spare");
        let spare = super::IDLE.lock().daemons.last().cloned();
        let spare = spare.expect("the spare waits on the pool");
        Self {
            held,
            spare,
            given_back,
        }
    }

    /// Refuses the spare's start once a call has taken it off the pool, and waits for the refusal
    /// to empty the pool, within 10 s each.
    pub(crate) fn refused_once_queued(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while super::IDLE
            .lock()
            .daemons
            .last()
            .is_some_and(|top| top.same_channel(&self.spare))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "a call took the spare"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        embedded::with(|py| py.run(c"__import__('builtins').queued.set()", None, None)).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !super::IDLE.lock().daemons.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "the spare's refused start emptied the pool"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Gives starts back, lets the held daemon thread go, and waits for its call's answer.
    pub(crate) fn released(self) {
        let Self {
            held, given_back, ..
        } = self;
        drop(given_back);
        held.join().unwrap().unwrap().unwrap();
    }
}

#[test]
fn a_call_queued_on_a_spare_that_did_not_start_is_let_go_and_the_next_start_is_tried_afresh() {
    let doomed = Doomed::holding();
    // Queued on the spare as it starts, a call goes with it, and so does the spare it asked for.
    let queued = std::thread::spawn(|| blocked(c"lambda: 7"));
    doomed.refused_once_queued();
    assert!(matches!(queued.join().unwrap(), Err(Unanswered::Dropped)));
    doomed.released();
    // Two calls at once: no spare that is gone lies beneath the daemon thread let go, so the
    // first's take of it starts a spare afresh, which the second takes.
    embedded::with(|py| {
        py.run(
            c"import builtins, threading; builtins.both = threading.Event()",
            None,
            None,
        )
    })
    .unwrap();
    let first = std::thread::spawn(|| blocked(c"lambda: __import__('builtins').both.wait(10)"));
    let second = blocked(c"lambda: __import__('builtins').both.set()");
    assert!(matches!(second, Ok(Ok(_))), "the second call is answered");
    let first = first.join().unwrap().unwrap().unwrap();
    embedded::with(|py| {
        assert!(
            first.extract::<bool>(py).unwrap(),
            "the first saw the second"
        );
    });
}

#[test]
fn a_daemon_thread_a_bug_parks_before_its_first_call_is_handed_none() {
    // The first daemon thread named, a spare whose loop meets a bug before it first waits.
    embedded::with(|py| {
        py.run(
            c"
import builtins, threading, time, types, rdlt._reading as reading
deadline = time.monotonic() + 10
while not any(t.name == 'rdlt-call' for t in threading.enumerate()) and time.monotonic() < deadline:
    time.sleep(0.001)
builtins.leave = threading.Event()
def current_thread():
    raise RuntimeError('a bug')
reading.threading = types.SimpleNamespace(current_thread=current_thread)
",
            None,
            None,
        )
    })
    .unwrap();
    let threading_back =
        Unpatch(c"import threading, rdlt._reading as reading\nreading.threading = threading");
    let held = std::thread::spawn(|| blocked(c"lambda: __import__('builtins').leave.wait(30)"));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !super::IDLE.lock().daemons.is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "the parked spare left the pool"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let told = Within::new();
    let telling = told.clone();
    std::thread::spawn(move || telling.give(blocked(c"lambda: 7")));
    let met = told
        .wait(Duration::from_secs(10))
        .expect("a call is told within 10 s, not queued on the parked thread for good");
    assert!(
        matches!(met, Err(Unanswered::Started(_))),
        "no daemon thread is idle"
    );
    drop(threading_back);
    embedded::with(|py| py.run(c"__import__('builtins').leave.set()", None, None)).unwrap();
    held.join().unwrap().unwrap().unwrap();
    let later = blocked(c"lambda: 7").expect("the daemon thread let go answers");
    embedded::with(|py| assert_eq!(later.unwrap().extract::<i64>(py).unwrap(), 7));
}

#[test]
fn a_daemon_thread_a_bug_escapes_answers_the_call_it_held_before_it_waits_for_good() {
    // Escaping once the answer was read, the answer is handed over; escaping before, the call is
    // told it went unanswered.
    embedded::with(|py| {
        py.run(
            c"
import builtins, rdlt._reading as reading
made_by = reading._made
def escaping(*made):
    if builtins.escape != 'before':
        made_by(*made)
    if builtins.escape is not None:
        raise RuntimeError('a bug')
builtins.escape = None
reading._made = escaping
",
            None,
            None,
        )
        .unwrap();
    });
    // A daemon thread that found `escaping` before it was put back escapes no more.
    let _made_back = Unpatch(
        c"import builtins, rdlt._reading as reading\nreading._made = made_by\nbuiltins.escape = None",
    );
    // A daemon thread finds what makes its next call as it begins to wait: after this call, both
    // that wait find `escaping`.
    blocked(c"lambda: 7").unwrap().unwrap();
    for (escape, answered) in [(c"'after'", true), (c"'before'", false)] {
        embedded::with(|py| {
            let builtins = py.import("builtins").unwrap();
            builtins
                .setattr("escape", py.eval(escape, None, None).unwrap())
                .unwrap();
        });
        let telling = Within::new();
        let teller = telling.clone();
        std::thread::spawn(move || teller.give(blocked(c"lambda: 7")));
        let told = telling
            .wait(Duration::from_secs(10))
            .expect("the call is answered, or told it went unanswered, within 10 s");
        match told {
            Ok(value) if answered => {
                embedded::with(|py| assert_eq!(value.unwrap().extract::<i64>(py).unwrap(), 7));
            }
            Err(Unanswered::Dropped) if !answered => {}
            other => panic!("escaping answered {answered}: told {:?}", other.err()),
        }
    }
}

/// The globals of an await made on a loop since closed, whose answer `released` lets it have and
/// whose loop's `call_soon_threadsafe` marks the thread that calls it, and sets `handed`.
fn awaited_on_a_closed_loop(
    released: tokio::sync::oneshot::Receiver<()>,
) -> Py<pyo3::types::PyDict> {
    let released = parking_lot::Mutex::new(Some(released));
    embedded::with(|py| {
        let make = pyo3::types::PyCFunction::new_closure(py, None, None, move |arguments, _| {
            let released = released.lock().take().expect("made once");
            let answered = async move {
                released.await.ok();
                7_i64
            };
            crate::awaitable::awaitable(arguments.py(), answered, |py, n| {
                pyo3::IntoPyObjectExt::into_bound_py_any(n, py)
            })
            .map(pyo3::Bound::unbind)
        })
        .unwrap();
        let globals = pyo3::types::PyDict::new(py);
        globals.set_item("make", make).unwrap();
        py.run(
            c"
import asyncio, builtins, threading
builtins.local = threading.local()
loop = asyncio.new_event_loop()
async def begun():
    return make()
awaited = loop.run_until_complete(begun())
handed = threading.Event()
closed = loop.call_soon_threadsafe
def recorded(*arguments, **keywords):
    builtins.local.mark = 1
    handed.set()
    return closed(*arguments, **keywords)
loop.call_soon_threadsafe = recorded
loop.close()
",
            Some(&globals),
            None,
        )
        .unwrap();
        globals.unbind()
    })
}

#[test]
fn an_await_whose_loop_closed_first_leaves_its_daemon_thread_to_the_next_call() {
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let globals = awaited_on_a_closed_loop(released);
    release.send(()).unwrap();
    let handed: bool = embedded::with(|py| {
        py.eval(c"handed.wait(30)", Some(globals.bind(py)), None)
            .unwrap()
            .extract()
            .unwrap()
    });
    assert!(handed, "the answer was handed to the closed loop");
    // Back on the pool, the daemon thread that handed it over is the top: two wait, it and the
    // spare it started.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while super::IDLE.lock().daemons.len() < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "the daemon thread went back on the pool"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let marked = blocked(MARK).unwrap().unwrap();
    embedded::with(|py| {
        assert_eq!(
            marked.extract::<i64>(py).unwrap(),
            1,
            "the next call is made on the daemon thread that handed the answer over"
        );
        // A closed loop's refusal stays in `awaited`, which says nothing of it.
        py.run(
            c"import rdlt._reading as reading\nreading.awaited(loop, awaited, 7, None)",
            Some(globals.bind(py)),
            None,
        )
        .unwrap();
    });
}

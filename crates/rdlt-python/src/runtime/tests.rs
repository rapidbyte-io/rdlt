use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::types::PyTypeMethods as _;

use super::{Interrupted, RUNTIME, forked_from, get, wait, waited};
use crate::embedded;

#[test]
fn a_runtime_is_a_copy_only_in_another_process() {
    assert!(!forked_from(7, 7));
    assert!(forked_from(7, 8));
}

#[test]
fn a_process_forked_after_the_runtime_started_is_refused_it() {
    embedded::with(|py| {
        let runtime = get(py).expect("the runtime builds");
        // As a forked child finds it: built by a process other than this one.
        *RUNTIME.lock() = Some((runtime, std::process::id().wrapping_add(1)));
        let refused = get(py).expect_err("a child may not use its parent's runtime");
        assert_eq!(
            refused.get_type(py).name().unwrap().to_string(),
            "InternalError"
        );
    });
}

#[test]
fn every_caller_gets_the_same_runtime() {
    embedded::with(|py| {
        let first = get(py).expect("the runtime builds");
        let second = get(py).expect("the runtime is held");
        assert!(ptr::eq(first, second));
    });
}

#[test]
fn a_wait_answers_what_its_future_does() {
    embedded::with(|py| {
        let answered = wait(py, async { 42 }).expect("the future ends");
        assert_eq!(answered, 42);
    });
}

/// Sets its flag when dropped.
struct Dropped(Arc<AtomicBool>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn a_signal_drops_the_future_and_raises_its_exception() {
    embedded::with(|py| {
        let runtime = get(py).expect("the runtime builds");
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(Arc::clone(&dropped));
        let task = runtime.spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        let mut looks = 0;
        let signals = || {
            looks += 1;
            if looks == 2 {
                Err(PyKeyboardInterrupt::new_err(()))
            } else {
                Ok(())
            }
        };
        let raised = waited(
            py,
            runtime,
            task,
            Duration::from_millis(1),
            signals,
            Interrupted::Raise,
        )
        .unwrap_err();
        assert!(raised.is_instance_of::<PyKeyboardInterrupt>(py));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !dropped.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "the future was not dropped within 5 s"
            );
            thread::sleep(Duration::from_millis(5));
        }
    });
}

#[test]
fn a_signal_answered_otherwise_lets_the_wait_go_on() {
    embedded::with(|py| {
        let runtime = get(py).expect("the runtime builds");
        let (answer, answered) = tokio::sync::oneshot::channel::<u8>();
        let task = runtime.spawn(async move { answered.await.unwrap_or_default() });
        let mut answer = Some(answer);
        let mut looks = 0;
        let signals = || {
            looks += 1;
            if looks == 1 {
                Err(PyKeyboardInterrupt::new_err(()))
            } else {
                Ok(())
            }
        };
        let interrupted = |_raised| {
            // The first signal is answered by letting the future end, as a stop after commit does.
            if let Some(answer) = answer.take() {
                answer.send(7).ok();
            }
            Interrupted::Continue
        };
        let ended = waited(
            py,
            runtime,
            task,
            Duration::from_millis(1),
            signals,
            interrupted,
        );
        assert_eq!(ended.expect("the wait went on to the future's end"), 7);
    });
}

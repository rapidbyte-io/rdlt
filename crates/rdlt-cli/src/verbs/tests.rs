use std::sync::mpsc;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};

use super::on_runtime;
use crate::Exit;

/// A multi-threaded runtime, as the command line runs on.
fn runtime() -> Runtime {
    Builder::new_multi_thread().enable_all().build().unwrap()
}

/// What `work`, on a thread of its own, answers within a bound: a wait that outlives the bound
/// fails the test, and the thread it left waits with the test's process.
fn within_bound<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    let (answer, answered) = mpsc::channel();
    std::thread::spawn(move || answer.send(work()));
    answered
        .recv_timeout(Duration::from_secs(30))
        .expect("the work ended within its bound")
}

#[test]
fn a_blocking_read_that_hangs_does_not_hold_the_exit() {
    let exit = within_bound(|| {
        on_runtime(runtime(), None, |runtime| {
            runtime.spawn_blocking(|| {
                // A read of a stalled file: no end the runtime could wait for.
                loop {
                    std::thread::park();
                }
            });
            Exit::Success
        })
    });
    assert_eq!(exit, Exit::Success);
}

#[test]
fn a_panic_is_a_defect_in_rdlt_and_holds_the_exit_no_more_than_a_hung_read() {
    let exit = within_bound(|| {
        on_runtime(runtime(), None, |runtime| {
            runtime.spawn_blocking(|| {
                loop {
                    std::thread::park();
                }
            });
            panic!("a defect");
        })
    });
    assert_eq!(exit, Exit::Internal);
}

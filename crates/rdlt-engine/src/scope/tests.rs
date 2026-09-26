#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock directly"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{ScopeError, TaskScope, contained};

#[derive(Debug, PartialEq, Eq)]
enum TestError {
    Real(&'static str),
    Cancelled,
    Panicked(String),
}

impl ScopeError for TestError {
    fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    fn panicked(message: String) -> Self {
        Self::Panicked(message)
    }
}

fn scope() -> TaskScope<TestError> {
    TaskScope::new(&CancellationToken::new())
}

#[tokio::test(start_paused = true)]
async fn an_empty_scope_joins_immediately() {
    assert_eq!(scope().join().await, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn join_succeeds_when_every_task_succeeds() {
    let mut scope = scope();
    for seconds in [3, 1, 2] {
        scope.spawn(async move {
            sleep(Duration::from_secs(seconds)).await;
            Ok(())
        });
    }
    assert_eq!(scope.join().await, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn the_first_error_cancels_the_remaining_tasks() {
    let mut scope = scope();
    scope.spawn(async {
        sleep(Duration::from_secs(1)).await;
        Err(TestError::Real("failed"))
    });
    let token = scope.token().clone();
    let saw_cancel = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&saw_cancel);
    scope.spawn(async move {
        tokio::select! {
            biased;
            () = token.cancelled() => {
                flag.store(true, Ordering::SeqCst);
                Err(TestError::Cancelled)
            }
            () = sleep(Duration::from_hours(1)) => Ok(()),
        }
    });
    assert_eq!(scope.join().await, Err(TestError::Real("failed")));
    assert!(saw_cancel.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn a_real_error_wins_over_an_earlier_cancellation() {
    let mut scope = scope();
    scope.spawn(async { Err(TestError::Cancelled) });
    scope.spawn(async {
        sleep(Duration::from_secs(1)).await;
        Err(TestError::Real("late"))
    });
    assert_eq!(scope.join().await, Err(TestError::Real("late")));
}

#[tokio::test(start_paused = true)]
async fn the_first_real_error_wins_over_later_ones() {
    let mut scope = scope();
    scope.spawn(async {
        sleep(Duration::from_secs(1)).await;
        Err(TestError::Real("first"))
    });
    scope.spawn(async {
        sleep(Duration::from_secs(2)).await;
        Err(TestError::Real("second"))
    });
    assert_eq!(scope.join().await, Err(TestError::Real("first")));
}

#[tokio::test(start_paused = true)]
async fn a_panicking_task_becomes_an_error() {
    let mut scope = scope();
    scope.spawn(async { panic!("task exploded") });
    assert_eq!(
        scope.join().await,
        Err(TestError::Panicked("task exploded".to_owned()))
    );
}

#[tokio::test(start_paused = true)]
async fn dropping_the_join_future_cancels_and_aborts_every_task() {
    let mut scope = scope();
    let token = scope.token().clone();
    let (held, mut released) = mpsc::channel::<()>(1);
    scope.spawn(async move {
        let _held = held;
        std::future::pending::<()>().await;
        Ok(())
    });

    let timed_out = tokio::time::timeout(Duration::from_secs(5), scope.join()).await;

    assert!(timed_out.is_err());
    assert!(released.recv().await.is_none());
    assert!(token.is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_parent_cancels_the_scope() {
    let parent = CancellationToken::new();
    let scope = TaskScope::<TestError>::new(&parent);
    parent.cancel();
    assert!(scope.token().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn only_cancellations_report_the_first_cancellation() {
    let mut scope = scope();
    scope.spawn(async { Err(TestError::Cancelled) });
    scope.spawn(async {
        sleep(Duration::from_secs(1)).await;
        Err(TestError::Cancelled)
    });
    assert_eq!(scope.join().await, Err(TestError::Cancelled));
}

#[tokio::test(start_paused = true)]
async fn panic_messages_are_kept_whatever_the_payload() {
    type PanicCase = (fn(), &'static str);
    let cases: [PanicCase; 3] = [
        (|| panic!("static message"), "static message"),
        (|| panic!("formatted {}", 42), "formatted 42"),
        (|| std::panic::panic_any(42_u8), "task panicked"),
    ];
    for (panics, expected) in cases {
        let mut scope = scope();
        scope.spawn(async move {
            panics();
            Ok(())
        });
        assert_eq!(
            scope.join().await,
            Err(TestError::Panicked(expected.to_owned()))
        );
    }
}

#[tokio::test]
async fn a_contained_future_gives_its_output_after_waiting() {
    let output = contained(async {
        tokio::task::yield_now().await;
        7
    })
    .await;
    assert_eq!(output.ok(), Some(7));
}

#[tokio::test]
async fn a_contained_future_that_panics_after_waiting_gives_its_panic() {
    let output = contained(async {
        tokio::task::yield_now().await;
        panic!("boom");
    })
    .await;
    let panic: Result<(), _> = output;
    assert_eq!(
        panic.map_err(|panicked| panicked.to_string()),
        Err("boom".to_owned())
    );
}

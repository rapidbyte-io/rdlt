use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use super::{Ended, POINTS, at_every_point};

/// How long each of a test operation's waits takes.
const STEP: Duration = Duration::from_millis(1);

/// The steps a test operation has begun.
#[derive(Default)]
struct Steps(usize);

/// Begins three steps, waiting after each, and returns how many it began.
async fn three(steps: &mut Steps) -> usize {
    for _ in 0..3 {
        steps.0 += 1;
        tokio::time::sleep(STEP).await;
    }
    steps.0
}

#[test]
fn an_operation_is_run_to_its_end_then_cancelled_at_each_of_its_waits() {
    let seen = Mutex::new(Vec::new());
    let points = at_every_point(
        async || Steps::default(),
        async |steps: &mut Steps| three(steps).await,
        async |steps: &mut Steps, ended: Ended<usize>| seen.lock().push((ended, steps.0)),
    );
    assert_eq!(points, 3);
    assert_eq!(
        *seen.lock(),
        [
            (Ended::Returned(3), 3),
            (Ended::Cancelled { at: 1 }, 1),
            (Ended::Cancelled { at: 2 }, 2),
            (Ended::Cancelled { at: 3 }, 3),
        ]
    );
}

#[test]
fn a_check_uses_the_object_again_on_its_run_s_paused_clock() {
    at_every_point(
        async || Steps::default(),
        async |steps: &mut Steps| three(steps).await,
        async |steps: &mut Steps, _: Ended<usize>| {
            let begun = steps.0;
            let (paused, real) = (tokio::time::Instant::now(), std::time::Instant::now());
            tokio::time::sleep(Duration::from_mins(30)).await;
            assert!(paused.elapsed() >= Duration::from_mins(30));
            assert!(
                real.elapsed() < Duration::from_secs(60),
                "the clock is paused"
            );
            assert_eq!(three(steps).await, begun + 3);
        },
    );
}

#[test]
fn an_operation_waiting_as_often_as_the_harness_allows_is_cut_at_every_wait() {
    let points = at_every_point(
        async || (),
        async |(): &mut ()| {
            for _ in 0..POINTS {
                tokio::time::sleep(STEP).await;
            }
        },
        async |(): &mut (), _: Ended<()>| (),
    );
    assert_eq!(points, POINTS);
}

#[test]
fn each_run_s_tasks_end_with_its_runtime() {
    let live = Arc::new(());
    at_every_point(
        async || {
            let live = Arc::clone(&live);
            tokio::spawn(async move {
                std::future::pending::<()>().await;
                drop(live);
            });
            Steps::default()
        },
        async |steps: &mut Steps| three(steps).await,
        async |_: &mut Steps, _: Ended<usize>| (),
    );
    assert_eq!(
        Arc::strong_count(&live),
        1,
        "no run's task outlives its runtime"
    );
}

#[test]
#[should_panic(expected = "never waited")]
fn an_operation_that_never_waits_fails() {
    at_every_point(
        async || (),
        async |(): &mut ()| (),
        async |(): &mut (), _: Ended<()>| (),
    );
}

#[test]
#[should_panic(expected = "more than")]
fn an_operation_that_waits_more_than_the_harness_cuts_at_fails() {
    at_every_point(
        async || (),
        async |(): &mut ()| {
            for _ in 0..=POINTS {
                tokio::time::sleep(STEP).await;
            }
        },
        async |(): &mut (), _: Ended<()>| (),
    );
}

#[test]
#[should_panic(expected = "ran otherwise")]
fn an_operation_that_waits_otherwise_when_run_again_fails() {
    let runs = AtomicUsize::new(0);
    at_every_point(
        async || runs.fetch_add(1, Ordering::SeqCst),
        async |run: &mut usize| {
            // Only the first run waits.
            if *run == 0 {
                tokio::time::sleep(STEP).await;
            }
        },
        async |_: &mut usize, _: Ended<()>| (),
    );
}

#[test]
#[should_panic(expected = "cancelled at wait 2 of 3")]
fn a_failing_check_names_the_wait_it_checked() {
    at_every_point(
        async || Steps::default(),
        async |steps: &mut Steps| three(steps).await,
        async |_: &mut Steps, ended: Ended<usize>| assert_ne!(ended, Ended::Cancelled { at: 2 }),
    );
}

#[test]
#[should_panic(expected = "did not end")]
fn an_operation_that_never_ends_fails_at_its_run_s_bound() {
    at_every_point(
        async || (),
        async |(): &mut ()| std::future::pending::<()>().await,
        async |(): &mut (), _: Ended<()>| (),
    );
}

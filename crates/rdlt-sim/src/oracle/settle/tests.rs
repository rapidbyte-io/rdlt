use std::panic::{self, AssertUnwindSafe};
use std::time::Duration;

use super::{OUTLIVED, settle};
use crate::seed::{Seed, run};

/// How long `settle` took on the simulated clock, with a task that ends after `lives`.
fn settling_with_a_task_that_lives(seed: u64, lives: Duration) -> Duration {
    run(Seed::new(seed), move |_| async move {
        tokio::spawn(tokio::time::sleep(lives));
        let started = tokio::time::Instant::now();
        settle(Seed::new(seed)).await;
        started.elapsed()
    })
}

#[test]
fn settling_ends_once_the_last_task_has() {
    let waited = settling_with_a_task_that_lives(1, Duration::from_millis(100));
    assert!(
        waited >= Duration::from_millis(100) && waited <= Duration::from_millis(101),
        "{waited:?}"
    );
}

#[test]
fn settling_with_no_task_left_takes_no_time() {
    let waited = run(Seed::new(2), |_| async {
        let started = tokio::time::Instant::now();
        settle(Seed::new(2)).await;
        started.elapsed()
    });
    assert_eq!(waited, Duration::ZERO);
}

#[test]
fn a_task_ending_just_before_the_bound_has_not_outlived_its_run() {
    // Two steps short, so whichever of its timer and settling's fires first on a step, it has
    // ended before the bound.
    let waited =
        settling_with_a_task_that_lives(3, OUTLIVED.saturating_sub(Duration::from_millis(2)));
    assert!(waited < OUTLIVED, "{waited:?}");
}

#[test]
fn a_task_alive_at_the_bound_outlived_its_run_and_fails_naming_the_seed() {
    // It ends a millisecond past the bound, so a settle that never gave up would still return.
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        settling_with_a_task_that_lives(4, OUTLIVED + Duration::from_millis(1))
    }));
    let payload = outcome.expect_err("the task outlived its run");
    let message = payload
        .downcast_ref::<String>()
        .expect("a formatted panic message");
    assert!(
        message.contains("seed 4: 1 tasks outlived their runs"),
        "{message}"
    );
}

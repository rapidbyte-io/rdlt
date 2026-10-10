#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock directly"
)]

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use super::Due;
use crate::compute::{ComputePool, Inline};
use crate::env::{Env, InlineEnv, Sleep};

/// The inline env, counting the sleeps made on it.
#[derive(Default)]
struct Counting {
    sleeps: AtomicUsize,
}

impl Env for Counting {
    fn now(&self) -> SystemTime {
        InlineEnv.now()
    }

    fn instant(&self) -> Instant {
        InlineEnv.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        InlineEnv.sleep(duration)
    }

    fn random(&self) -> u64 {
        InlineEnv.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        &Inline
    }

    fn cores(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }
}

#[tokio::test(start_paused = true)]
async fn pushes_that_keep_arriving_neither_delay_nor_rearm_the_first_pushs_flush() {
    let env = Counting::default();
    let start = env.instant();
    let deadline = start + Duration::from_secs(1);
    let mut due = Due::default();
    // Three turns of the loop before the deadline, a push arriving at each, follow its deadline.
    for _ in 0..3 {
        due.follow(Some(deadline), &env);
        let waited = tokio::time::timeout(Duration::from_millis(300), due.passed()).await;
        assert!(waited.is_err(), "the deadline has not passed yet");
    }
    due.follow(Some(deadline), &env);
    tokio::time::timeout(Duration::from_millis(500), due.passed())
        .await
        .expect("the deadline passes");
    let passed = env.instant().duration_since(start);
    assert!(
        passed >= Duration::from_secs(1) && passed < Duration::from_millis(1_100),
        "{passed:?}"
    );
    assert_eq!(env.sleeps.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_moved_deadline_makes_one_sleep_and_none_is_due_without_one() {
    let env = Counting::default();
    let start = env.instant();
    let mut due = Due::default();
    due.follow(Some(start + Duration::from_secs(1)), &env);
    due.follow(Some(start + Duration::from_secs(2)), &env);
    assert_eq!(env.sleeps.load(Ordering::SeqCst), 2);
    due.follow(None, &env);
    let waited = tokio::time::timeout(Duration::from_secs(5), due.passed()).await;
    assert!(waited.is_err(), "nothing is due without a deadline");
    assert_eq!(env.sleeps.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn a_deadline_followed_again_once_passed_waits_anew() {
    let env = Counting::default();
    let deadline = env.instant() + Duration::from_secs(1);
    let mut due = Due::default();
    due.follow(Some(deadline), &env);
    tokio::time::timeout(Duration::from_secs(2), due.passed())
        .await
        .expect("the deadline passes");
    // A passed sleep is never polled again: the same deadline makes a new one, already due.
    due.follow(Some(deadline), &env);
    tokio::time::timeout(Duration::from_millis(1), due.passed())
        .await
        .expect("a deadline in the past is due at once");
    assert_eq!(env.sleeps.load(Ordering::SeqCst), 2);
}

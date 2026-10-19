//! What a budget tells its environment of each change of what it holds reserved.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use parking_lot::Mutex;

use super::{BUDGET, HOUR};
use crate::budget::MemoryBudget;
use crate::compute::ComputePool;
use crate::env::{Env, InlineEnv, Sleep};

/// An environment that keeps each change of a budget's reservations it is told.
#[derive(Default)]
struct Told {
    changes: Mutex<Vec<i64>>,
}

impl Env for Told {
    fn now(&self) -> SystemTime {
        InlineEnv.now()
    }

    fn instant(&self) -> Instant {
        InlineEnv.instant()
    }

    fn sleep(&self, duration: std::time::Duration) -> Sleep {
        InlineEnv.sleep(duration)
    }

    fn random(&self) -> u64 {
        InlineEnv.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        &crate::compute::Inline
    }

    fn cores(&self) -> NonZeroUsize {
        NonZeroUsize::MIN
    }

    fn charged(&self, change: i64) {
        self.changes.lock().push(change);
    }
}

#[test]
fn a_budget_tells_each_reservation_and_what_each_shrink_and_release_gives_back() {
    let told = Arc::new(Told::default());
    let budget = MemoryBudget::new(BUDGET, Arc::clone(&told) as Arc<dyn Env>, HOUR);
    let mut held = budget
        .try_acquire_working(100)
        .expect("the budget has room");
    held.shrink(40);
    let kept = budget.keep(10).expect("the reads' share has room");
    drop(held);
    drop(kept);
    assert_eq!(*told.changes.lock(), [100, -60, 10, -40, -10]);
}

#[test]
fn a_reservation_lent_and_closed_is_told_as_its_parts_go_back() {
    let told = Arc::new(Told::default());
    let budget = MemoryBudget::new(BUDGET, Arc::clone(&told) as Arc<dyn Env>, HOUR);
    let lent = budget
        .try_acquire_working(100)
        .expect("the budget has room")
        .lend();
    let part = lent.try_take(30).expect("the loan has room");
    lent.close();
    drop(part);
    drop(lent);
    assert_eq!(*told.changes.lock(), [100, -70, -30]);
}

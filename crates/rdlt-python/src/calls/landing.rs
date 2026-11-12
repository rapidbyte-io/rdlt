//! A one-shot that a caller waits on for a blocking call's answer, through `parking_lot`: std's
//! waits park on a dispatch semaphore on macOS, which a forked child cannot use where its thread
//! made one before the fork.

use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use super::Unanswered;

/// Gives the answer; dropped without giving one, it lands the call as dropped.
pub(super) struct Giving<T>(Option<Arc<Landing<T>>>);

/// Waits for the answer [`Giving`] gives.
pub(super) struct Expecting<T>(Arc<Landing<T>>);

impl<T> Giving<T> {
    pub(super) fn give(mut self, answer: T) {
        if let Some(landing) = self.0.take() {
            landing.land(Ok(answer));
        }
    }
}

impl<T> Drop for Giving<T> {
    fn drop(&mut self) {
        if let Some(landing) = self.0.take() {
            landing.land(Err(Unanswered::Dropped));
        }
    }
}

impl<T> Expecting<T> {
    /// The answer, once it lands: given, or the call dropped unanswered.
    pub(super) fn wait(self) -> Result<T, Unanswered> {
        let mut slot = self.0.answer.lock();
        loop {
            if let Some(answer) = slot.take() {
                return answer;
            }
            self.0.landed.wait(&mut slot);
        }
    }
}

/// A one-shot's two ends.
pub(super) fn landing<T>() -> (Giving<T>, Expecting<T>) {
    let landing = Arc::new(Landing {
        answer: Mutex::new(None),
        landed: Condvar::new(),
    });
    (Giving(Some(Arc::clone(&landing))), Expecting(landing))
}

/// Where a blocking call's answer lands.
struct Landing<T> {
    answer: Mutex<Option<Result<T, Unanswered>>>,
    landed: Condvar,
}

impl<T> Landing<T> {
    fn land(&self, answer: Result<T, Unanswered>) {
        let mut slot = self.answer.lock();
        if slot.is_none() {
            *slot = Some(answer);
            self.landed.notify_one();
        }
    }
}

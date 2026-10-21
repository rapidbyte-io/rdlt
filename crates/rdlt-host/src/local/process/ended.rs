//! A value set once, which tasks wait for in the order they began to wait.
//!
//! Tokio's `watch` channel spreads its waiters over queues it picks at random, so tasks woken by
//! one value would run in an order a simulated seed does not decide.

use std::sync::{Mutex, PoisonError};

use tokio::sync::Notify;

#[cfg(test)]
mod tests;

/// A value set once, and what waits for it.
#[derive(Debug, Default)]
pub struct Ended<T> {
    value: Mutex<Option<T>>,
    set: Notify,
}

impl<T: Copy> Ended<T> {
    /// Sets the value, unless it is set already, and wakes every waiter.
    pub fn set(&self, value: T) {
        self.value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(value);
        self.set.notify_waiters();
    }

    /// The value, if it is set.
    pub fn get(&self) -> Option<T> {
        *self.value.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The value, once it is set.
    pub async fn wait(&self) -> T {
        loop {
            let set = self.set.notified();
            tokio::pin!(set);
            set.as_mut().enable();
            if let Some(value) = self.get() {
                return value;
            }
            set.await;
        }
    }
}

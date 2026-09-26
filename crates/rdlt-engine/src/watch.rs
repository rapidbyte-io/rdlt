//! A value tasks wait on to change, as with tokio's `watch` channel, but waking its waiters in an
//! order that depends only on the order they began to wait.
//!
//! Tokio's channel spreads its waiters over several queues, picked by a generator the runtime
//! seeds at random, so tasks woken by one change would run in an order a simulated seed does not
//! decide.

#[cfg(test)]
mod tests;

use std::pin::pin;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

/// The channel's sender is gone, and the receiver has seen everything it sent.
#[derive(Debug, thiserror::Error)]
#[error("the sender is gone")]
pub(crate) struct Closed;

struct Shared<T> {
    state: Mutex<State<T>>,
    /// Wakes every waiting receiver, in the order they began to wait.
    changed: Notify,
}

struct State<T> {
    value: T,
    /// How many values the sender has sent.
    version: u64,
    closed: bool,
}

/// Sends values to every [`Receiver`] of its channel.
pub(crate) struct Sender<T> {
    shared: Arc<Shared<T>>,
}

/// Waits for the values its channel's [`Sender`] sends.
pub(crate) struct Receiver<T> {
    shared: Arc<Shared<T>>,
    /// The version of the newest value this receiver has seen.
    seen: u64,
}

/// A channel holding `value`: its sender, and a receiver that has seen `value`.
pub(crate) fn channel<T>(value: T) -> (Sender<T>, Receiver<T>) {
    let sender = Sender::new(value);
    let receiver = sender.subscribe();
    (sender, receiver)
}

impl<T> Sender<T> {
    /// A sender holding `value`, with no receiver yet.
    pub(crate) fn new(value: T) -> Self {
        let state = State {
            value,
            version: 0,
            closed: false,
        };
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(state),
                changed: Notify::new(),
            }),
        }
    }

    /// Replaces the value with `value`, waking every receiver waiting for a change; the value
    /// replaced.
    pub(crate) fn send_replace(&self, value: T) -> T {
        let replaced = {
            let mut state = self.shared.state.lock();
            state.version += 1;
            std::mem::replace(&mut state.value, value)
        };
        self.shared.changed.notify_waiters();
        replaced
    }

    /// A receiver that has seen the value held now.
    pub(crate) fn subscribe(&self) -> Receiver<T> {
        let seen = self.shared.state.lock().version;
        Receiver {
            shared: Arc::clone(&self.shared),
            seen,
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.state.lock().closed = true;
        self.shared.changed.notify_waiters();
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            seen: self.seen,
        }
    }
}

impl<T: Copy> Receiver<T> {
    /// The value held now, marked seen.
    pub(crate) fn borrow_and_update(&mut self) -> T {
        let state = self.shared.state.lock();
        self.seen = state.version;
        state.value
    }
}

impl<T> Receiver<T> {
    /// Waits for a value this receiver has not seen, and marks it seen.
    ///
    /// # Errors
    ///
    /// [`Closed`], once the sender is gone and this receiver has seen every value it sent.
    pub(crate) async fn changed(&mut self) -> Result<(), Closed> {
        self.wait(|state, seen| state.version != seen).await
    }

    /// Waits until `ready` holds for the value, and marks it seen.
    ///
    /// # Errors
    ///
    /// [`Closed`], once the sender is gone while `ready` does not hold.
    pub(crate) async fn wait_for(&mut self, ready: impl Fn(&T) -> bool) -> Result<(), Closed> {
        self.wait(|state, _| ready(&state.value)).await
    }

    async fn wait(&mut self, ready: impl Fn(&State<T>, u64) -> bool) -> Result<(), Closed> {
        loop {
            // Waiting starts before the state is read, so a change in between still wakes it.
            let mut notified = pin!(self.shared.changed.notified());
            notified.as_mut().enable();
            {
                let state = self.shared.state.lock();
                if ready(&state, self.seen) {
                    self.seen = state.version;
                    return Ok(());
                }
                if state.closed {
                    return Err(Closed);
                }
            }
            notified.await;
        }
    }
}

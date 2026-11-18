//! When a Python destination's session calls and table writer calls reach it: each in its turn,
//! taken where the Rust SDK makes the call, before it is handed to a daemon thread.
//!
//! The Rust SDK makes the call in the engine where the connector runs in process, and in the
//! service's handler where it is served. Calls made one after another come in that order, and
//! calls made at once in either.
//!
//! A session's calls come one at a time, and so do each table writer's: with the session's
//! where the destination runs one writer, beside them where it runs more. A call takes a shared
//! turn of its session and then its one-at-a-time turn; `commit` and `close` take the session's
//! whole turn. Each holds its turn until its Python call has returned, a call the engine stopped
//! waiting for included. Tokio's locks queue their waiters in order, a shared turn behind a whole
//! one waiting, and the shared turn is taken first, so a call waiting for its one-at-a-time turn
//! already holds back a `commit` or `close` made after it. A call the engine stopped waiting for
//! before its turn came is never made.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// The turns of a session's calls, or of one of its table writers' calls.
#[derive(Clone)]
pub(super) struct Turns {
    /// Shared by each call but `commit` and `close`, which take it whole.
    session: Arc<RwLock<()>>,
    /// Held by each call that comes one at a time with the others holding it.
    one_at_a_time: Arc<Mutex<()>>,
}

impl Turns {
    /// The turns of a new session's calls.
    pub(super) fn new() -> Self {
        Self {
            session: Arc::new(RwLock::new(())),
            one_at_a_time: Arc::new(Mutex::new(())),
        }
    }

    /// The turns of a table writer of this session: one at a time with each other alone where
    /// `at_once`, with the session's calls otherwise.
    pub(super) fn writer(&self, at_once: bool) -> Self {
        let one_at_a_time = if at_once {
            Arc::new(Mutex::new(()))
        } else {
            Arc::clone(&self.one_at_a_time)
        };
        Self {
            session: Arc::clone(&self.session),
            one_at_a_time,
        }
    }

    /// A call's turn, but a `commit`'s or a `close`'s, held until it is let go.
    pub(super) async fn call(&self) -> (OwnedRwLockReadGuard<()>, OwnedMutexGuard<()>) {
        let shared = Arc::clone(&self.session).read_owned().await;
        let alone = Arc::clone(&self.one_at_a_time).lock_owned().await;
        (shared, alone)
    }

    /// The turn of a `commit` or a `close`, held until it is let go.
    pub(super) async fn whole(&self) -> OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.session).write_owned().await
    }
}

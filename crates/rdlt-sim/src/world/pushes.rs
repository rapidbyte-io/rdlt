//! What a world's sources hold of the pushes they make before they send them on: accounted for on
//! the thread that holds them while they are held, and the largest push and cursor since an
//! attempt began.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::oracle::Kept;

/// The pushes a world's sources made and have not yet sent on.
#[derive(Debug, Default)]
pub(crate) struct Pushes {
    /// Bytes: the largest push made since the count restarted.
    push: AtomicUsize,
    /// Bytes: the largest checkpoint's cursor made since the count restarted.
    cursor: AtomicUsize,
}

/// Bytes: the largest events a world's sources made since the count restarted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Largest {
    /// The largest push.
    pub(crate) push: u64,
    /// The largest checkpoint's cursor, which a read carries as an event, as it does a push.
    pub(crate) cursor: u64,
}

impl Largest {
    /// Bytes: the largest event, a push or a checkpoint's cursor.
    pub(crate) fn event(self) -> u64 {
        self.push.max(self.cursor)
    }
}

impl Pushes {
    /// Counts the largest events afresh from now on.
    pub(crate) fn restart(&self) {
        self.push.store(0, Ordering::SeqCst);
        self.cursor.store(0, Ordering::SeqCst);
    }

    /// Notes a push of `bytes` made, accounted for on this thread until what this returns drops:
    /// once the push is sent on, or given up.
    pub(crate) fn hold(&self, bytes: usize) -> Kept {
        self.push.fetch_max(bytes, Ordering::SeqCst);
        Kept::held(bytes)
    }

    /// Notes a checkpoint's cursor of `bytes`.
    pub(crate) fn cursor(&self, bytes: usize) {
        self.cursor.fetch_max(bytes, Ordering::SeqCst);
    }

    /// The largest push and cursor made since the count restarted.
    pub(crate) fn largest(&self) -> Largest {
        let bytes = |largest: &AtomicUsize| {
            u64::try_from(largest.load(Ordering::SeqCst)).unwrap_or(u64::MAX)
        };
        Largest {
            push: bytes(&self.push),
            cursor: bytes(&self.cursor),
        }
    }
}

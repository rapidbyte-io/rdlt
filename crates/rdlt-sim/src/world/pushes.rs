//! What a world's sources hold of the pushes they make before they send them on: accounted for on
//! the thread that holds them while they are held, and the largest event since an attempt began.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::oracle::Kept;

/// The pushes a world's sources made and have not yet sent on.
#[derive(Debug, Default)]
pub(crate) struct Pushes {
    /// Bytes: the largest event made since the count restarted: a push, or a checkpoint's cursor.
    largest: AtomicUsize,
}

impl Pushes {
    /// Counts the largest event afresh from now on.
    pub(crate) fn restart(&self) {
        self.largest.store(0, Ordering::SeqCst);
    }

    /// Notes a push of `bytes` made, accounted for on this thread until what this returns drops:
    /// once the push is sent on, or given up.
    pub(crate) fn hold(&self, bytes: usize) -> Kept {
        self.largest.fetch_max(bytes, Ordering::SeqCst);
        Kept::held(bytes)
    }

    /// Notes a checkpoint's cursor of `bytes`, which a read carries as an event, as it does a push.
    pub(crate) fn cursor(&self, bytes: usize) {
        self.largest.fetch_max(bytes, Ordering::SeqCst);
    }

    /// Bytes: the largest event made since the count restarted: a push, or a checkpoint's cursor.
    pub(crate) fn largest(&self) -> u64 {
        u64::try_from(self.largest.load(Ordering::SeqCst)).unwrap_or(u64::MAX)
    }
}

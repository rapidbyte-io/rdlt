//! What a world's sources hold of the pushes they make before they send them on: accounted for on
//! the thread that holds them while they are held, and the largest push since an attempt began.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::oracle::Kept;

/// The pushes a world's sources made and have not yet sent on.
#[derive(Debug, Default)]
pub(crate) struct Pushes {
    /// Bytes: the largest push made since the count restarted.
    largest: AtomicUsize,
}

impl Pushes {
    /// Counts the largest push afresh from now on.
    pub(crate) fn restart(&self) {
        self.largest.store(0, Ordering::SeqCst);
    }

    /// Notes a push of `bytes` made, accounted for on this thread until what this returns drops:
    /// once the push is sent on, or given up.
    pub(crate) fn hold(&self, bytes: usize) -> Kept {
        self.largest.fetch_max(bytes, Ordering::SeqCst);
        Kept::held(bytes)
    }

    /// Bytes: the largest push made since the count restarted.
    pub(crate) fn largest(&self) -> u64 {
        u64::try_from(self.largest.load(Ordering::SeqCst)).unwrap_or(u64::MAX)
    }
}

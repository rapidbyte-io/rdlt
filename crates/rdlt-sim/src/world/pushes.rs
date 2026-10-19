//! What a world's sources hold of the pushes they make before they send them on: accounted for on
//! the thread that holds them while they are held, and the largest push since an attempt began.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::oracle::account;

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
    pub(crate) fn hold(&self, bytes: usize) -> Holding {
        self.largest.fetch_max(bytes, Ordering::SeqCst);
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        account(bytes);
        Holding { bytes }
    }

    /// Bytes: the largest push made since the count restarted.
    pub(crate) fn largest(&self) -> u64 {
        u64::try_from(self.largest.load(Ordering::SeqCst)).unwrap_or(u64::MAX)
    }
}

/// A push its source holds until it is sent on.
#[derive(Debug)]
#[must_use = "the push counts as held only while this lives"]
pub(crate) struct Holding {
    bytes: i64,
}

impl Drop for Holding {
    fn drop(&mut self) {
        account(-self.bytes);
    }
}

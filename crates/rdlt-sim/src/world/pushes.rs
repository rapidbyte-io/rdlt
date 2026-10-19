//! What a world's sources hold of the pushes they make before they send them on: the bytes held
//! at once at their most, and the largest push, since an attempt began.

use std::sync::atomic::{AtomicUsize, Ordering};

/// The bytes of the pushes a world's sources made and have not yet sent on.
#[derive(Debug, Default)]
pub(crate) struct Pushes {
    /// Bytes: what is held now.
    held: AtomicUsize,
    /// Bytes: the most held at once since the count restarted.
    most: AtomicUsize,
    /// Bytes: the largest push made since the count restarted.
    largest: AtomicUsize,
}

impl Pushes {
    /// Counts afresh from now on: the most held at once from what is held now, and the largest
    /// push from none.
    pub(crate) fn restart(&self) {
        self.most
            .store(self.held.load(Ordering::SeqCst), Ordering::SeqCst);
        self.largest.store(0, Ordering::SeqCst);
    }

    /// Notes a push of `bytes` made, held until what this returns drops: once the push is sent
    /// on, or given up.
    pub(crate) fn hold(&self, bytes: usize) -> Holding<'_> {
        let held = self
            .held
            .fetch_add(bytes, Ordering::SeqCst)
            .saturating_add(bytes);
        self.most.fetch_max(held, Ordering::SeqCst);
        self.largest.fetch_max(bytes, Ordering::SeqCst);
        Holding {
            pushes: self,
            bytes,
        }
    }

    /// Bytes: the most held at once since the count restarted.
    pub(crate) fn most(&self) -> u64 {
        u64::try_from(self.most.load(Ordering::SeqCst)).unwrap_or(u64::MAX)
    }

    /// Bytes: the largest push made since the count restarted.
    pub(crate) fn largest(&self) -> u64 {
        u64::try_from(self.largest.load(Ordering::SeqCst)).unwrap_or(u64::MAX)
    }
}

/// A push its source holds until it is sent on.
#[derive(Debug)]
#[must_use = "the push counts as held only while this lives"]
pub(crate) struct Holding<'a> {
    pushes: &'a Pushes,
    bytes: usize,
}

impl Drop for Holding<'_> {
    fn drop(&mut self) {
        self.pushes.held.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

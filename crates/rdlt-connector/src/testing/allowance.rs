//! What a clause may still take of what a connector sends it: bytes and rows, spent by everything
//! the clause holds together.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Bytes and rows a clause may still take, all its takers together; a spend that asks for more
/// than is left of either leaves nothing of it.
#[derive(Debug)]
pub struct Allowance {
    bytes: AtomicUsize,
    rows: AtomicUsize,
}

impl Allowance {
    /// An allowance of `bytes` and `rows`.
    pub fn new(bytes: usize, rows: usize) -> Self {
        Self {
            bytes: AtomicUsize::new(bytes),
            rows: AtomicUsize::new(rows),
        }
    }

    /// Takes `bytes` and `rows`; whether that much of both was left.
    pub fn spend(&self, bytes: usize, rows: usize) -> bool {
        let bytes = take(&self.bytes, bytes);
        let rows = take(&self.rows, rows);
        bytes && rows
    }

    /// The bytes and rows left.
    #[cfg(test)]
    pub(crate) fn left(&self) -> (usize, usize) {
        (
            self.bytes.load(Ordering::SeqCst),
            self.rows.load(Ordering::SeqCst),
        )
    }
}

/// Takes `spent` from what `left` holds; whether that much was left, none being left after.
fn take(left: &AtomicUsize, spent: usize) -> bool {
    let took = left.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
        Some(left.saturating_sub(spent))
    });
    took.is_ok_and(|left| left >= spent)
}

#[cfg(test)]
mod tests;

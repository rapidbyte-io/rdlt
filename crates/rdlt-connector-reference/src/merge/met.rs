//! Whether a row has met a truncate: the comparison the search for a row's truncates makes, and
//! the only one, which the merge's tests count.

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    /// How many comparisons this thread made.
    static COMPARED: Cell<u64> = const { Cell::new(0) };
}

/// Whether a truncate at `truncate` comes at or before the row at `row`, which it then leaves.
pub(super) fn before(truncate: &[u8], row: &[u8]) -> bool {
    #[cfg(test)]
    COMPARED.with(|compared| compared.set(compared.get() + 1));
    truncate <= row
}

/// How many comparisons `work` made on this thread.
#[cfg(test)]
pub(super) fn counted<T>(work: impl FnOnce() -> T) -> (T, u64) {
    let from = COMPARED.with(Cell::get);
    let done = work();
    (done, COMPARED.with(Cell::get) - from)
}

//! A panic in a library a query is parsed or planned by, contained: SQL is hostile input, and a
//! bug its text reaches refuses the text rather than ending the embedder's process.

use std::panic::{self, AssertUnwindSafe};

/// What `work` returns, or none where it panicked; whatever it was making is dropped.
pub(crate) fn contained<T>(work: impl FnOnce() -> T) -> Option<T> {
    panic::catch_unwind(AssertUnwindSafe(work)).ok()
}

/// A name that makes [`panics_at`] panic, standing in for a library's bug.
#[cfg(test)]
pub(crate) const PANICS: &str = "rdlt_sql_panics_here";

/// Panics where `name` holds [`PANICS`], as a library a query reaches might.
#[cfg(test)]
pub(crate) fn panics_at(name: &str) {
    assert!(!name.contains(PANICS), "a library's bug, standing in");
}

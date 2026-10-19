//! What the simulated connectors hold for the engine while it runs: their sessions, writers and
//! reads, each counted while it lives, so one a run leaves behind shows.

#[cfg(test)]
mod tests;

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The sessions, writers and reads of one world's connectors that live now.
#[derive(Debug, Default)]
pub(crate) struct Owners {
    pub(crate) sessions: Arc<AtomicUsize>,
    pub(crate) writers: Arc<AtomicUsize>,
    pub(crate) reads: Arc<AtomicUsize>,
}

impl Owners {
    /// How many of each live now.
    pub(crate) fn held(&self) -> Held {
        let count = |count: &AtomicUsize| count.load(Ordering::SeqCst);
        Held {
            sessions: count(&self.sessions),
            writers: count(&self.writers),
            reads: count(&self.reads),
        }
    }
}

/// How many sessions, writers and reads live at a moment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Held {
    pub(crate) sessions: usize,
    pub(crate) writers: usize,
    pub(crate) reads: usize,
}

impl Held {
    /// Whether nothing lives.
    pub(crate) fn is_empty(self) -> bool {
        self == Self::default()
    }
}

impl fmt::Display for Held {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} sessions, {} writers and {} reads",
            self.sessions, self.writers, self.reads
        )
    }
}

/// One in each of its counts while it lives.
#[derive(Debug)]
pub(crate) struct Live(Vec<Arc<AtomicUsize>>);

impl Live {
    /// One more in each of `counts`, until it is dropped.
    pub(crate) fn of(counts: &[&Arc<AtomicUsize>]) -> Self {
        for count in counts {
            count.fetch_add(1, Ordering::SeqCst);
        }
        Self(counts.iter().map(|count| Arc::clone(count)).collect())
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        for count in &self.0 {
            count.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

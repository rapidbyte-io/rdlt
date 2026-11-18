//! Which partitions of its followed streams the source names, while a streaming phase churns
//! them.

use std::collections::{BTreeMap, BTreeSet};

use parking_lot::Mutex;
use tokio::time::Instant;

/// The partitions each followed stream names, by the stream's index and theirs.
pub(crate) type Naming = BTreeMap<usize, BTreeSet<usize>>;

/// Which partitions the source names: every one, unless a streaming phase churns them.
#[derive(Debug, Default)]
pub(crate) struct Named(Mutex<Churned>);

/// What a churning source names, and since when.
#[derive(Debug, Default)]
struct Churned {
    naming: Option<Naming>,
    /// Since when it has named each partition its naming names, by the stream's index and its own.
    since: BTreeMap<(usize, usize), Instant>,
}

impl Named {
    /// Whether the source names partition `partition` of stream `stream`.
    pub(crate) fn names(&self, stream: usize, partition: usize) -> bool {
        self.0
            .lock()
            .naming
            .as_ref()
            .and_then(|naming| naming.get(&stream))
            .is_none_or(|named| named.contains(&partition))
    }

    /// What the source names, where a phase churns it.
    pub(crate) fn naming(&self) -> Option<Naming> {
        self.0.lock().naming.clone()
    }

    /// Since when the source has named each partition it names, where a phase churns it.
    pub(crate) fn since(&self) -> BTreeMap<(usize, usize), Instant> {
        self.0.lock().since.clone()
    }

    /// Names what `naming` says from now on, and every partition where it is `None`.
    pub(crate) fn name(&self, naming: Option<Naming>) {
        let now = Instant::now();
        let named: BTreeSet<(usize, usize)> = naming
            .iter()
            .flatten()
            .flat_map(|(stream, named)| named.iter().map(|partition| (*stream, *partition)))
            .collect();
        let mut churned = self.0.lock();
        churned.since.retain(|key, _| named.contains(key));
        for key in named {
            churned.since.entry(key).or_insert(now);
        }
        churned.naming = naming;
    }
}

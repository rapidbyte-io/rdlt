//! Which partitions of its followed streams the source names, while a streaming phase churns
//! them.

use std::collections::{BTreeMap, BTreeSet};

use parking_lot::Mutex;

/// The partitions each followed stream names, by the stream's index and theirs.
pub(crate) type Naming = BTreeMap<usize, BTreeSet<usize>>;

/// Which partitions the source names: every one, unless a streaming phase churns them.
#[derive(Debug, Default)]
pub(crate) struct Named(Mutex<Option<Naming>>);

impl Named {
    /// Whether the source names partition `partition` of stream `stream`.
    pub(crate) fn names(&self, stream: usize, partition: usize) -> bool {
        self.0
            .lock()
            .as_ref()
            .and_then(|naming| naming.get(&stream))
            .is_none_or(|named| named.contains(&partition))
    }

    /// What the source names, where a phase churns it.
    pub(crate) fn naming(&self) -> Option<Naming> {
        self.0.lock().clone()
    }

    /// Names what `naming` says from now on, and every partition where it is `None`.
    pub(crate) fn name(&self, naming: Option<Naming>) {
        *self.0.lock() = naming;
    }
}

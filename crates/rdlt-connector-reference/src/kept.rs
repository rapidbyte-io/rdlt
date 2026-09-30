//! Positions a source keeps outside the engine, by name, as a replication slot or a consumer
//! group keeps them beyond a connection: the position the engine last told it is committed, per
//! stream and partition, never moving back.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use rdlt_connector::PartitionId;

/// One keeper of positions: each partition's acknowledged position, by stream and partition.
#[derive(Debug)]
pub(crate) struct Kept<P>(Mutex<BTreeMap<(String, PartitionId), P>>);

impl<P> Default for Kept<P> {
    fn default() -> Self {
        Self(Mutex::default())
    }
}

impl<P: Copy + Ord> Kept<P> {
    /// Acknowledges `position` of `partition` of `stream`, unless the keeper stands past it.
    pub(crate) fn advance(&self, stream: &str, partition: &PartitionId, position: P) {
        let mut positions = self.0.lock();
        let standing = positions
            .entry((stream.to_owned(), partition.clone()))
            .or_insert(position);
        *standing = (*standing).max(position);
    }

    /// Where `partition` of `stream` stands.
    pub(crate) fn position(&self, stream: &str, partition: &PartitionId) -> Option<P> {
        self.0
            .lock()
            .get(&(stream.to_owned(), partition.clone()))
            .copied()
    }
}

/// Keepers by name, for as long as the process runs.
pub(crate) struct Registry<P>(LazyLock<Mutex<BTreeMap<String, Arc<Kept<P>>>>>);

impl<P> Registry<P> {
    pub(crate) const fn new() -> Self {
        Self(LazyLock::new(|| Mutex::new(BTreeMap::new())))
    }

    /// The keeper named `name`, shared by every source of this process that names it; the
    /// default keeper, which every source naming none shares, where `name` is none.
    pub(crate) fn named(&self, name: Option<&str>) -> Arc<Kept<P>> {
        let name = name.unwrap_or_default();
        Arc::clone(self.0.lock().entry(name.to_owned()).or_default())
    }
}

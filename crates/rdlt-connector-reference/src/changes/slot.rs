//! Where a change source stands outside the engine, as a replication slot keeps it: the position
//! the engine last told it is committed, per stream and partition, never moving back.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use rdlt_connector::PartitionId;

use super::Position;

/// Slots by name, for as long as the process runs.
static SLOTS: LazyLock<Mutex<BTreeMap<String, Arc<Slot>>>> = LazyLock::new(Mutex::default);

/// One slot: each partition's acknowledged position, by stream and partition.
#[derive(Debug, Default)]
pub(super) struct Slot(Mutex<BTreeMap<(String, PartitionId), Position>>);

impl Slot {
    /// The slot named `name`, shared by every source of this process that names it; the default
    /// slot, which every source naming none shares, where `name` is none.
    pub(super) fn named(name: Option<&str>) -> Arc<Self> {
        let name = name.unwrap_or_default();
        Arc::clone(SLOTS.lock().entry(name.to_owned()).or_default())
    }

    /// Acknowledges `position` of `partition` of `stream`, unless the slot stands past it.
    pub(super) fn advance(&self, stream: &str, partition: &PartitionId, position: Position) {
        let mut positions = self.0.lock();
        let standing = positions
            .entry((stream.to_owned(), partition.clone()))
            .or_insert(position);
        *standing = (*standing).max(position);
    }

    /// Where `partition` of `stream` stands.
    pub(super) fn position(&self, stream: &str, partition: &PartitionId) -> Option<Position> {
        self.0
            .lock()
            .get(&(stream.to_owned(), partition.clone()))
            .copied()
    }
}

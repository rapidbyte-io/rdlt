//! Making room in stored state for a commit that would pass its limit: the done markers of
//! partitions the attempt's plans no longer name go, the earliest recorded first.
//!
//! A done marker says a partition was read to its end, so a plan naming it reads nothing from it.
//! One forgotten is read again from its beginning should a plan name it again, which the report
//! says. A partition that is not done keeps its position whatever the pressure.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{CommitMeta, PartitionId, StateChange, StateKey, StreamName};

use super::Coordinator;
use crate::limits::STATE_BYTES_EXCEEDED;

impl Coordinator {
    /// Deletes with `meta` as few done markers of partitions the latest plans of the attempt's
    /// streams leave out as it takes for the state it leaves to fit, the earliest recorded first;
    /// those it deleted, by stream.
    ///
    /// Nothing goes where `meta` is admitted as it is, is refused for anything but its stored
    /// state, or would not fit with every such marker gone: it is then refused as it would be.
    /// `born` are the keys of the records of new child tables `meta` makes, with their streams.
    pub(super) fn relieve(
        &self,
        meta: &mut CommitMeta,
        born: &[(StreamName, String)],
    ) -> BTreeMap<StreamName, Vec<PartitionId>> {
        let stored = &self.parts.stored;
        match stored.admit(meta, born) {
            Err(error) if error.code() == Some(STATE_BYTES_EXCEEDED) => {}
            _ => return BTreeMap::new(),
        }
        let touched: BTreeSet<&str> = meta
            .state_delta
            .iter()
            .map(|change| match change {
                StateChange::Put(record) => record.key.as_str(),
                StateChange::Delete(key) => key.as_str(),
            })
            .collect();
        let named: BTreeMap<&StreamName, &BTreeSet<PartitionId>> = self
            .parts
            .streams
            .iter()
            .map(|stream| (&stream.name, &stream.named))
            .collect();
        let unplanned: Vec<(StreamName, PartitionId, String)> = self
            .parts
            .positions
            .done()
            .into_iter()
            .filter(|(_, stream, id)| named.get(stream).is_some_and(|named| !named.contains(*id)))
            .map(|(_, stream, id)| {
                let key = StateKey::Partition(stream.clone(), id.clone()).encode();
                (stream.clone(), id.clone(), key)
            })
            .filter(|(.., key)| !touched.contains(key.as_str()))
            .collect();
        let keys: Vec<&str> = unplanned.iter().map(|(.., key)| key.as_str()).collect();
        let Some(count) = stored.relief(meta, &keys) else {
            return BTreeMap::new();
        };
        let mut forgotten: BTreeMap<StreamName, Vec<PartitionId>> = BTreeMap::new();
        for (stream, id, key) in unplanned.into_iter().take(count) {
            meta.state_delta.push(StateChange::Delete(key));
            forgotten.entry(stream).or_default().push(id);
        }
        forgotten
    }

    /// Notes for the report that the commit that landed forgot the done markers `forgotten`.
    pub(super) fn forgot(&self, forgotten: BTreeMap<StreamName, Vec<PartitionId>>) {
        let mut log = self.parts.log.lock();
        for (stream, ids) in forgotten {
            log.forgotten.entry(stream).or_default().extend(ids);
        }
    }
}

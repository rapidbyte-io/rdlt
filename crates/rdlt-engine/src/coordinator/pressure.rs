//! Making room in stored state for a commit that would pass its limit: the done markers of
//! partitions the attempt's plans no longer name go, the earliest recorded first.
//!
//! A done marker says a partition was read to its end, so a plan naming it reads nothing from it.
//! One forgotten is read again from its beginning should a plan name it again, which the report
//! says. A partition that is not done keeps its position whatever the pressure.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{CommitMeta, PartitionId, StateChange, StateKey, StreamName};

use super::Coordinator;
use crate::error::{Error, codes};
use crate::stored::Measured;

impl Coordinator {
    /// Admits the commit `meta`, deleting with it as few done markers of partitions the latest
    /// plans of the attempt's streams leave out as it takes for the state it leaves to fit, the
    /// earliest recorded first: what it measured of the commit, and the markers it deleted, by
    /// stream.
    ///
    /// Nothing goes where `meta` is admitted as it is, or is refused for anything but its
    /// stored state. `born` are the keys of the records of new child tables `meta` makes, with
    /// their streams.
    ///
    /// # Errors
    ///
    /// `state_bytes_exceeded` where the commit's request, its deletes included, or the state it
    /// leaves would pass what a message carrying state may take, `child_tables_exceeded` where
    /// the state would fit but for the child tables it records first: nothing of the commit is
    /// logged, acknowledged or sent.
    pub(super) fn relieve(
        &self,
        meta: &mut CommitMeta,
        born: &[(StreamName, String)],
    ) -> Result<(Measured, BTreeMap<StreamName, Vec<PartitionId>>), Error> {
        let stored = &self.parts.stored;
        let measured = Measured::of(meta);
        let refused = match stored.admit(&measured, born) {
            Ok(()) => return Ok((measured, BTreeMap::new())),
            Err(refused) if refused.code() == Some(codes::STATE_BYTES_EXCEEDED) => refused,
            Err(refused) => return Err(refused),
        };
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
        // None fit, or the state fits and only the request is past its limit.
        let count = match stored.relief(&measured, &keys) {
            Some(count) if count > 0 => count,
            _ => return Err(refused),
        };
        let mut forgotten: BTreeMap<StreamName, Vec<PartitionId>> = BTreeMap::new();
        let mut deleted = Vec::with_capacity(count);
        for (stream, id, key) in unplanned.into_iter().take(count) {
            meta.state_delta.push(StateChange::Delete(key.clone()));
            deleted.push(key);
            forgotten.entry(stream).or_default().push(id);
        }
        let measured = measured.deleting(meta, deleted);
        stored.admit(&measured, born)?;
        Ok((measured, forgotten))
    }

    /// Notes for the report that the commit that landed forgot the done markers `forgotten`.
    pub(super) fn forgot(&self, forgotten: BTreeMap<StreamName, Vec<PartitionId>>) {
        let mut log = self.parts.log.lock();
        for (stream, ids) in forgotten {
            log.forgotten.entry(stream).or_default().note(ids);
        }
    }
}

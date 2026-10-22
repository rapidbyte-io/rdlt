//! Partitions' positions as a destination holds them, followed through the commits that change
//! them.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::{
    LoadId, PartitionId, PartitionState, PipelineState, StateChange, StateEntry, StateKey,
    StateRecord, StreamName,
};

/// Each partition's position, by stream and partition, with the load whose commit recorded it,
/// and each stream's phase; a partition without a position has none, and a stream without a
/// phase is at phase 0.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Positions {
    partitions: BTreeMap<(StreamName, PartitionId), PartitionState>,
    loads: BTreeMap<(StreamName, PartitionId), LoadId>,
    phases: BTreeMap<StreamName, u16>,
}

/// The states a load's own commit records its partitions at, by stream and partition, and the
/// load: what applying the commit takes as they are, rather than decoding its records.
#[derive(Debug)]
pub(crate) struct Recorded<'a> {
    pub(crate) load: LoadId,
    pub(crate) states: &'a BTreeMap<(StreamName, PartitionId), PartitionState>,
}

impl Positions {
    /// The phase `stream` is at.
    pub(crate) fn phase(&self, stream: &StreamName) -> u16 {
        self.phases.get(stream).copied().unwrap_or_default()
    }

    /// Where `partition` of `stream` stands.
    pub(crate) fn get(
        &self,
        stream: &StreamName,
        partition: &PartitionId,
    ) -> Option<&PartitionState> {
        self.partitions.get(&(stream.clone(), partition.clone()))
    }

    /// The partitions that are `Done`, each with its stream and the load whose commit recorded it
    /// so, the earliest first.
    pub(crate) fn done(&self) -> Vec<(LoadId, &StreamName, &PartitionId)> {
        let mut done: Vec<_> = self
            .partitions
            .iter()
            .filter(|(_, position)| **position == PartitionState::Done)
            .filter_map(|(key, _)| Some((*self.loads.get(key)?, &key.0, &key.1)))
            .collect();
        done.sort();
        done
    }

    /// Records that `partition` of `stream` stands at `position`, as a load's log follows it; the
    /// load that records it is noted once its commit applies.
    pub(crate) fn set(
        &mut self,
        stream: StreamName,
        partition: PartitionId,
        position: PartitionState,
    ) {
        self.partitions.insert((stream, partition), position);
    }

    /// The positions once `delta`, a commit's state changes in order, applies; its other records
    /// change nothing here, and are not read.
    pub(crate) fn apply(&mut self, delta: &[StateChange]) {
        self.applied(delta, None);
    }

    /// The positions once `delta`, the changes of a commit `recorded` was made from, applies: a
    /// partition `recorded` names stands at its state there, sharing its bytes.
    pub(crate) fn landed(&mut self, delta: &[StateChange], recorded: &Recorded<'_>) {
        self.applied(delta, Some(recorded));
    }

    fn applied(&mut self, delta: &[StateChange], recorded: Option<&Recorded<'_>>) {
        for change in delta {
            match change {
                StateChange::Put(record) => self.put(record, recorded),
                StateChange::Delete(key) => match StateKey::parse(key) {
                    Ok(StateKey::Partition(stream, partition)) => {
                        let key = (stream, partition);
                        self.loads.remove(&key);
                        self.partitions.remove(&key);
                    }
                    Ok(StateKey::Phase(stream)) => {
                        self.phases.remove(&stream);
                    }
                    _ => {}
                },
            }
        }
    }

    /// Applies the put of `record`, taking a partition's state from `recorded` where it names it.
    fn put(&mut self, record: &StateRecord, recorded: Option<&Recorded<'_>>) {
        match StateKey::parse(&record.key) {
            Ok(StateKey::Partition(stream, partition)) => {
                let key = (stream, partition);
                if let Some(recorded) = recorded
                    && let Some(state) = recorded.states.get(&key)
                {
                    self.loads.insert(key.clone(), recorded.load);
                    self.partitions.insert(key, state.clone());
                    return;
                }
            }
            Ok(StateKey::Phase(_)) => {}
            _ => return,
        }
        match StateEntry::from_record(record) {
            Ok(StateEntry::Partition {
                stream,
                partition,
                state,
                load,
            }) => {
                self.loads.insert((stream.clone(), partition.clone()), load);
                self.set(stream, partition, state);
            }
            Ok(StateEntry::Phase { stream, phase }) => {
                self.phases.insert(stream, phase);
            }
            _ => {}
        }
    }
}

impl From<PipelineState> for Positions {
    /// The positions and phases `state` records, taking its cursors as they are.
    fn from(state: PipelineState) -> Self {
        let mut partitions = BTreeMap::new();
        let mut phases = BTreeMap::new();
        for (stream, streamed) in state.streams {
            phases.insert(stream.clone(), streamed.phase);
            for (partition, position) in streamed.partitions {
                partitions.insert((stream.clone(), partition), position);
            }
        }
        Self {
            partitions,
            loads: state.recorded_by,
            phases,
        }
    }
}

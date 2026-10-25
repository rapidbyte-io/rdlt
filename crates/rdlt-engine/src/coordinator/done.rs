//! The partitions a destination holds done, followed through the commits that change them: the
//! done markers state pressure may forget.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::{
    LoadId, PartitionId, PartitionState, PipelineState, StateChange, StateEntry, StateKey,
    StateRecord, StreamName,
};

/// Each partition the destination holds done, by stream and partition, with the load whose
/// commit recorded it so.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DoneMarkers {
    loads: BTreeMap<(StreamName, PartitionId), LoadId>,
}

/// The states a load's own commit records its partitions at, by stream and partition, and the
/// load: what landing the commit reads, rather than decoding its records.
#[derive(Debug)]
pub(crate) struct Recorded<'a> {
    pub(crate) load: LoadId,
    pub(crate) states: &'a BTreeMap<(StreamName, PartitionId), PartitionState>,
}

impl DoneMarkers {
    /// The partitions held done, each with its stream and the load whose commit recorded it so,
    /// the earliest first.
    pub(crate) fn listed(&self) -> Vec<(LoadId, &StreamName, &PartitionId)> {
        let mut done: Vec<_> = self
            .loads
            .iter()
            .map(|(key, load)| (*load, &key.0, &key.1))
            .collect();
        done.sort();
        done
    }

    /// The markers once `delta`, the changes of a commit `recorded` was made from, applies: a
    /// partition `recorded` names is done where its state there is; its other records change
    /// nothing here, and are not read.
    pub(crate) fn landed(&mut self, delta: &[StateChange], recorded: &Recorded<'_>) {
        for change in delta {
            match change {
                StateChange::Put(record) => self.put(record, recorded),
                StateChange::Delete(key) => {
                    if let Ok(StateKey::Partition(stream, partition)) = StateKey::parse(key) {
                        self.loads.remove(&(stream, partition));
                    }
                }
            }
        }
    }

    /// Applies the put of `record`, taking a partition's state from `recorded` where it names it.
    fn put(&mut self, record: &StateRecord, recorded: &Recorded<'_>) {
        let Ok(StateKey::Partition(stream, partition)) = StateKey::parse(&record.key) else {
            return;
        };
        let key = (stream, partition);
        let done = match recorded.states.get(&key) {
            Some(state) => (*state == PartitionState::Done).then_some(recorded.load),
            None => match StateEntry::from_record(record) {
                Ok(StateEntry::Partition { state, load, .. }) => {
                    (state == PartitionState::Done).then_some(load)
                }
                _ => return,
            },
        };
        match done {
            Some(load) => self.loads.insert(key, load),
            None => self.loads.remove(&key),
        };
    }
}

impl From<&PipelineState> for DoneMarkers {
    /// The partitions `state` records done, each by the load it says recorded it.
    fn from(state: &PipelineState) -> Self {
        let mut loads = BTreeMap::new();
        for (stream, streamed) in &state.streams {
            for (partition, position) in &streamed.partitions {
                let key = (stream.clone(), partition.clone());
                if *position == PartitionState::Done
                    && let Some(load) = state.recorded_by.get(&key)
                {
                    loads.insert(key, *load);
                }
            }
        }
        Self { loads }
    }
}

//! Partitions' positions as a destination holds them, followed through the commits that change
//! them.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::{
    PartitionId, PartitionState, PipelineState, StateChange, StateEntry, StateKey, StreamName,
};

/// Each partition's position, by stream and partition; a partition without one has none.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Positions(BTreeMap<(StreamName, PartitionId), PartitionState>);

impl Positions {
    /// The positions `state` records.
    pub(crate) fn of(state: &PipelineState) -> Self {
        let positions = state.streams.iter().flat_map(|(stream, streamed)| {
            streamed.partitions.iter().map(|(partition, position)| {
                ((stream.clone(), partition.clone()), position.clone())
            })
        });
        Self(positions.collect())
    }

    /// Where `partition` of `stream` stands.
    pub(crate) fn get(
        &self,
        stream: &StreamName,
        partition: &PartitionId,
    ) -> Option<&PartitionState> {
        self.0.get(&(stream.clone(), partition.clone()))
    }

    /// Records that `partition` of `stream` stands at `position`.
    pub(crate) fn set(
        &mut self,
        stream: StreamName,
        partition: PartitionId,
        position: PartitionState,
    ) {
        self.0.insert((stream, partition), position);
    }

    /// The positions once `delta`, a commit's state changes in order, applies; its other records
    /// change nothing here.
    pub(crate) fn apply(&mut self, delta: &[StateChange]) {
        for change in delta {
            match change {
                StateChange::Put(record) => {
                    if let Ok(StateEntry::Partition {
                        stream,
                        partition,
                        state,
                    }) = StateEntry::from_record(record)
                    {
                        self.set(stream, partition, state);
                    }
                }
                StateChange::Delete(key) => {
                    if let Ok(StateKey::Partition(stream, partition)) = StateKey::parse(key) {
                        self.0.remove(&(stream, partition));
                    }
                }
            }
        }
    }
}

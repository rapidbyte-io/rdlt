//! Partitions' positions as a destination holds them, followed through the commits that change
//! them.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::{
    PartitionId, PartitionState, PipelineState, StateChange, StateEntry, StateKey, StreamName,
};

/// Each partition's position, by stream and partition, and each stream's phase; a partition
/// without a position has none, and a stream without a phase is at phase 0.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Positions {
    partitions: BTreeMap<(StreamName, PartitionId), PartitionState>,
    phases: BTreeMap<StreamName, u16>,
}

impl Positions {
    /// The positions and phases `state` records.
    pub(crate) fn of(state: &PipelineState) -> Self {
        let partitions = state.streams.iter().flat_map(|(stream, streamed)| {
            streamed.partitions.iter().map(|(partition, position)| {
                ((stream.clone(), partition.clone()), position.clone())
            })
        });
        let phases = state
            .streams
            .iter()
            .map(|(stream, streamed)| (stream.clone(), streamed.phase));
        Self {
            partitions: partitions.collect(),
            phases: phases.collect(),
        }
    }

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

    /// Records that `partition` of `stream` stands at `position`.
    pub(crate) fn set(
        &mut self,
        stream: StreamName,
        partition: PartitionId,
        position: PartitionState,
    ) {
        self.partitions.insert((stream, partition), position);
    }

    /// The positions once `delta`, a commit's state changes in order, applies; its other records
    /// change nothing here.
    pub(crate) fn apply(&mut self, delta: &[StateChange]) {
        for change in delta {
            match change {
                StateChange::Put(record) => match StateEntry::from_record(record) {
                    Ok(StateEntry::Partition {
                        stream,
                        partition,
                        state,
                    }) => self.set(stream, partition, state),
                    Ok(StateEntry::Phase { stream, phase }) => {
                        self.phases.insert(stream, phase);
                    }
                    _ => {}
                },
                StateChange::Delete(key) => match StateKey::parse(key) {
                    Ok(StateKey::Partition(stream, partition)) => {
                        self.partitions.remove(&(stream, partition));
                    }
                    Ok(StateKey::Phase(stream)) => {
                        self.phases.remove(&stream);
                    }
                    _ => {}
                },
            }
        }
    }
}

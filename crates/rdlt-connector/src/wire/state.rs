//! State records, the changes a commit makes to them, and a stream's state on the wire.

use std::collections::BTreeMap;

use super::{Invalid, narrow, required, v1};
use crate::cursor::Cursor;
use crate::id::{GenerationId, PartitionId};
use crate::state::{PartitionState, StateChange, StateRecord, StreamState};

impl From<&StateRecord> for v1::StateRecord {
    fn from(record: &StateRecord) -> Self {
        Self {
            key: record.key.clone(),
            value: record.value.clone(),
        }
    }
}

impl From<v1::StateRecord> for StateRecord {
    fn from(record: v1::StateRecord) -> Self {
        Self {
            key: record.key,
            value: record.value,
        }
    }
}

impl From<&StateChange> for v1::StateChange {
    fn from(change: &StateChange) -> Self {
        use v1::state_change::Change;
        let change = match change {
            StateChange::Put(record) => Change::Put(v1::StateRecord::from(record)),
            StateChange::Delete(key) => Change::Delete(key.clone()),
        };
        Self {
            change: Some(change),
        }
    }
}

impl TryFrom<v1::StateChange> for StateChange {
    type Error = Invalid;

    fn try_from(change: v1::StateChange) -> Result<Self, Invalid> {
        use v1::state_change::Change;
        Ok(match required("state change", change.change)? {
            Change::Put(record) => Self::Put(StateRecord::from(record)),
            Change::Delete(key) => Self::Delete(key),
        })
    }
}

impl From<&StreamState> for v1::StreamState {
    fn from(state: &StreamState) -> Self {
        use v1::partition_state::State;
        let partitions = state
            .partitions
            .iter()
            .map(|(partition, position)| v1::PartitionState {
                partition: partition.as_str().to_owned(),
                state: Some(match position {
                    PartitionState::Cursor(cursor) => State::Cursor(v1::Cursor::from(cursor)),
                    PartitionState::Done => State::Done(v1::Unit {}),
                }),
            })
            .collect();
        Self {
            phase: u32::from(state.phase),
            partitions,
            generation: state.generation.map(|generation| generation.0),
            completed: state
                .completed
                .iter()
                .map(|generation| generation.0)
                .collect(),
        }
    }
}

impl TryFrom<v1::StreamState> for StreamState {
    type Error = Invalid;

    fn try_from(state: v1::StreamState) -> Result<Self, Invalid> {
        use v1::partition_state::State;
        let mut partitions = BTreeMap::new();
        for entry in state.partitions {
            let id = PartitionId::parse(entry.partition)
                .map_err(|error| Invalid::rejected("partition id", error))?;
            let position = match required("partition state", entry.state)? {
                State::Cursor(cursor) => PartitionState::Cursor(Cursor::try_from(cursor)?),
                State::Done(_) => PartitionState::Done,
            };
            if partitions.insert(id, position).is_some() {
                return Err(Invalid::Duplicate("partition"));
            }
        }
        Ok(Self {
            phase: narrow("stream phase", state.phase)?,
            partitions,
            generation: state.generation.map(GenerationId),
            completed: state.completed.into_iter().map(GenerationId).collect(),
        })
    }
}

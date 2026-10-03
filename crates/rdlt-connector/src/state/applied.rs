//! What putting or deleting one entry of state does to a pipeline's state.

use super::{Epoch, NameMap, PipelineState, StateEntry, StateKey};

impl PipelineState {
    pub(super) fn put(&mut self, entry: StateEntry) {
        match entry {
            StateEntry::Epoch(epoch) => self.epoch = epoch,
            StateEntry::Phase { stream, phase } => {
                self.streams.entry(stream).or_default().phase = phase;
            }
            StateEntry::Partition {
                stream,
                partition,
                state,
                load,
            } => {
                let positions = &mut self.streams.entry(stream.clone()).or_default().partitions;
                positions.insert(partition.clone(), state);
                self.recorded_by.insert((stream, partition), load);
            }
            StateEntry::Generation { stream, generation } => {
                self.streams.entry(stream).or_default().generation = Some(generation);
            }
            StateEntry::Completed {
                stream,
                generations,
            } => {
                self.streams.entry(stream).or_default().completed = generations;
            }
            StateEntry::Reset { stream, epoch } => {
                self.resets.insert(stream, epoch);
            }
            StateEntry::Schema {
                table,
                version,
                schema,
                exact,
            } => {
                let state = self.tables.entry(table).or_default();
                (state.schema, state.exact) = (Some((version, schema)), exact);
            }
            StateEntry::Names {
                table,
                physical,
                names,
            } => {
                let state = self.tables.entry(table).or_default();
                (state.physical, state.names) = (Some(physical), names);
            }
            StateEntry::Sequences {
                table,
                sequences,
                history,
                key,
                change_time,
            } => {
                let state = self.tables.entry(table).or_default();
                (state.sequences, state.history) = (Some(sequences), history);
                (state.key, state.change_time) = (key, change_time);
            }
            StateEntry::Receipt(receipt) => self.last_receipt = Some(receipt),
            StateEntry::Origin(origin) => self.origin = Some(origin),
        }
    }

    pub(super) fn delete(&mut self, key: &StateKey) {
        match key {
            StateKey::Epoch => self.epoch = Epoch::default(),
            StateKey::Phase(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.phase = 0;
                }
            }
            StateKey::Partition(stream, partition) => {
                self.recorded_by
                    .remove(&(stream.clone(), partition.clone()));
                if let Some(state) = self.streams.get_mut(stream) {
                    state.partitions.remove(partition);
                }
            }
            StateKey::Generation(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.generation = None;
                }
            }
            StateKey::Completed(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.completed.clear();
                }
            }
            StateKey::Reset(stream) => {
                self.resets.remove(stream);
            }
            StateKey::Schema(table) => {
                if let Some(state) = self.tables.get_mut(table) {
                    state.schema = None;
                    state.exact.clear();
                }
            }
            StateKey::Names(table) => {
                if let Some(state) = self.tables.get_mut(table) {
                    state.physical = None;
                    state.names = NameMap::default();
                }
            }
            StateKey::Sequences(table) => {
                if let Some(state) = self.tables.get_mut(table) {
                    state.sequences = None;
                    state.history = false;
                    state.key.clear();
                    state.change_time = None;
                }
            }
            StateKey::Receipt => self.last_receipt = None,
            StateKey::Origin => self.origin = None,
        }
    }
}

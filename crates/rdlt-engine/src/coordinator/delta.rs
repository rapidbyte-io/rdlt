//! What a commit carries: the sealed segments it publishes, the state it records and what each
//! stream contributes to it.

use std::collections::BTreeMap;

use rdlt_connector::{
    GenerationId, PartitionState, SegmentSet, StateChange, StateEntry, StateKey, StreamName,
    TablePath,
};

use super::{Coordinator, KEPT_COMPLETIONS};
use crate::plan::WriteMode;
use crate::report::StreamReport;

/// What the sealed segments of a commit add up to.
pub(super) struct Collected {
    pub(super) segments: SegmentSet,
    pub(super) positions: BTreeMap<usize, PartitionState>,
    pub(super) streams: BTreeMap<usize, StreamReport>,
}

impl Coordinator {
    /// The sealed segments with rows, each partition's newest position, and each stream's counts,
    /// discards included.
    pub(super) fn collect(&mut self) -> Collected {
        let mut collected = Collected {
            segments: SegmentSet::new(),
            positions: BTreeMap::new(),
            streams: BTreeMap::new(),
        };
        for seal in std::mem::take(&mut self.sealed) {
            let stream = self.parts.partitions[seal.partition].stream;
            if seal.rows > 0 {
                collected.segments.insert(seal.segment);
                let counts = collected.streams.entry(stream).or_default();
                counts.rows += seal.rows;
                counts.bytes += seal.bytes;
                counts.commits = 1;
            }
            if seal.discarded_rows > 0 || seal.discarded_values > 0 {
                let counts = collected.streams.entry(stream).or_default();
                counts.discarded_rows += seal.discarded_rows;
                counts.discarded_values += seal.discarded_values;
            }
            collected.positions.insert(seal.partition, seal.state);
        }
        collected
    }

    /// What each stream contributes to a commit that ends the cycles of `completing`, by name.
    pub(super) fn stream_reports(
        &self,
        mut streams: BTreeMap<usize, StreamReport>,
        completing: &[usize],
    ) -> BTreeMap<StreamName, StreamReport> {
        for index in completing {
            if self.parts.streams[*index].write == WriteMode::Replace {
                streams.entry(*index).or_default().generations_swapped = 1;
            }
        }
        streams
            .into_iter()
            .map(|(index, counts)| (self.parts.streams[index].name.clone(), counts))
            .collect()
    }

    /// The receipt the next commit records in state, from the engine's own counts.
    pub(super) fn marker(
        &self,
        streams: &BTreeMap<StreamName, StreamReport>,
    ) -> rdlt_connector::Receipt {
        rdlt_connector::Receipt {
            load_id: self.parts.load_id,
            commit_seq: self.seq,
            committed_at: self.parts.env.now(),
            rows: streams.values().map(|counts| counts.rows).sum(),
            bytes: streams.values().map(|counts| counts.bytes).sum(),
        }
    }

    /// The stream state changes of a commit that publishes `positions` and ends the cycles of
    /// `completing`; the tables add their own.
    pub(super) fn state_delta(
        &self,
        positions: &BTreeMap<usize, PartitionState>,
        completing: &[usize],
    ) -> Vec<StateChange> {
        let mut delta = Vec::new();
        for stream in &self.parts.streams {
            if let Some(cycle) = stream.cycle.as_ref().filter(|cycle| !cycle.recorded) {
                for partition in &cycle.stale {
                    let key = StateKey::Partition(stream.name.clone(), partition.clone());
                    delta.push(StateChange::Delete(key.encode()));
                }
                let entry = StateEntry::Generation {
                    stream: stream.name.clone(),
                    generation: cycle.generation,
                };
                delta.push(StateChange::Put(entry.to_record()));
            }
        }
        for (partition, state) in positions {
            let partition = &self.parts.partitions[*partition];
            let entry = StateEntry::Partition {
                stream: self.parts.streams[partition.stream].name.clone(),
                partition: partition.id.clone(),
                state: state.clone(),
            };
            delta.push(StateChange::Put(entry.to_record()));
        }
        for index in completing {
            let stream = &self.parts.streams[*index];
            let Some(cycle) = &stream.cycle else {
                continue;
            };
            let key = StateKey::Generation(stream.name.clone());
            delta.push(StateChange::Delete(key.encode()));
            let mut generations = cycle.completed.clone();
            generations.push(cycle.generation);
            let excess = generations.len().saturating_sub(KEPT_COMPLETIONS);
            generations.drain(..excess);
            let completed = StateEntry::Completed {
                stream: stream.name.clone(),
                generations,
            };
            delta.push(StateChange::Put(completed.to_record()));
        }
        delta
    }

    /// The generations a commit ending the cycles of `completing` swaps in.
    pub(super) fn finish_generations(
        &self,
        completing: &[usize],
    ) -> Vec<(TablePath, GenerationId)> {
        completing
            .iter()
            .map(|index| &self.parts.streams[*index])
            .filter(|stream| stream.write == WriteMode::Replace)
            .filter_map(|stream| {
                let cycle = stream.cycle.as_ref()?;
                Some((stream.table, cycle.generation))
            })
            .flat_map(|(table, generation)| {
                // A stream's child tables swap in with its table.
                let family = self.parts.tables.family(table);
                family.into_iter().map(move |path| (path, generation))
            })
            .collect()
    }
}

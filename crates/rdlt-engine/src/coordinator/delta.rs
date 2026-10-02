//! What a commit carries: the sealed segments it publishes, the state it records and what each
//! stream contributes to it.

use std::collections::BTreeMap;

use rdlt_connector::{
    Cursor, GenerationId, PartitionState, SegmentSet, StateChange, StateEntry, StateKey,
    StreamName, TablePath,
};

use super::{Coordinator, KEPT_COMPLETIONS};
use crate::partition::{CursorHold, Seal};
use crate::plan::WriteMode;
use crate::report::StreamReport;
use crate::wal::Sealed;

/// What the sealed segments of a commit add up to.
pub(super) struct Collected {
    pub(super) segments: SegmentSet,
    pub(super) positions: BTreeMap<usize, PartitionState>,
    /// Each partition's newest cursor among the seals, which its source is told is committed: a
    /// partition that ends done after its last checkpoint is still told that checkpoint.
    pub(super) reported: BTreeMap<usize, Cursor>,
    pub(super) streams: BTreeMap<usize, StreamReport>,
    /// Every seal, as the load's log records them before their commit: empty segments move
    /// their partitions too.
    pub(super) sealed: Vec<Sealed>,
    /// What holds the seals' cursors in the budget.
    pub(super) held: Vec<CursorHold>,
}

impl Coordinator {
    /// The sealed segments with rows, each partition's newest position, and each stream's counts,
    /// discards included; `begun` is the phases the commit begins, which its log records first.
    pub(super) fn collect(&mut self, begun: &[StateChange]) -> Collected {
        let mut collected = Collected {
            segments: SegmentSet::new(),
            positions: BTreeMap::new(),
            reported: BTreeMap::new(),
            streams: BTreeMap::new(),
            sealed: Vec::new(),
            held: Vec::new(),
        };
        let seals = self.sealed.take();
        if self.parts.wal.is_some() {
            collected.sealed = self.logged(&seals, begun);
        }
        for seal in seals {
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
            if seal.deletes_ignored > 0 || seal.truncates_ignored > 0 {
                let counts = collected.streams.entry(stream).or_default();
                counts.deletes_ignored += seal.deletes_ignored;
                counts.truncates_ignored += seal.truncates_ignored;
            }
            if let PartitionState::Cursor(cursor) = &seal.state {
                collected.reported.insert(seal.partition, cursor.clone());
            }
            collected.positions.insert(seal.partition, seal.state);
            collected.held.push(seal.held);
        }
        // A partition done with no cursor among the seals is told the cursor it stood at: a
        // report of it that failed is not made by any later attempt, which plans it no more.
        for (partition, state) in &collected.positions {
            let stood = &self.parts.partitions[*partition].stands;
            if let (PartitionState::Done, Some(PartitionState::Cursor(cursor))) = (state, stood)
                && !collected.reported.contains_key(partition)
            {
                collected.reported.insert(*partition, cursor.clone());
            }
        }
        collected
    }

    /// `seals` as the load's log records them: each with its stream's phase and where its partition
    /// stood before it, as the commit's changes reach it, the phase it begins, `begun`, first.
    fn logged(&self, seals: &[Seal], begun: &[StateChange]) -> Vec<Sealed> {
        let mut positions = self.parts.positions.clone();
        positions.apply(begun);
        seals
            .iter()
            .map(|seal| {
                let partition = &self.parts.partitions[seal.partition];
                let stream = &self.parts.streams[partition.stream];
                let from = positions.get(&stream.name, &partition.id).cloned();
                positions.set(
                    stream.name.clone(),
                    partition.id.clone(),
                    seal.state.clone(),
                );
                Sealed {
                    segment: seal.segment,
                    stream: stream.name.clone(),
                    partition: partition.id.clone(),
                    replayable: stream.replayable,
                    phase: positions.phase(&stream.name),
                    from,
                    state: seal.state.clone(),
                }
            })
            .collect()
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

    /// The state changes recording how each stream's table sequences and matches its rows, where
    /// state records otherwise: the next commit takes them.
    pub(super) fn sequences_delta(&mut self) -> Vec<StateChange> {
        self.parts
            .streams
            .iter_mut()
            .filter_map(|stream| stream.sequences.take())
            .map(|(table, keying)| {
                let entry = StateEntry::Sequences {
                    table,
                    sequences: keying.sequences,
                    history: keying.history,
                    key: keying.key,
                    change_time: keying.change_time,
                };
                StateChange::Put(entry.to_record())
            })
            .collect()
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
            delta.push(self.position(*partition, state));
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

    /// The state change recording that partition `partition` stands at `state`.
    pub(super) fn position(&self, partition: usize, state: &PartitionState) -> StateChange {
        let partition = &self.parts.partitions[partition];
        let entry = StateEntry::Partition {
            stream: self.parts.streams[partition.stream].name.clone(),
            partition: partition.id.clone(),
            state: state.clone(),
        };
        StateChange::Put(entry.to_record())
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

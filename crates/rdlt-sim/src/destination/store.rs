//! The destination's store: tables every pipeline shares, each pipeline's epoch, state and
//! staging, and how a commit publishes into them.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, GenerationId, LoadId, MergeKey, PartitionId, PipelineId, Receipt,
    SegmentId, StateChange, StateEntry, StateRecord, StreamName, TablePath,
};

use super::read::{names, next_offset};
use super::{Stored, cells, columns};
use crate::world::World;

/// The destination's contents, kept in its world: tables every pipeline shares, and each
/// pipeline's own epoch, state and staging.
#[derive(Debug, Default)]
pub(crate) struct Store {
    pub(super) pipelines: BTreeMap<PipelineId, PipelineStore>,
    pub(super) receipts: BTreeMap<(LoadId, CommitSeq), Receipt>,
    /// Staged rows, by the pipeline whose session staged them and their segment.
    pub(super) staged: BTreeMap<(PipelineId, SegmentId), Vec<Staged>>,
    pub(super) names: BTreeMap<TablePath, String>,
    pub(super) tables: BTreeMap<String, Table>,
}

/// What the destination keeps for one pipeline.
#[derive(Debug, Default)]
pub(super) struct PipelineStore {
    pub(super) epoch: Epoch,
    pub(super) state: BTreeMap<String, StateRecord>,
    /// The generations of full reads completed, by stream and the phase they completed in.
    ///
    /// A run that read a stream twice would complete the same generation twice, and count once.
    pub(super) completions: BTreeMap<(String, usize), BTreeSet<GenerationId>>,
}

#[derive(Debug)]
pub(super) struct Staged {
    pub(super) table: String,
    pub(super) generation: Option<GenerationId>,
    pub(super) merge: Option<MergeKey>,
    pub(super) rows: Vec<Stored>,
}

#[derive(Debug, Default)]
pub(super) struct Table {
    pub(super) columns: columns::Columns,
    pub(super) published: Vec<Stored>,
    pub(super) generations: BTreeMap<GenerationId, Vec<Stored>>,
}

/// A digest of everything a destination holds: its tables' columns and rows, and each pipeline's
/// state and receipts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Digest(u64);

impl Store {
    /// A digest of everything the store holds.
    pub(crate) fn digest(&self) -> Digest {
        let mut hasher = DefaultHasher::new();
        for (name, table) in &self.tables {
            name.hash(&mut hasher);
            format!("{:?}", table.columns).hash(&mut hasher);
            for row in &table.published {
                format!("{:?}", row.cells).hash(&mut hasher);
            }
        }
        for (pipeline, store) in &self.pipelines {
            pipeline.to_string().hash(&mut hasher);
            format!("{:?}", store.state).hash(&mut hasher);
        }
        format!("{:?}", self.receipts).hash(&mut hasher);
        Digest(hasher.finish())
    }

    /// The epoch of `pipeline`'s newest session.
    pub(super) fn epoch(&self, pipeline: &PipelineId) -> Epoch {
        self.pipelines
            .get(pipeline)
            .map_or_else(Epoch::default, |store| store.epoch)
    }

    /// Publishes the segments of `meta` that `pipeline` staged and swaps in the generations it
    /// finishes; returns the rows published, by table.
    pub(super) fn publish(
        &mut self,
        pipeline: &PipelineId,
        meta: &CommitMeta,
    ) -> Vec<(String, Vec<Stored>)> {
        let mut published = Vec::new();
        let mut merging: BTreeMap<String, (MergeKey, Vec<Stored>)> = BTreeMap::new();
        for segment in meta.segments.iter() {
            let staged = self.staged.remove(&(pipeline.clone(), segment));
            for staged in staged.unwrap_or_default() {
                published.push((staged.table.clone(), staged.rows.clone()));
                if let Some(key) = staged.merge {
                    merging
                        .entry(staged.table)
                        .or_insert_with(|| (key, Vec::new()))
                        .1
                        .extend(staged.rows);
                    continue;
                }
                let table = self.tables.entry(staged.table).or_default();
                match staged.generation {
                    Some(generation) => table
                        .generations
                        .entry(generation)
                        .or_default()
                        .extend(staged.rows),
                    None => table.published.extend(staged.rows),
                }
            }
        }
        for child in &meta.child_tables {
            merging
                .entry(child.table.to_string())
                .or_insert_with(|| (child.merge.clone(), Vec::new()));
        }
        let roots: BTreeMap<String, Vec<Stored>> = merging
            .iter()
            .filter(|(_, (key, _))| key.root.is_none())
            .map(|(name, (_, rows))| (name.clone(), rows.clone()))
            .collect();
        for (name, (key, rows)) in merging {
            let published = &mut self.tables.entry(name).or_default().published;
            match &key.root {
                None => cells::merge(published, rows, &key),
                Some(root) => {
                    let roots = roots
                        .get(root.table.as_ref())
                        .map_or(&[][..], Vec::as_slice);
                    cells::merge_children(published, rows, &key, root, roots);
                }
            }
        }
        for (path, generation) in &meta.finish_generations {
            let Some(name) = self.names.get(path).cloned() else {
                continue;
            };
            let table = self.tables.entry(name).or_default();
            table.published = table.generations.remove(generation).unwrap_or_default();
            table.generations.clear();
        }
        published
    }

    /// Applies the state changes of `meta` to `pipeline`'s state, checking that no partition's
    /// cursor moves backwards and counting completed full reads.
    pub(super) fn apply(&mut self, world: &World, pipeline: &PipelineId, meta: &CommitMeta) {
        let store = self.pipelines.entry(pipeline.clone()).or_default();
        for change in &meta.state_delta {
            match change {
                StateChange::Put(record) => {
                    match StateEntry::from_record(record) {
                        Ok(StateEntry::Partition {
                            stream, partition, ..
                        }) => {
                            let before = next_offset(&store.state, &stream, &partition);
                            store.state.insert(record.key.clone(), record.clone());
                            let after = next_offset(&store.state, &stream, &partition);
                            if before.is_some_and(|before| after < Some(before)) {
                                world.violation(format!(
                                    "stream {stream} partition {partition}: cursor moved back \
                                     from {before:?} to {after:?}"
                                ));
                            }
                            continue;
                        }
                        Ok(StateEntry::Completed {
                            stream,
                            generations,
                        }) => {
                            // The read that just completed is the newest in the list.
                            let key = (stream.to_string(), world.phase());
                            let newest = generations.last().copied();
                            store.completions.entry(key).or_default().extend(newest);
                        }
                        Ok(_) => {}
                        Err(error) => world.violation(format!("unreadable state record: {error}")),
                    }
                    store.state.insert(record.key.clone(), record.clone());
                }
                StateChange::Delete(key) => {
                    store.state.remove(key);
                }
            }
        }
    }

    /// Checks that every row just published to a stream's table lies before its partition's
    /// cursor `pipeline` committed.
    pub(super) fn check_cursors(
        &self,
        world: &World,
        pipeline: &PipelineId,
        published: &[(String, Vec<Stored>)],
    ) {
        let Some(store) = self.pipelines.get(pipeline) else {
            return;
        };
        for (table, rows) in published {
            let Some((path, _)) = self.names.iter().find(|(_, name)| *name == table) else {
                continue;
            };
            // A child table's rows carry no position; the oracle checks them against their rows.
            if path.segments().count() > 1 {
                continue;
            }
            let Some(stream) = path
                .segments()
                .next()
                .and_then(|name| StreamName::new(name).ok())
            else {
                continue;
            };
            let Some((_, names)) = names(&store.state, path) else {
                world.violation(format!("stream {stream}: rows published without names"));
                continue;
            };
            for row in rows {
                let number = |column: &str| cells::number(row, &names, column);
                let (Some(partition), Some(offset)) = (number("partition"), number("offset"))
                else {
                    world.violation(format!("stream {stream}: a row lacks its position"));
                    continue;
                };
                let partition =
                    PartitionId::parse(format!("p{partition}")).expect("valid partition id");
                let next = next_offset(&store.state, &stream, &partition);
                if next.is_none_or(|next| offset >= next) {
                    world.violation(format!(
                        "stream {stream} partition {partition}: row {offset} published past the \
                         committed cursor {next:?}"
                    ));
                }
            }
        }
    }
}

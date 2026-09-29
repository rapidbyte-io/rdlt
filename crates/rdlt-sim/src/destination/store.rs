//! The destination's store: tables every pipeline shares, each pipeline's epoch, state and
//! staging, and how a commit publishes into them.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectorError, Epoch, GenerationId, LoadId, MergeKey, PartitionId,
    PipelineId, Receipt, Result, SegmentId, StateChange, StateEntry, StateRecord, StreamName,
    TablePath, TableRef,
};

use super::read::{names, next_offset};
use super::tombstones::Tombstones;
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
    /// The pipeline the table belongs to: the first to refer to it.
    pub(super) owner: Option<PipelineId>,
    pub(super) columns: columns::Columns,
    pub(super) published: Vec<Stored>,
    pub(super) generations: BTreeMap<GenerationId, Vec<Stored>>,
    /// A change stream's tombstones.
    pub(super) tombstones: Tombstones,
}

/// A digest of everything a destination holds: its tables' columns and rows, and each pipeline's
/// state and receipts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Digest(u64);

impl Store {
    /// A digest of everything the store holds.
    pub(crate) fn digest(&self) -> Digest {
        let mut hasher = DefaultHasher::new();
        self.hash_tables(&mut hasher);
        for (pipeline, store) in &self.pipelines {
            pipeline.to_string().hash(&mut hasher);
            format!("{:?}", store.state).hash(&mut hasher);
        }
        format!("{:?}", self.receipts).hash(&mut hasher);
        Digest(hasher.finish())
    }

    /// A digest of the tables alone: their columns and published rows.
    pub(crate) fn tables_digest(&self) -> Digest {
        let mut hasher = DefaultHasher::new();
        self.hash_tables(&mut hasher);
        Digest(hasher.finish())
    }

    fn hash_tables(&self, hasher: &mut DefaultHasher) {
        for (name, table) in &self.tables {
            name.hash(hasher);
            format!("{:?}", table.columns).hash(hasher);
            for row in &table.published {
                format!("{:?}", row.cells).hash(hasher);
            }
        }
    }

    /// Whether the table of `stream` exists.
    pub(crate) fn has_table(&self, stream: &str) -> bool {
        TablePath::new([stream])
            .ok()
            .and_then(|path| self.names.get(&path))
            .is_some_and(|name| self.tables.contains_key(name))
    }

    /// The table `table` refers to, claimed for `pipeline` where no pipeline owns it yet, its
    /// name recorded for its path; another pipeline's table is refused as `table_owned`.
    pub(super) fn claim(&mut self, pipeline: &PipelineId, table: &TableRef) -> Result<&mut Table> {
        let entry = self.tables.entry(table.name.to_string()).or_default();
        let owner = entry.owner.get_or_insert_with(|| pipeline.clone());
        if owner != pipeline {
            return Err(ConnectorError::table_owned(&table.name, owner.as_str()));
        }
        self.names
            .insert(table.path.clone(), table.name.to_string());
        Ok(entry)
    }

    /// The rows published to the table of `stream`.
    pub(crate) fn published(&self, stream: &str) -> Vec<Stored> {
        TablePath::new([stream])
            .ok()
            .and_then(|path| self.names.get(&path))
            .and_then(|name| self.tables.get(name))
            .map(|table| table.published.clone())
            .unwrap_or_default()
    }

    /// Every pipeline's state records.
    pub(crate) fn states(&self) -> impl Iterator<Item = &BTreeMap<String, StateRecord>> {
        self.pipelines.values().map(|store| &store.state)
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
            let table = self.tables.entry(name).or_default();
            let published = &mut table.published;
            match (&key.root, &key.changes) {
                (None, Some(changes)) => {
                    cells::merge_changes(published, &mut table.tombstones, rows, &key, changes);
                }
                (None, None) => cells::merge(published, rows, &key),
                (Some(root), _) => {
                    let roots = roots
                        .get(root.table.as_ref())
                        .map_or(&[][..], Vec::as_slice);
                    cells::merge_children(published, rows, &key, root, roots);
                }
            }
        }
        self.finish(meta);
        published
    }

    /// Swaps in the generations `meta` finishes, each table's rows replaced whole.
    fn finish(&mut self, meta: &CommitMeta) {
        for (path, generation) in &meta.finish_generations {
            let Some(name) = self.names.get(path).cloned() else {
                continue;
            };
            let table = self.tables.entry(name).or_default();
            table.published = table.generations.remove(generation).unwrap_or_default();
            table.generations.clear();
            table.tombstones = Tombstones::default();
        }
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
        // A change stream's rows carry positions of their own, which its oracle checks.
        if !world.changes.streams.is_empty() {
            return;
        }
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

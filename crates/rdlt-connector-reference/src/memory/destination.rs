//! A transactional destination that keeps tables and state in process memory.

mod owners;
mod receipts;
mod table;

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{
    CommitSeq, DeleteModes, Epoch, GenerationId, LoadId, PipelineId, SchemaChanges, SegmentId,
    SegmentSet, StateRecord, TablePath, TypeKind,
};

use crate::merge::Merged;
use schemars::JsonSchema;
use serde::Deserialize;
use table::{Plan, Staged, Table, holds_key};

/// Configuration of [`MemoryDestination`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemoryDestinationConfig {
    /// The store to write to; connections naming the same store share its data, those of one
    /// host where the connector listens for hosts.
    pub store: String,
}

/// Keeps published tables, staging and pipeline state in a named in-process store.
///
/// A store is its host's: where the connector listens for hosts, each host named to it has stores
/// of its own, whatever their names, and reaches no other host's. In its host's own process, or
/// spawned by it, the stores are that host's alone.
///
/// A replace generation's rows stay hidden until the commit that finishes the generation swaps
/// them in for the table's rows. A merge table keeps one row per key: the newest commit's, and
/// within a commit the row with the greatest sequence; a history table keeps every version of
/// each key. Commits are atomic under the store's lock, idempotent on `(load_id, commit_seq)` and
/// fenced by the pipeline's epoch; so are flushes, so a fenced worker cannot stage rows that the
/// latest session would publish.
#[derive(Debug)]
pub struct MemoryDestination {
    store: Arc<Mutex<Store>>,
}

/// Every published batch of `table` in `store`, in commit order.
///
/// This and the functions beside it read the stores of this process's own host, not those a
/// listening connector keeps for the hosts named to it.
pub fn published(store: &str, table: &str) -> Vec<RecordBatch> {
    let store = named(None, store);
    let store = store.lock();
    store.tables.get(table).map(Table::read).unwrap_or_default()
}

/// The tables of `store`, by name.
pub fn tables(store: &str) -> Vec<String> {
    let store = named(None, store);
    let store = store.lock();
    store.tables.keys().cloned().collect()
}

/// The rows staged in `table` of `store` and not yet published, whichever session staged them.
pub fn staged(store: &str, table: &str) -> usize {
    let store = named(None, store);
    let store = store.lock();
    store.tables.get(table).map_or(0, |table| {
        table
            .staged
            .values()
            .flatten()
            .map(|(_, batch)| batch.num_rows())
            .sum()
    })
}

/// The columns of `table` in `store`, once it exists.
pub fn schema(store: &str, table: &str) -> Option<TableSchema> {
    let store = named(None, store);
    let store = store.lock();
    store
        .tables
        .get(table)
        .and_then(|table| table.schema.clone())
}

/// The stores, each by the host it is kept for and its name.
type Stores = BTreeMap<(Option<String>, String), Arc<Mutex<Store>>>;

static STORES: LazyLock<Mutex<Stores>> = LazyLock::new(Mutex::default);

/// The store `name` of `host`: of this process's own host, where none is named.
fn named(host: Option<&str>, name: &str) -> Arc<Mutex<Store>> {
    let key = (host.map(str::to_owned), name.to_owned());
    Arc::clone(STORES.lock().entry(key).or_default())
}

#[derive(Debug, Default)]
struct Store {
    pipelines: BTreeMap<PipelineId, PipelineStore>,
    tables: BTreeMap<String, Table>,
    /// The name of each table a pipeline has referred to, by the pipeline and its logical path.
    names: BTreeMap<(PipelineId, TablePath), String>,
}

impl Store {
    /// Publishes the segments of `meta` that `pipeline`'s session at `epoch` staged and swaps in
    /// the generations it finishes; returns the rows and bytes published.
    ///
    /// Every table's publish is worked out before anything changes, so a failure leaves the store
    /// as it was.
    fn publish(
        &mut self,
        pipeline: &PipelineId,
        epoch: Epoch,
        meta: &CommitMeta,
    ) -> Result<(u64, u64)> {
        self.owns(pipeline, meta)?;
        let plans = self.plans(pipeline, epoch, meta)?;
        let (mut rows, mut bytes) = (0, 0);
        for table in self.tables.values_mut() {
            let published: Vec<_> = staged_in(table, pipeline, epoch, &meta.segments)
                .map(|(key, _)| key.clone())
                .collect();
            for key in published {
                table.staged.remove(&key);
            }
            // What the session staged in a segment the load abandoned is never published.
            table.staged.retain(|(staged, at, segment), _| {
                staged != pipeline || *at != epoch || !meta.abandoned.contains(*segment)
            });
        }
        for (name, staged, merged) in plans {
            let table = self.tables.entry(name).or_default();
            for (generation, batch) in staged {
                rows += table::counted(batch.num_rows());
                bytes += table::counted(batch.get_array_memory_size());
                if merged.is_none() {
                    match generation {
                        Some(generation) => {
                            table.generations.entry(generation).or_default().push(batch);
                        }
                        None => table.published.push(batch),
                    }
                }
            }
            if let Some(merged) = merged {
                table.published = merged.rows;
                table.tombstones = merged.tombstones;
            }
        }
        for (path, generation) in &meta.finish_generations {
            // A path the pipeline registered no table for names one it never created.
            let Some(name) = self.named(pipeline, path).map(str::to_owned) else {
                continue;
            };
            let table = self.tables.entry(name).or_default();
            table.published = table.generations.remove(generation).unwrap_or_default();
            table.generations.clear();
            table.tombstones.clear();
        }
        for dropped in &meta.drop_tables {
            self.tables.remove(&*dropped.name);
            self.names.retain(|_, name| **name != *dropped.name);
        }
        Ok((rows, bytes))
    }

    /// What publishing `meta` does to each table: the batches it staged, and for a merge table
    /// its rows once merged.
    fn plans(&self, pipeline: &PipelineId, epoch: Epoch, meta: &CommitMeta) -> Result<Vec<Plan>> {
        let mut plans = Vec::new();
        for (name, table) in &self.tables {
            let staged: Staged = staged_in(table, pipeline, epoch, &meta.segments)
                .flat_map(|(_, batches)| batches)
                .cloned()
                .collect();
            // A child table the commit lists follows its root's staged rows even where it staged
            // nothing.
            let listed = meta
                .child_tables
                .iter()
                .find(|child| *child.table == **name)
                .map(|child| &child.merge)
                .filter(|key| {
                    key.root.as_ref().is_some_and(|root| {
                        !self
                            .staged_rows(&root.table, pipeline, epoch, meta)
                            .is_empty()
                    })
                });
            if staged.is_empty() && listed.is_none() {
                continue;
            }
            self.owned(pipeline, name)?;
            let merge = table.merge.as_ref().or(listed);
            let merged = match merge {
                Some(key) => match &key.root {
                    Some(root) => {
                        let roots = self.staged_rows(&root.table, pipeline, epoch, meta);
                        let rows = table.merged_children(&staged, key, root, &roots)?;
                        Some(Merged {
                            rows,
                            tombstones: Vec::new(),
                        })
                    }
                    None => Some(table.merged(&staged, key)?),
                },
                None => None,
            };
            plans.push((name.clone(), staged, merged));
        }
        Ok(plans)
    }

    /// The batches `pipeline`'s session at `epoch` staged for the table `name` in `meta`'s
    /// segments.
    fn staged_rows(
        &self,
        name: &str,
        pipeline: &PipelineId,
        epoch: Epoch,
        meta: &CommitMeta,
    ) -> Vec<RecordBatch> {
        let Some(table) = self.tables.get(name) else {
            return Vec::new();
        };
        staged_in(table, pipeline, epoch, &meta.segments)
            .flat_map(|(_, batches)| batches)
            .map(|(_, batch)| batch.clone())
            .collect()
    }
}

/// What `pipeline`'s session at `epoch` staged in `table` in `segments`, with its keys, in the
/// order of its segments.
///
/// It walks what was staged, not the ids `segments` names: a range a host sends may name every
/// id there is.
fn staged_in<'a>(
    table: &'a Table,
    pipeline: &PipelineId,
    epoch: Epoch,
    segments: &'a SegmentSet,
) -> impl Iterator<Item = (&'a (PipelineId, Epoch, SegmentId), &'a Staged)> {
    let first = (pipeline.clone(), epoch, SegmentId(0));
    let last = (pipeline.clone(), epoch, SegmentId(u64::MAX));
    table
        .staged
        .range(first..=last)
        .filter(|((_, _, segment), _)| segments.contains(*segment))
}

#[derive(Debug, Default)]
struct PipelineStore {
    epoch: Epoch,
    state: BTreeMap<String, StateRecord>,
    receipts: BTreeMap<(LoadId, CommitSeq), Receipt>,
}

#[destination(id = "io.rapidbyte.memory")]
impl DestinationConnector for MemoryDestination {
    type Config = MemoryDestinationConfig;
    type Session = MemorySession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        capabilities.write_modes.replace = true;
        capabilities.write_modes.merge = true;
        capabilities.write_modes.history = true;
        capabilities.delete_modes = DeleteModes {
            hard: true,
            soft: true,
        };
        capabilities.partial_updates = true;
        capabilities.merge_changes = true;
        capabilities.drop_tables = true;
        capabilities.schema_changes = SchemaChanges::all();
        capabilities.nested.structs = true;
        capabilities.nested.lists = true;
        capabilities.nested.json = true;
        capabilities.types.extend([
            TypeKind::Uuid,
            TypeKind::Json,
            TypeKind::Struct,
            TypeKind::List,
        ]);
        capabilities
    }

    async fn connect(config: MemoryDestinationConfig, context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            store: named(context.host(), &config.store),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<MemorySession>> {
        let mut store = self.store.lock();
        let pipeline = store.pipelines.entry(context.pipeline.clone()).or_default();
        let Some(epoch) = pipeline.epoch.next() else {
            return Err(ConnectorError::data(format!(
                "pipeline {} holds the last epoch there is",
                context.pipeline
            )));
        };
        pipeline.epoch = epoch;
        let state = pipeline.state.values().cloned().collect();
        let session = MemorySession {
            store: Arc::clone(&self.store),
            pipeline: context.pipeline.clone(),
            epoch,
        };
        Ok(Opened {
            session,
            epoch,
            state,
        })
    }
}

/// A [`MemoryDestination`] session.
#[derive(Debug)]
pub struct MemorySession {
    store: Arc<Mutex<Store>>,
    pipeline: PipelineId,
    epoch: Epoch,
}

impl Session for MemorySession {
    type Writer = MemoryWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        let mut store = self.store.lock();
        let entry = store.table(&self.pipeline, self.epoch, change.table())?;
        entry.change(change)
    }

    async fn writer(&mut self, table: &TableRef) -> Result<MemoryWriter> {
        crate::merge::refuse_history_generation(table)?;
        let mut store = self.store.lock();
        if let Some(key) = &table.merge {
            let held = store.tables.get(&*table.name);
            holds_key(&table.name, key, held.and_then(|held| held.schema.as_ref()))?;
        }
        store.table(&self.pipeline, self.epoch, table)?;
        drop(store);
        Ok(MemoryWriter {
            store: Arc::clone(&self.store),
            pipeline: self.pipeline.clone(),
            epoch: self.epoch,
            table: table.name.to_string(),
            generation: table.generation,
            buffered: Vec::new(),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        let mut store = self.store.lock();
        for table in store.tables.values_mut() {
            table.staged.retain(|(pipeline, epoch, _), _| {
                *pipeline != self.pipeline || *epoch >= self.epoch
            });
        }
        Ok(())
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let mut store = self.store.lock();
        let current = store
            .pipelines
            .entry(self.pipeline.clone())
            .or_default()
            .epoch;
        if current != self.epoch || meta.epoch != self.epoch {
            let message = format!(
                "pipeline {} is at epoch {current}; this session opened at {}",
                self.pipeline, self.epoch
            );
            return Err(ConnectorError::fenced(message));
        }
        let key = (meta.load_id, meta.commit_seq);
        if let Some(receipt) = store.pipelines[&self.pipeline].receipts.get(&key) {
            return Ok(receipt.clone());
        }
        let (rows, bytes) = store.publish(&self.pipeline, self.epoch, meta)?;
        let receipt = Receipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
            committed_at: SystemTime::now(),
            rows,
            bytes,
        };
        let pipeline = store.pipelines.entry(self.pipeline.clone()).or_default();
        pipeline.committed(meta, &receipt);
        Ok(receipt)
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

/// Buffers a table's batches and stages them on flush.
#[derive(Debug)]
pub struct MemoryWriter {
    store: Arc<Mutex<Store>>,
    pipeline: PipelineId,
    epoch: Epoch,
    table: String,
    generation: Option<GenerationId>,
    buffered: Vec<(SegmentId, RecordBatch)>,
}

impl TableWriter for MemoryWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        self.buffered.push((segment, batch));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        let mut store = self.store.lock();
        let current = store
            .pipelines
            .get(&self.pipeline)
            .map(|pipeline| pipeline.epoch)
            .unwrap_or_default();
        if current != self.epoch {
            return Err(ConnectorError::fenced(format!(
                "pipeline {} is at epoch {current}; this writer's session opened at {}",
                self.pipeline, self.epoch
            )));
        }
        // The table may have been dropped, and claimed by another pipeline, since the writer opened.
        store.owned(&self.pipeline, &self.table)?;
        let table = store.tables.entry(self.table.clone()).or_default();
        // Nothing of a flush is staged where a batch of it is refused.
        for (_, batch) in &self.buffered {
            table.admits(batch)?;
        }
        let mut stats = WriteStats::default();
        for (segment, batch) in self.buffered.drain(..) {
            stats.rows += table::counted(batch.num_rows());
            stats.bytes += table::counted(batch.get_array_memory_size());
            table
                .staged
                .entry((self.pipeline.clone(), self.epoch, segment))
                .or_default()
                .push((self.generation, batch));
        }
        Ok(stats)
    }
}

#[cfg(feature = "certify")]
impl ReadBack for MemoryDestination {
    async fn published(&self, table: &TableRef, rows: PublishedRows) -> Result<()> {
        // A reader is given every column; the lock is not held while sending.
        let published = {
            let store = self.store.lock();
            store.tables.get(&*table.name).map(Table::read)
        };
        for batch in published.unwrap_or_default() {
            rows.send(batch).await?;
        }
        Ok(())
    }
}

//! A transactional destination that keeps tables and state in process memory.

mod table;

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{
    CommitSeq, DeleteModes, Epoch, GenerationId, LoadId, PipelineId, SchemaChanges, SegmentId,
    StateChange, StateRecord, TablePath, TypeKind,
};

use crate::columns::changed;
use crate::merge::Merged;
use schemars::JsonSchema;
use serde::Deserialize;
use table::{Plan, Staged, Table};

/// Configuration of [`MemoryDestination`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemoryDestinationConfig {
    /// The store to write to; connections naming the same store share its data.
    pub store: String,
}

/// Keeps published tables, staging and pipeline state in a named in-process store.
///
/// A replace generation's rows stay hidden until the commit that finishes the generation swaps
/// them in for the table's rows. A merge table keeps one row per key: the newest commit's, and
/// within a commit the row with the greatest sequence. Commits are atomic under the store's lock, idempotent on `(load_id, commit_seq)` and
/// fenced by the pipeline's epoch; so are flushes, so a fenced worker cannot stage rows that the
/// latest session would publish.
#[derive(Debug)]
pub struct MemoryDestination {
    store: Arc<Mutex<Store>>,
}

/// Every published batch of `table` in `store`, in commit order.
pub fn published(store: &str, table: &str) -> Vec<RecordBatch> {
    let store = named(store);
    let store = store.lock();
    store
        .tables
        .get(table)
        .map(|table| table.published.clone())
        .unwrap_or_default()
}

/// The tables of `store`, by name.
pub fn tables(store: &str) -> Vec<String> {
    let store = named(store);
    let store = store.lock();
    store.tables.keys().cloned().collect()
}

/// The rows staged in `table` of `store` and not yet published, whichever session staged them.
pub fn staged(store: &str, table: &str) -> usize {
    let store = named(store);
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
    let store = named(store);
    let store = store.lock();
    store
        .tables
        .get(table)
        .and_then(|table| table.schema.clone())
}

static STORES: LazyLock<Mutex<BTreeMap<String, Arc<Mutex<Store>>>>> = LazyLock::new(Mutex::default);

fn named(name: &str) -> Arc<Mutex<Store>> {
    Arc::clone(STORES.lock().entry(name.to_owned()).or_default())
}

#[derive(Debug, Default)]
struct Store {
    pipelines: BTreeMap<PipelineId, PipelineStore>,
    tables: BTreeMap<String, Table>,
    /// The name of each table the engine has referred to, by logical path.
    names: BTreeMap<TablePath, String>,
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
        let plans = self.plans(pipeline, epoch, meta)?;
        let (mut rows, mut bytes) = (0, 0);
        for table in self.tables.values_mut() {
            for segment in meta.segments.iter() {
                table.staged.remove(&(pipeline.clone(), epoch, segment));
            }
        }
        for (name, staged, merged) in plans {
            let table = self.tables.entry(name).or_default();
            for (generation, batch) in staged {
                rows += batch.num_rows() as u64;
                bytes += batch.get_array_memory_size() as u64;
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
            let Some(name) = self.names.get(path).cloned() else {
                continue;
            };
            let table = self.tables.entry(name).or_default();
            table.published = table.generations.remove(generation).unwrap_or_default();
            table.generations.clear();
            table.tombstones.clear();
        }
        Ok((rows, bytes))
    }

    /// What publishing `meta` does to each table: the batches it staged, and for a merge table
    /// its rows once merged.
    fn plans(&self, pipeline: &PipelineId, epoch: Epoch, meta: &CommitMeta) -> Result<Vec<Plan>> {
        let mut plans = Vec::new();
        for (name, table) in &self.tables {
            let staged: Staged = meta
                .segments
                .iter()
                .filter_map(|segment| table.staged.get(&(pipeline.clone(), epoch, segment)))
                .flatten()
                .cloned()
                .collect();
            // A child table the commit lists follows its root even where it staged nothing.
            let listed = meta
                .child_tables
                .iter()
                .find(|child| *child.table == **name)
                .map(|child| &child.merge);
            if staged.is_empty() && listed.is_none() {
                continue;
            }
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
        meta.segments
            .iter()
            .filter_map(|segment| table.staged.get(&(pipeline.clone(), epoch, segment)))
            .flatten()
            .map(|(_, batch)| batch.clone())
            .collect()
    }

    /// The table `table` refers to, claimed for `pipeline` where no pipeline owns it yet,
    /// recording its name for its path and how it merges; another pipeline's table is refused.
    fn table(&mut self, pipeline: &PipelineId, table: &TableRef) -> Result<&mut Table> {
        let entry = self.tables.entry(table.name.to_string()).or_default();
        let owner = entry.owner.get_or_insert_with(|| pipeline.clone());
        if owner != pipeline {
            return Err(ConnectorError::table_owned(&table.name, owner.as_str()));
        }
        entry.merge.clone_from(&table.merge);
        self.names
            .insert(table.path.clone(), table.name.to_string());
        Ok(entry)
    }
}

#[derive(Debug, Default)]
struct PipelineStore {
    epoch: Epoch,
    state: BTreeMap<String, StateRecord>,
    receipts: BTreeMap<(LoadId, CommitSeq), Receipt>,
}

#[destination(id = "io.rapidbyte.memory", read_back)]
impl DestinationConnector for MemoryDestination {
    type Config = MemoryDestinationConfig;
    type Session = MemorySession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        capabilities.write_modes.replace = true;
        capabilities.write_modes.merge = true;
        capabilities.delete_modes = DeleteModes {
            hard: true,
            soft: true,
        };
        capabilities.partial_updates = true;
        capabilities.merge_changes = true;
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

    async fn connect(config: MemoryDestinationConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            store: named(&config.store),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<MemorySession>> {
        let mut store = self.store.lock();
        let pipeline = store.pipelines.entry(context.pipeline.clone()).or_default();
        pipeline.epoch = pipeline.epoch.next();
        let epoch = pipeline.epoch;
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
        let entry = store.table(&self.pipeline, change.table())?;
        entry.schema = Some(changed(entry.schema.as_ref(), change)?);
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<MemoryWriter> {
        self.store.lock().table(&self.pipeline, table)?;
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
        let pipeline = store
            .pipelines
            .get_mut(&self.pipeline)
            .expect("the pipeline entry was created above");
        for change in &meta.state_delta {
            match change {
                StateChange::Put(record) => {
                    pipeline.state.insert(record.key.clone(), record.clone());
                }
                StateChange::Delete(key) => {
                    pipeline.state.remove(key);
                }
            }
        }
        let receipt = Receipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
            committed_at: SystemTime::now(),
            rows,
            bytes,
        };
        pipeline.receipts.insert(key, receipt.clone());
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
        let table = store.tables.entry(self.table.clone()).or_default();
        let mut stats = WriteStats::default();
        for (segment, batch) in self.buffered.drain(..) {
            stats.rows += batch.num_rows() as u64;
            stats.bytes += batch.get_array_memory_size() as u64;
            table
                .staged
                .entry((self.pipeline.clone(), self.epoch, segment))
                .or_default()
                .push((self.generation, batch));
        }
        Ok(stats)
    }
}

impl ReadBack for MemoryDestination {
    async fn published(&self, table: &TableRef) -> Result<Vec<RecordBatch>> {
        let store = self.store.lock();
        Ok(store
            .tables
            .get(&*table.name)
            .map(|table| table.published.clone())
            .unwrap_or_default())
    }
}

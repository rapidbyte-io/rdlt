//! A transactional destination that keeps tables and state in process memory.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{
    CommitSeq, Epoch, GenerationId, LoadId, MergeKey, PipelineId, SchemaChanges, SegmentId,
    StateChange, StateRecord, TablePath, TypeKind,
};

use crate::columns::changed;
use crate::merge::merge;
use schemars::JsonSchema;
use serde::Deserialize;

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
        let mut plans = Vec::new();
        for (name, table) in &self.tables {
            let staged: Staged = meta
                .segments
                .iter()
                .filter_map(|segment| table.staged.get(&(pipeline.clone(), epoch, segment)))
                .flatten()
                .cloned()
                .collect();
            if staged.is_empty() {
                continue;
            }
            let merged = match &table.merge {
                Some(key) => Some(table.merged(&staged, key)?),
                None => None,
            };
            plans.push((name.clone(), staged, merged));
        }
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
                table.published = merged;
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
        Ok((rows, bytes))
    }

    /// The table `table` refers to, recording its name for its path and how it merges.
    fn table(&mut self, table: &TableRef) -> &mut Table {
        self.names
            .insert(table.path.clone(), table.name.to_string());
        let entry = self.tables.entry(table.name.to_string()).or_default();
        entry.merge.clone_from(&table.merge);
        entry
    }
}

#[derive(Debug, Default)]
struct PipelineStore {
    epoch: Epoch,
    state: BTreeMap<String, StateRecord>,
    receipts: BTreeMap<(LoadId, CommitSeq), Receipt>,
}

#[derive(Debug, Default)]
struct Table {
    schema: Option<TableSchema>,
    /// How the table merges; `None` appends.
    merge: Option<MergeKey>,
    published: Vec<RecordBatch>,
    /// Committed rows of replace generations not yet swapped in.
    generations: BTreeMap<GenerationId, Vec<RecordBatch>>,
    /// Staged batches by the pipeline and epoch of the session that staged them, and segment.
    staged: BTreeMap<(PipelineId, Epoch, SegmentId), Staged>,
}

/// Batches staged under one segment, each for the table itself or for a replace generation.
type Staged = Vec<(Option<GenerationId>, RecordBatch)>;

impl Table {
    /// The table's rows once `staged` is merged in by `key`.
    fn merged(&self, staged: &Staged, key: &MergeKey) -> Result<Vec<RecordBatch>> {
        let schema = match &self.schema {
            Some(schema) => Arc::new(schema.to_arrow()),
            None => staged
                .first()
                .map(|(_, batch)| batch.schema())
                .ok_or_else(|| ConnectorError::internal("merging nothing"))?,
        };
        let incoming: Vec<RecordBatch> = staged.iter().map(|(_, batch)| batch.clone()).collect();
        merge(&schema, &self.published, &incoming, key)
            .map_err(|error| ConnectorError::data(format!("merging rows: {error}")))
    }
}

#[destination(id = "io.rapidbyte.memory")]
impl DestinationConnector for MemoryDestination {
    type Config = MemoryDestinationConfig;
    type Session = MemorySession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        capabilities.write_modes.replace = true;
        capabilities.write_modes.merge = true;
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
        let entry = store.table(change.table());
        entry.schema = Some(changed(entry.schema.as_ref(), change)?);
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<MemoryWriter> {
        self.store.lock().table(table);
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

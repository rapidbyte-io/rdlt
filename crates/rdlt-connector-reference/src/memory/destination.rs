//! A transactional destination that keeps tables and state in process memory.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::{
    CommitSeq, Epoch, LoadId, PipelineId, SegmentId, StateChange, StateRecord, TypeKind,
};
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
/// Commits are atomic under the store's lock, idempotent on `(load_id, commit_seq)` and
/// fenced by the pipeline's epoch.
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

static STORES: LazyLock<Mutex<BTreeMap<String, Arc<Mutex<Store>>>>> = LazyLock::new(Mutex::default);

fn named(name: &str) -> Arc<Mutex<Store>> {
    Arc::clone(STORES.lock().entry(name.to_owned()).or_default())
}

#[derive(Debug, Default)]
struct Store {
    pipelines: BTreeMap<PipelineId, PipelineStore>,
    tables: BTreeMap<String, Table>,
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
    published: Vec<RecordBatch>,
    staged: BTreeMap<(PipelineId, SegmentId), Vec<RecordBatch>>,
}

#[destination(id = "io.rapidbyte.memory")]
impl DestinationConnector for MemoryDestination {
    type Config = MemoryDestinationConfig;
    type Session = MemorySession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
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
        match change {
            TableChange::Create { table, schema } => {
                store
                    .tables
                    .entry(table.name.to_string())
                    .or_default()
                    .schema
                    .get_or_insert_with(|| schema.clone());
                Ok(())
            }
            TableChange::AddColumn { table, .. } | TableChange::Widen { table, .. } => {
                if store.tables.contains_key(table.name.as_ref()) {
                    Ok(())
                } else {
                    Err(ConnectorError::data(format!(
                        "table {} does not exist",
                        table.name
                    )))
                }
            }
        }
    }

    async fn writer(&mut self, table: &TableRef) -> Result<MemoryWriter> {
        Ok(MemoryWriter {
            store: Arc::clone(&self.store),
            pipeline: self.pipeline.clone(),
            table: table.name.to_string(),
            buffered: Vec::new(),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        let mut store = self.store.lock();
        for table in store.tables.values_mut() {
            table
                .staged
                .retain(|(pipeline, _), _| *pipeline != self.pipeline);
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
        let (mut rows, mut bytes) = (0, 0);
        for table in store.tables.values_mut() {
            for segment in meta.segments.iter() {
                for batch in table
                    .staged
                    .remove(&(self.pipeline.clone(), segment))
                    .unwrap_or_default()
                {
                    rows += batch.num_rows() as u64;
                    bytes += batch.get_array_memory_size() as u64;
                    table.published.push(batch);
                }
            }
        }
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
    table: String,
    buffered: Vec<(SegmentId, RecordBatch)>,
}

impl TableWriter for MemoryWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        self.buffered.push((segment, batch));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        let mut store = self.store.lock();
        let table = store.tables.entry(self.table.clone()).or_default();
        let mut stats = WriteStats::default();
        for (segment, batch) in self.buffered.drain(..) {
            stats.rows += batch.num_rows() as u64;
            stats.bytes += batch.get_array_memory_size() as u64;
            table
                .staged
                .entry((self.pipeline.clone(), segment))
                .or_default()
                .push(batch);
        }
        Ok(stats)
    }
}

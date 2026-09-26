//! The simulated destination: transactional commits, receipts, fencing, staging and merges,
//! checking the engine's invariants as it commits.

mod cells;
mod columns;
mod read;
mod store;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::{
    Capabilities, CommitMeta, ConnectContext, ConnectorError, DestinationConnector, Epoch,
    GenerationId, MergeKey, OpenContext, Opened, PipelineId, Receipt, Result, SchemaVersion,
    SegmentId, Session, TableChange, TableRef, TableWriter, WriteStats,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::world::{FaultPoint, World};

pub use cells::{Cells, Stored};
pub use read::completions;
pub(crate) use read::{Published, published_table, table_paths};
pub(crate) use read::{committed_next, reads_in_progress};
pub use store::Digest;
use store::Staged;
pub(crate) use store::Store;

/// Configuration of [`SimDestination`]: the world to write to.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct SimDestinationConfig {
    /// The world's registered name.
    pub world: String,
}

/// Writes to a world's store, with the capabilities the world drew.
#[derive(Debug)]
pub struct SimDestination {
    world: Arc<World>,
}

impl DestinationConnector for SimDestination {
    const ID: &'static str = "io.rapidbyte.sim";
    const VERSION: &'static str = "0.0.0";
    type Config = SimDestinationConfig;
    type Session = SimSession;

    fn capabilities(&self) -> Capabilities {
        self.world.capabilities()
    }

    async fn connect(config: SimDestinationConfig, _context: &ConnectContext) -> Result<Self> {
        let world = World::named(&config.world)
            .ok_or_else(|| ConnectorError::config(format!("no world named {}", config.world)))?;
        Ok(Self { world })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<SimSession>> {
        self.world.latency().await;
        if let Some(fault) = self.world.fault(FaultPoint::Open) {
            return Err(fault);
        }
        let mut store = self.world.store.lock();
        let pipeline = store.pipelines.entry(context.pipeline.clone()).or_default();
        pipeline.epoch = pipeline.epoch.next();
        Ok(Opened {
            session: SimSession {
                world: Arc::clone(&self.world),
                pipeline: context.pipeline.clone(),
                epoch: pipeline.epoch,
            },
            epoch: pipeline.epoch,
            state: pipeline.state.values().cloned().collect(),
        })
    }
}

/// A session of [`SimDestination`], for one pipeline.
#[derive(Debug)]
pub struct SimSession {
    world: Arc<World>,
    pipeline: PipelineId,
    epoch: Epoch,
}

impl Session for SimSession {
    type Writer = SimWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        if let Some(fault) = self.world.fault(FaultPoint::ApplyBefore) {
            return Err(fault);
        }
        let table = change.table();
        {
            let mut store = self.world.store.lock();
            store
                .names
                .insert(table.path.clone(), table.name.to_string());
            let entry = store.tables.entry(table.name.to_string()).or_default();
            columns::apply(&mut entry.columns, change)?;
        }
        match self.world.fault(FaultPoint::ApplyAfter) {
            Some(fault) => Err(fault),
            None => Ok(()),
        }
    }

    async fn writer(&mut self, table: &TableRef) -> Result<SimWriter> {
        if let Some(fault) = self.world.fault(FaultPoint::Writer) {
            return Err(fault);
        }
        let mut store = self.world.store.lock();
        store
            .names
            .insert(table.path.clone(), table.name.to_string());
        Ok(SimWriter {
            world: Arc::clone(&self.world),
            pipeline: self.pipeline.clone(),
            epoch: self.epoch,
            table: table.name.to_string(),
            version: table.version,
            schema: None,
            generation: table.generation,
            merge: table.merge.clone(),
            buffered: Vec::new(),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        let pipeline = &self.pipeline;
        self.world
            .store
            .lock()
            .staged
            .retain(|(staged, _), _| staged != pipeline);
        Ok(())
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        self.world.latency().await;
        if let Some(fault) = self.world.fault(FaultPoint::CommitBefore) {
            return Err(fault);
        }
        let receipt = {
            let mut store = self.world.store.lock();
            let epoch = store.epoch(&self.pipeline);
            if epoch != self.epoch || meta.epoch != self.epoch {
                return Err(ConnectorError::fenced(format!(
                    "pipeline {} is at epoch {epoch}; this session opened at {}",
                    self.pipeline, self.epoch
                )));
            }
            let key = (meta.load_id, meta.commit_seq);
            if let Some(receipt) = store.receipts.get(&key) {
                return Ok(receipt.clone());
            }
            let published = store.publish(&self.pipeline, meta);
            store.apply(&self.world, &self.pipeline, meta);
            store.check_cursors(&self.world, &self.pipeline, &published);
            let receipt = Receipt {
                load_id: meta.load_id,
                commit_seq: meta.commit_seq,
                committed_at: UNIX_EPOCH,
                rows: published.iter().map(|(_, rows)| rows.len() as u64).sum(),
                bytes: 0,
            };
            store.receipts.insert(key, receipt.clone());
            receipt
        };
        match self.world.fault(FaultPoint::CommitAfter) {
            Some(fault) => Err(fault),
            None => Ok(receipt),
        }
    }

    async fn close(self) -> Result<()> {
        match self.world.fault(FaultPoint::Close) {
            Some(fault) => Err(fault),
            None => Ok(()),
        }
    }
}

/// A writer of [`SimDestination`]: buffers rows and stages them on flush.
#[derive(Debug)]
pub struct SimWriter {
    world: Arc<World>,
    pipeline: PipelineId,
    epoch: Epoch,
    table: String,
    /// The schema version the writer's writes follow.
    version: SchemaVersion,
    /// The schema of its first batch, which each of its batches must have: a writer serves one
    /// version, and every batch lowered for a version has the version's schema.
    schema: Option<SchemaRef>,
    generation: Option<GenerationId>,
    merge: Option<MergeKey>,
    buffered: Vec<(SegmentId, Vec<Stored>)>,
}

impl TableWriter for SimWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        if let Some(fault) = self.world.fault(FaultPoint::Write) {
            return Err(fault);
        }
        let unfit = {
            let store = self.world.store.lock();
            let held = store.tables.get(&self.table).map(|table| &table.columns);
            columns::unfit(held.unwrap_or(&columns::Columns::new()), &batch)
        };
        let first = self.schema.get_or_insert_with(|| batch.schema());
        if *first != batch.schema() {
            self.world.violation(format!(
                "table {}: a writer of version {} wrote batches of two schemas",
                self.table, self.version.0
            ));
        }
        for finding in unfit {
            self.world
                .violation(format!("table {}: {finding}", self.table));
        }
        self.buffered.push((segment, cells::rows(&batch)?));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        self.world.latency().await;
        if let Some(fault) = self.world.fault(FaultPoint::Flush) {
            return Err(fault);
        }
        let mut store = self.world.store.lock();
        if store.epoch(&self.pipeline) != self.epoch {
            return Err(ConnectorError::fenced("a newer session holds the pipeline"));
        }
        let mut stats = WriteStats::default();
        for (segment, rows) in self.buffered.drain(..) {
            stats.rows += rows.len() as u64;
            let key = (self.pipeline.clone(), segment);
            store.staged.entry(key).or_default().push(Staged {
                table: self.table.clone(),
                generation: self.generation,
                merge: self.merge.clone(),
                rows,
            });
        }
        Ok(stats)
    }
}

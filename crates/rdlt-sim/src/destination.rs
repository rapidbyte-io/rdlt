//! The simulated destination: transactional commits answered by the key that landed last,
//! fencing, staging and merges, checking the engine's invariants as it commits; a try it answers
//! transiently may still land later.

mod cells;
mod columns;
mod merged;
mod read;
mod running;
mod store;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use rdlt_connector::limits::ListenLimits;
use rdlt_connector::{
    Capabilities, CommitMeta, ConnectContext, ConnectorError, ConnectorErrorKind,
    DestinationConnector, Epoch, GenerationId, MergeKey, OpenContext, Opened, PipelineId, Receipt,
    Result, SchemaVersion, SegmentId, Session, TableChange, TableRef, TableWriter, WriteStats,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::owners::Live;
use crate::world::{FaultPoint, World};

pub(crate) use cells::WRITTEN;
pub use cells::{Cells, Stored};
pub use read::completions;
pub(crate) use read::{Published, published_table, table_paths};
pub(crate) use read::{committed_next, reads_in_progress, records};
pub use store::Digest;
use store::Staged;
pub(crate) use store::Store;

/// Configuration of [`SimDestination`]: the world to write to, and the secrets it was told.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct SimDestinationConfig {
    /// The world's registered name.
    pub world: String,
    /// The secrets its host resolved; a host that resolves nothing tells none.
    ///
    /// Left out of the schema: every handshake carries the schema over the simulated network,
    /// and bytes added there change what a seed's network faults do.
    #[schemars(skip)]
    pub told: Option<crate::world::Told>,
}

/// Writes to a world's store, with the capabilities the world drew.
#[derive(Debug)]
pub struct SimDestination {
    world: Arc<World>,
    /// The sessions it holds now, each from before it opens until its close ends: those of one
    /// connection, where it is served.
    sessions: Arc<AtomicUsize>,
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
        world.heard.hear(config.told.as_ref());
        Ok(Self {
            world,
            sessions: Arc::default(),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, context: &OpenContext) -> Result<Opened<SimSession>> {
        // A session counts from before its destination opens it, as a served connection
        // counts it, so an open in flight counts too.
        let held = Live::of(&[&self.sessions, &self.world.owners.sessions]);
        let holding = self.sessions.load(Ordering::SeqCst);
        let bound = ListenLimits::default().connection_sessions();
        if holding > bound {
            self.world.violation(format!(
                "a destination held {holding} sessions at once, beyond the {bound} one \
                 connection may hold"
            ));
        }
        self.world.latency().await;
        self.world.hang(FaultPoint::Open).await;
        if let Some(fault) = self.world.fault(FaultPoint::Open) {
            return Err(fault);
        }
        let mut store = self.world.store.lock();
        let pipeline = store.pipelines.entry(context.pipeline.clone()).or_default();
        let Some(epoch) = pipeline.epoch.next() else {
            return Err(ConnectorError::data(format!(
                "pipeline {} holds the last epoch there is",
                context.pipeline
            )));
        };
        pipeline.epoch = epoch;
        Ok(Opened {
            session: SimSession {
                world: Arc::clone(&self.world),
                pipeline: context.pipeline.clone(),
                epoch: pipeline.epoch,
                _held: held,
            },
            epoch: pipeline.epoch,
            landed: pipeline.landed.clone(),
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
    /// Counts it among its destination's sessions and its world's, until it is dropped, which
    /// a close does once it ends.
    _held: Live,
}

impl Session for SimSession {
    type Writer = SimWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        self.world.hang(FaultPoint::ApplyBefore).await;
        if let Some(fault) = self.world.fault(FaultPoint::ApplyBefore) {
            return Err(fault);
        }
        let table = change.table();
        {
            let mut store = self.world.store.lock();
            let entry = store.claim(&self.pipeline, self.epoch, table)?;
            columns::apply(&mut entry.columns, change)?;
        }
        self.world.hang(FaultPoint::ApplyAfter).await;
        match self.world.fault(FaultPoint::ApplyAfter) {
            Some(fault) => Err(fault),
            None => Ok(()),
        }
    }

    async fn writer(&mut self, table: &TableRef) -> Result<SimWriter> {
        self.world.hang(FaultPoint::Writer).await;
        if let Some(fault) = self.world.fault(FaultPoint::Writer) {
            return Err(fault);
        }
        self.world
            .store
            .lock()
            .claim(&self.pipeline, self.epoch, table)?;
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
            _held: Live::of(&[&self.world.owners.writers]),
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
        self.world.hang(FaultPoint::CommitBefore).await;
        running::begun(&self.world, &self.pipeline);
        if let Some(fault) = self.world.fault(FaultPoint::CommitBefore) {
            if fault.kind() == ConnectorErrorKind::Transient
                && let Some(outlasts) = self.world.outlasts()
            {
                let mut store = self.world.store.lock();
                store.leave_running(&self.pipeline, self.epoch, meta, outlasts);
            }
            return Err(fault);
        }
        let landed = {
            let mut store = self.world.store.lock();
            land(&self.world, &mut store, &self.pipeline, self.epoch, meta)
        };
        running::ended(&self.world, &self.pipeline);
        let receipt = landed?;
        self.world.hang(FaultPoint::CommitAfter).await;
        match self.world.fault(FaultPoint::CommitAfter) {
            Some(fault) => Err(fault),
            None => Ok(receipt),
        }
    }

    async fn close(self) -> Result<()> {
        self.world.hang(FaultPoint::Close).await;
        match self.world.fault(FaultPoint::Close) {
            Some(fault) => Err(fault),
            None => Ok(()),
        }
    }
}

/// Lands `meta`, a commit of `pipeline`'s session of `epoch`, in `store`, answered as a commit is
/// by the key that landed last: the receipt of the key landed, refused where it is fenced or
/// stale, or applied whole.
fn land(
    world: &World,
    store: &mut Store,
    pipeline: &PipelineId,
    epoch: Epoch,
    meta: &CommitMeta,
) -> Result<Receipt> {
    if let Some(landed) = store.fence(pipeline).decide(meta)? {
        return Ok(landed);
    }
    if meta.key.epoch != epoch {
        return Err(ConnectorError::fenced(format!(
            "a session of epoch {epoch} cannot apply {}",
            meta.key
        )));
    }
    store.owned(pipeline, meta)?;
    // Where the destination held each partition before this commit.
    let held = store.positions(pipeline);
    let (published, reached) = store.publish(pipeline, meta)?;
    world.reach_all(reached);
    store.apply(world, pipeline, meta);
    store.check_cursors(world, pipeline, &published, &held);
    let receipt = Receipt {
        key: meta.key,
        committed_at: UNIX_EPOCH,
        rows: published.iter().map(|(_, rows)| rows.len() as u64).sum(),
        bytes: 0,
    };
    store.pipelines.entry(pipeline.clone()).or_default().landed = Some(receipt.clone());
    Ok(receipt)
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
    /// Counts it among its world's writers until it is dropped.
    _held: Live,
}

impl TableWriter for SimWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        self.world.hang(FaultPoint::Write).await;
        if let Some(fault) = self.world.fault(FaultPoint::Write) {
            return Err(fault);
        }
        let unfit = {
            let store = self.world.store.lock();
            let held = store.tables.get(&self.table).map(|table| &table.columns);
            // A newer session of the pipeline dropped the table: this session is fenced.
            if held.is_none() && store.epoch(&self.pipeline) != self.epoch {
                return Err(ConnectorError::fenced(format!(
                    "table {} was dropped by a newer session",
                    self.table
                )));
            }
            columns::unfit(held.unwrap_or(&columns::Columns::new()), &batch)
        };
        let first = self.schema.get_or_insert_with(|| batch.schema());
        if *first != batch.schema() {
            self.world.violation(format!(
                "table {}: a writer of version {} wrote batches of two schemas",
                self.table, self.version.0
            ));
        }
        // A change stream's written batches carry the columns that direct its merge, which no
        // schema change names.
        let directs = |finding: &String| {
            self.merge
                .as_ref()
                .and_then(|key| key.changes.as_ref())
                .is_some_and(|changes| {
                    let named = |column: &str| finding.contains(&format!("column {column},"));
                    named(&changes.op) || changes.unchanged.as_deref().is_some_and(named)
                })
        };
        for finding in unfit.into_iter().filter(|finding| !directs(finding)) {
            self.world
                .violation(format!("table {}: {finding}", self.table));
        }
        self.buffered.push((segment, cells::rows(&batch)?));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        self.world.latency().await;
        self.world.hang(FaultPoint::Flush).await;
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

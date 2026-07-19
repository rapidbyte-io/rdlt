//! The loader: drives one `LoadSession` through ensure → write → commit, owns the
//! run's accounting (no silent failures), and applies the `CommitPolicy`.
//!
//! Commits happen ONLY at checkpoint boundaries (plus one final commit for trailing
//! work): committing mid-span would publish rows the committed cursor doesn't cover,
//! and a crash would then re-extract them as duplicates (recovery invariant 2).

use std::time::Instant;

pub(crate) mod lowering;

use rdlt_connector::{DestCapabilities, LoadSession, RecordBatch};
use rdlt_core::{
    CommitCounters, CommitMeta, CommitPolicy, Cursor, LoadId, RdltError, RunReport, SchemaDelta,
    StateDoc, StreamName, TableName, TableSchema, WriteMode,
};

use crate::runtime::channel::ByteSized;
use crate::wal::Wal;

/// One unit of work flowing shred → load. Per-table order within the channel is the
/// ordering guarantee (delta before first batch at the new version).
#[derive(Debug)]
pub(crate) enum LoadItem {
    Delta {
        schema: TableSchema,
        delta: SchemaDelta,
        mode: WriteMode,
    },
    Batch {
        table: TableName,
        batch: RecordBatch,
    },
    /// A source checkpoint: rows pushed before this are complete up to `cursor`.
    Checkpoint { stream: StreamName, cursor: Cursor },
    /// Policy-driven discards — counted, never silent (spec FR-010/FR-012).
    Discarded {
        table: TableName,
        rows: u64,
        values: u64,
    },
}

impl ByteSized for LoadItem {
    fn byte_size(&self) -> usize {
        match self {
            LoadItem::Batch { batch, .. } => batch.get_array_memory_size(),
            LoadItem::Delta { .. } | LoadItem::Checkpoint { .. } | LoadItem::Discarded { .. } => 0,
        }
    }
}

pub(crate) struct Loader {
    session: Box<dyn LoadSession>,
    pub(crate) report: RunReport,
    /// The evolving pipeline state; every commit persists a snapshot of it.
    state: StateDoc,
    load_id: LoadId,
    policy: CommitPolicy,
    counters: CommitCounters,
    commit_seq: u64,
    checkpoints_since_commit: u32,
    bytes_since_commit: u64,
    last_commit_at: Instant,
    /// Anything (rows, cursors, schemas) not yet covered by a commit.
    dirty: bool,
    /// Write-ahead log; `None` when no workdir is configured (recovery then always
    /// degrades to cursor re-extraction — slower, never wrong).
    wal: Option<Wal>,
    events: tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
    /// Destination capabilities drive lowering at this seam (design doc §5.3).
    caps: DestCapabilities,
}

impl Loader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session: Box<dyn LoadSession>,
        report: RunReport,
        base_state: StateDoc,
        load_id: LoadId,
        policy: CommitPolicy,
        wal: Option<Wal>,
        events: tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
        caps: DestCapabilities,
    ) -> Self {
        Self {
            session,
            report,
            state: base_state,
            load_id,
            policy,
            counters: CommitCounters::default(),
            commit_seq: 0,
            checkpoints_since_commit: 0,
            bytes_since_commit: 0,
            last_commit_at: Instant::now(),
            dirty: false,
            wal,
            events,
            caps,
        }
    }

    fn emit(&self, event: rdlt_core::PipelineEvent) {
        let _ = self.events.send(event); // no listeners is fine
    }

    pub(crate) async fn process(&mut self, item: LoadItem) -> Result<(), RdltError> {
        let span = tracing::info_span!("rdlt.load");
        let _guard = span.enter();
        // Write-ahead: the item is durable-intent before the destination sees it.
        if let Some(wal) = &mut self.wal {
            wal.record(&item)?;
        }
        match item {
            LoadItem::Delta {
                schema,
                delta,
                mode,
            } => {
                // Lowering at the destination seam: flatten/downcast for the
                // destination's capabilities; the engine keeps the rich schema.
                let lowered = lowering::lower_schema(&schema, &self.caps);
                self.session
                    .ensure_table(&lowered, &mode)
                    .await
                    .map_err(RdltError::destination)?;
                self.state
                    .schema_hashes
                    .insert(schema.table.clone(), schema.content_hash());
                self.emit(rdlt_core::PipelineEvent::SchemaEvolved {
                    delta: delta.clone(),
                });
                self.report.schema_migrations.push(delta);
                self.dirty = true;
            }
            LoadItem::Batch { table, batch } => {
                let rows = batch.num_rows() as u64;
                let bytes = batch.get_array_memory_size() as u64;
                let lowered = lowering::lower_batch(&batch, &self.caps)?;
                self.session
                    .write(&table, lowered)
                    .await
                    .map_err(RdltError::destination)?;
                self.emit(rdlt_core::PipelineEvent::BatchLoaded {
                    table: table.clone(),
                    rows,
                    bytes,
                });
                let entry = self.report.table_mut(&table);
                entry.rows += rows;
                entry.bytes += bytes;
                self.counters.rows += rows;
                self.counters.bytes += bytes;
                self.bytes_since_commit += bytes;
                self.dirty = true;
            }
            LoadItem::Checkpoint { stream, cursor } => {
                self.state.cursors.insert(stream.clone(), cursor.clone());
                self.report.cursors.insert(stream, cursor);
                self.checkpoints_since_commit += 1;
                self.dirty = true;
                // Commit decisions are made only here — a checkpoint boundary.
                if self.policy_triggers() {
                    self.commit().await?;
                }
            }
            LoadItem::Discarded {
                table,
                rows,
                values,
            } => {
                self.emit(rdlt_core::PipelineEvent::Discarded {
                    table: table.clone(),
                    rows,
                    values,
                    reason: "schema policy".to_owned(),
                });
                let entry = self.report.table_mut(&table);
                entry.discarded_rows += rows;
                entry.discarded_values += values;
                self.counters.discarded_rows += rows;
                self.counters.discarded_values += values;
            }
        }
        Ok(())
    }

    fn policy_triggers(&self) -> bool {
        match self.policy {
            CommitPolicy::EveryCheckpoints(n) => self.checkpoints_since_commit >= n.max(1),
            CommitPolicy::EveryBytes(bytes) => self.bytes_since_commit >= bytes,
            CommitPolicy::EverySeconds(secs) => {
                self.last_commit_at.elapsed().as_secs() >= u64::from(secs)
            }
        }
    }

    /// Trailing work (rows after the last checkpoint, or a run that never
    /// checkpointed) gets one final commit; a clean no-op run still commits once so a
    /// fresh pipeline's state document exists.
    pub(crate) async fn finish(&mut self) -> Result<(), RdltError> {
        if self.dirty || self.commit_seq == 0 {
            self.commit().await?;
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), RdltError> {
        self.commit_seq += 1;
        self.state.last_commit = Some(rdlt_core::LastCommit {
            load_id: self.load_id.clone(),
            commit_seq: self.commit_seq,
        });
        // Commit protocol step 1: the WAL span becomes durable BEFORE the
        // destination commit — a crash after this point replays instead of
        // re-extracting (crash-matrix row 2).
        if let Some(wal) = &mut self.wal {
            wal.sync_for_commit()?;
        }
        let meta = CommitMeta {
            load_id: self.load_id.clone(),
            commit_seq: self.commit_seq,
            state: self.state.clone(),
            counters: std::mem::take(&mut self.counters),
        };
        self.session
            .commit(meta)
            .await
            .map_err(RdltError::destination)?;
        // Step 3: receipt in hand — mark and reclaim covered segments.
        if let Some(wal) = &mut self.wal {
            wal.mark_committed(self.commit_seq)?;
        }
        self.emit(rdlt_core::PipelineEvent::Committed {
            commit_seq: self.commit_seq,
            cursors: self.state.cursors.clone(),
        });
        self.report.commits += 1;
        self.checkpoints_since_commit = 0;
        self.bytes_since_commit = 0;
        self.last_commit_at = Instant::now();
        self.dirty = false;
        Ok(())
    }
}

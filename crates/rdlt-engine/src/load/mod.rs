//! The loader: drives one `LoadSession` through ensure → write → commit, owns the
//! run's accounting (no silent failures), and applies the `CommitPolicy`.
//!
//! Commits happen ONLY at checkpoint boundaries (plus one final commit for trailing
//! work): committing mid-span would publish rows the committed cursor doesn't cover,
//! and a crash would then re-extract them as duplicates.

use std::time::Instant;

pub(crate) mod apply;
pub(crate) mod lowering;

use rdlt_connector::{DestCapabilities, LoadSession, RecordBatch};
use rdlt_core::{
    CommitCounters, CommitMeta, CommitPolicy, Cursor, LoadId, RdltError, RunReport, SchemaDelta,
    StateDoc, StreamName, TableName, TableSchema, WriteMode,
};

use crate::runtime::channel::ByteSized;
use crate::wal::Wal;
use rdlt_core::crash_point;

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
    /// Policy-driven discards — counted, never silent.
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

/// The destination and how to lower for it — the two are always used together at
/// the write seam (`apply_delta`/`apply_batch` take exactly this pair), so they
/// travel as one.
pub(crate) struct Sink {
    pub(crate) session: Box<dyn LoadSession>,
    pub(crate) caps: DestCapabilities,
}

pub(crate) struct Loader {
    sink: Sink,
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
    /// Keyed structured merge: tables whose write mode is Merge
    /// and whose schema carries NO per-row identity — their key columns must
    /// never be NULL (keys are identities; validated per batch).
    structured_merge_keys: std::collections::BTreeMap<TableName, Vec<String>>,
}

impl Loader {
    pub(crate) fn new(
        sink: Sink,
        report: RunReport,
        base_state: StateDoc,
        load_id: LoadId,
        policy: CommitPolicy,
        wal: Option<Wal>,
        events: tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
    ) -> Self {
        Self {
            sink,
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
            structured_merge_keys: std::collections::BTreeMap::new(),
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
                // Lowering + ensure + hash-record at the destination seam, shared
                // with WAL replay so recovery reproduces this exactly.
                apply::apply_delta(
                    &mut *self.sink.session,
                    &mut self.state,
                    &self.sink.caps,
                    &schema,
                    &mode,
                )
                .await?;
                crash_point!(
                    "session.after_ensure",
                    Err(RdltError::config(
                        "injected crash after ensure_table (failpoint)",
                    ))
                );
                // Track keyed STRUCTURED merges (no `_rdlt_id` column ⇒ the
                // stream is structured): batches must carry non-NULL keys.
                if let rdlt_core::WriteMode::Merge { key } = &mode
                    && !schema
                        .columns
                        .iter()
                        .any(|c| c.name == rdlt_core::schema::system_columns::ID)
                {
                    self.structured_merge_keys
                        .insert(schema.table.clone(), key.clone());
                }
                self.emit(rdlt_core::PipelineEvent::SchemaEvolved {
                    delta: delta.clone(),
                });
                self.report.schema_migrations.push(delta);
                self.dirty = true;
            }
            LoadItem::Batch { table, batch } => {
                // Keyed structured merge (006): merge keys are identities —
                // a NULL key is a typed error, never a silent mis-merge.
                if let Some(keys) = self.structured_merge_keys.get(&table) {
                    for key in keys {
                        let column = batch.column_by_name(key).ok_or_else(|| {
                            RdltError::config(format!(
                                "merge key `{key}` is not a column of table `{table}`"
                            ))
                        })?;
                        if column.null_count() > 0 {
                            return Err(RdltError::config(format!(
                                "merge key `{key}` contains NULLs in table `{table}` — \
                                 merge keys are identities"
                            )));
                        }
                    }
                }
                let rows = batch.num_rows() as u64;
                let bytes = batch.get_array_memory_size() as u64;
                apply::apply_batch(&mut *self.sink.session, &self.sink.caps, &table, &batch)
                    .await?;
                crash_point!(
                    "session.after_write",
                    Err(RdltError::config("injected crash after write (failpoint)",))
                );
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
        // re-extracting.
        if let Some(wal) = &mut self.wal {
            wal.sync_for_commit()?;
        }
        let meta = CommitMeta {
            load_id: self.load_id.clone(),
            commit_seq: self.commit_seq,
            state: self.state.clone(),
            counters: std::mem::take(&mut self.counters),
        };
        self.sink
            .session
            .commit(meta)
            .await
            .map_err(RdltError::destination)?;
        // The canonical redelivery window: destination acknowledged, WAL not yet
        // marked — a crash here MUST replay idempotently (D3).
        crash_point!(
            "session.after_commit",
            Err(RdltError::config(
                "injected crash after destination commit (failpoint)",
            ))
        );
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

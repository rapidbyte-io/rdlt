//! The loader: drives one `LoadSession` through ensure → write → commit, owns the
//! run's accounting (no silent failures), and applies the `CommitPolicy`.
//!
//! Commits happen ONLY at checkpoint boundaries (plus one final commit for trailing
//! work): committing mid-span would publish rows the committed cursor doesn't cover,
//! and a crash would then re-extract them as duplicates.

pub(crate) mod apply;
pub(crate) mod lowering;

use std::time::Instant;

use rdlt_connector::{DestinationCapabilities, LoadSession, RecordBatch, channel::ByteSized};
use rdlt_core::{
    CommitCounters, CommitMeta, CommitPolicy, Cursor, LoadId, RdltError, RunReport, SchemaDelta,
    StateDoc, StreamName, TableName, TableSchema, WriteMode, crash_point,
};

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
    pub(crate) caps: DestinationCapabilities,
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
        // No `enter()` guard: this function awaits, and a guard held across an
        // await stays on the worker thread's span stack while other tasks run
        // there. The loader is a single task, so the span is bound to its future
        // at the call site instead.
        // Write-ahead: the item is durable-intent before the destination sees it.
        if let Some(wal) = &mut self.wal {
            wal.record(&item).await?;
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
                // Keyed structured merge: merge keys are identities — a NULL
                // key is a typed error, never a silent mis-merge.
                //
                // This check runs AFTER the item has been recorded to the
                // recovery log, so a rejected batch is already on disk, and
                // replay does not repeat the check. That is safe only because
                // the rejection aborts the run before any checkpoint: a span
                // with no checkpoint is not replayable, so the bad batch is
                // discarded rather than replayed past its guard. Any change
                // that lets this path reach a checkpoint must move the check
                // ahead of the recovery-log write.
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
            wal.sync_for_commit().await?;
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
            .map_err(|e| crate::runtime::classify_dest_error(&e))?;
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
            wal.mark_committed(self.commit_seq).await?;
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
// Inline rather than in `tests/`: a child module can read `Loader`'s private
// fields directly, which these pins need.

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_connector::channel::{Permitted, byte_channel};

    use crate::runtime::STAGE_MSG_CAPACITY;
    use rdlt_core::{PipelineId, StreamName};
    use std::sync::Arc;
    use std::time::Duration;

    fn batch_of(rows: usize) -> RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let array = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(array)],
        )
        .expect("batch")
    }

    /// `LoadItem::byte_size` has exactly ONE consumer — the stage channel's
    /// permit request — so its only observable consequence is backpressure.
    /// Nothing about the run report can detect a wrong answer: `table.bytes` is
    /// read straight off the batch in `process`, not through this trait. A
    /// constant 0 therefore removes backpressure entirely (peak memory would
    /// scale with input instead of staying capped) while every counter stays
    /// correct, which is why the mutant survived a suite that asserts counters.
    #[tokio::test]
    async fn byte_size_is_what_makes_backpressure_real() {
        let batch = batch_of(64);
        let size = batch.get_array_memory_size();
        assert!(size > 0, "a real batch occupies memory");

        let (tx, mut rx) = byte_channel::<LoadItem>(size, STAGE_MSG_CAPACITY);
        tx.send(LoadItem::Batch {
            table: TableName::new("t"),
            batch: batch.clone(),
        })
        .await
        .expect("a batch at exactly the budget sends");

        // Budget exhausted: the next batch MUST park. Under `byte_size → 0` it
        // sails through; under `→ 1` it also sails through (1 of `size` used).
        let parked = tokio::time::timeout(
            Duration::from_millis(100),
            tx.send(LoadItem::Batch {
                table: TableName::new("t"),
                batch: batch.clone(),
            }),
        )
        .await;
        assert!(
            parked.is_err(),
            "the byte budget is exhausted — the producer must park, which IS the backpressure"
        );

        // Receiving the first item releases its permit, and the parked send completes.
        let received = rx
            .recv()
            .await
            .map(Permitted::into_value)
            .expect("first item");
        assert!(matches!(received, LoadItem::Batch { .. }));
        tokio::time::timeout(
            Duration::from_secs(5),
            tx.send(LoadItem::Batch {
                table: TableName::new("t"),
                batch,
            }),
        )
        .await
        .expect("recv released the budget, so this send must complete")
        .expect("channel still open");
    }

    /// Markers carry no rows and must never be gated by the byte budget: a
    /// checkpoint that could not enqueue would stall committing forever. This is
    /// the other half of the `byte_size` pin — under a constant 1 this send
    /// never completes on a zero budget.
    #[tokio::test]
    async fn zero_sized_markers_pass_a_zero_budget() {
        let (tx, mut rx) = byte_channel::<LoadItem>(0, STAGE_MSG_CAPACITY);
        for item in [
            LoadItem::Checkpoint {
                stream: StreamName::new("s"),
                cursor: Cursor::new(serde_json::json!({"b": 1})),
            },
            LoadItem::Discarded {
                table: TableName::new("t"),
                rows: 1,
                values: 2,
            },
        ] {
            tokio::time::timeout(Duration::from_secs(5), tx.send(item))
                .await
                .expect("a zero-byte marker must not wait on the byte budget")
                .expect("channel still open");
        }
        assert!(matches!(
            rx.recv().await.map(Permitted::into_value),
            Some(LoadItem::Checkpoint { .. })
        ));
        assert!(matches!(
            rx.recv().await.map(Permitted::into_value),
            Some(LoadItem::Discarded { .. })
        ));
    }

    /// `policy_triggers` is a pure function of the loader's counters and clock;
    /// no destination call is reachable from it, and this session asserts that by
    /// refusing every call.
    struct UnusedSession;

    #[async_trait::async_trait]
    impl LoadSession for UnusedSession {
        async fn ensure_table(
            &mut self,
            _: &TableSchema,
            _: &WriteMode,
        ) -> Result<(), rdlt_connector::DestinationError> {
            unreachable!("policy_triggers never touches the destination")
        }
        async fn write(
            &mut self,
            _: &TableName,
            _: RecordBatch,
        ) -> Result<(), rdlt_connector::DestinationError> {
            unreachable!("policy_triggers never touches the destination")
        }
        async fn commit(
            &mut self,
            _: CommitMeta,
        ) -> Result<rdlt_core::CommitReceipt, rdlt_connector::DestinationError> {
            unreachable!("policy_triggers never touches the destination")
        }
        async fn read_state(
            &mut self,
            _: &PipelineId,
        ) -> Result<Option<StateDoc>, rdlt_connector::DestinationError> {
            unreachable!("policy_triggers never touches the destination")
        }
    }

    fn loader_with(policy: CommitPolicy) -> Loader {
        let (events, _rx) = tokio::sync::broadcast::channel(16);
        let pipeline = PipelineId::new("p");
        let load_id = LoadId::new("l");
        Loader::new(
            Sink {
                session: Box::new(UnusedSession),
                caps: DestinationCapabilities::default(),
            },
            RunReport::new(pipeline.clone(), load_id.clone()),
            StateDoc::new(pipeline, "test"),
            load_id,
            policy,
            None,
            events,
        )
    }

    /// The time-based policy compares ELAPSED seconds against the threshold, so
    /// it is pinned by back-dating `last_commit_at` rather than by sleeping:
    /// wide margins on both sides, no clock control, no flakiness. `>=` → `<`
    /// inverts the comparison, which either commits on every checkpoint or never
    /// commits at all — both caught here.
    #[test]
    fn every_seconds_boundary_fires_only_past_the_threshold() {
        let mut loader = loader_with(CommitPolicy::EverySeconds(30));
        loader.last_commit_at = Instant::now() - Duration::from_secs(1);
        assert!(
            !loader.policy_triggers(),
            "1s elapsed against a 30s policy must not commit"
        );
        loader.last_commit_at = Instant::now() - Duration::from_secs(600);
        assert!(
            loader.policy_triggers(),
            "600s elapsed against a 30s policy must commit"
        );
    }

    #[test]
    fn checkpoint_and_byte_policy_boundaries_are_exact() {
        let mut loader = loader_with(CommitPolicy::EveryCheckpoints(2));
        loader.checkpoints_since_commit = 1;
        assert!(!loader.policy_triggers(), "one short of the threshold");
        loader.checkpoints_since_commit = 2;
        assert!(loader.policy_triggers(), "at the threshold, not past it");

        // `n.max(1)` normalizes a zero threshold to one, and what that actually
        // buys is the ZERO-accumulation case: `0 >= 1` is false where `0 >= 0`
        // would be true. Today `policy_triggers` is only ever called just after
        // `checkpoints_since_commit += 1`, so that state is unreachable from the
        // one call site — which is precisely why the mutant survives a test that
        // only checks a nonzero count. Pinned as the function's own contract:
        // with nothing accumulated there is nothing to commit, and a commit
        // covering no new checkpoint would publish a cursor it does not own.
        let mut loader = loader_with(CommitPolicy::EveryCheckpoints(0));
        loader.checkpoints_since_commit = 0;
        assert!(
            !loader.policy_triggers(),
            "no checkpoints accumulated: nothing to commit, even at threshold 0"
        );
        loader.checkpoints_since_commit = 1;
        assert!(
            loader.policy_triggers(),
            "EveryCheckpoints(0) behaves as 1, so one checkpoint commits"
        );

        let mut loader = loader_with(CommitPolicy::EveryBytes(100));
        loader.bytes_since_commit = 99;
        assert!(!loader.policy_triggers(), "one byte short");
        loader.bytes_since_commit = 100;
        assert!(loader.policy_triggers(), "at the threshold");
    }
}

//! The loader: drives one `LoadSession` through ensure → write → commit, owns the
//! run's accounting (no silent failures), and applies the `CommitPolicy`.
//!
//! Commits happen ONLY at COVERED checkpoint boundaries (plus one final commit
//! for trailing work): a boundary where every stream with rows in the unit has
//! a checkpoint of its own. Committing anything less would publish rows the
//! committed cursors don't cover, and a crash would then re-extract them as
//! duplicates — a checkpoint of ANOTHER stream covers nothing (042 T7E), so a
//! trigger that fires there defers to a later, covered one.

use std::time::Instant;

use rdlt_connector::{DestinationCapabilities, LoadSession, RecordBatch};
use rdlt_core::{
    CommitCounters, CommitMeta, CommitPolicy, LoadId, RdltError, RunReport, StateDoc, TableName,
    crash_point,
};

use crate::wal::Wal;

use super::{apply, item::LoadItem};

/// The destination and how to lower for it — the two are always used together at
/// the write seam (`apply_delta`/`apply_batch` take exactly this pair), so they
/// travel as one.
pub(crate) struct Sink {
    pub(crate) session: Box<dyn LoadSession>,
    pub(crate) capabilities: DestinationCapabilities,
}

pub(crate) struct Loader {
    sink: Sink,
    pub(crate) report: RunReport,
    /// The evolving pipeline state; every commit persists a snapshot of it.
    state: StateDoc,
    load_id: LoadId,
    policy: CommitPolicy,
    /// How much to accumulate before each destination write. The
    /// default writes straight through.
    batch_policy: rdlt_core::BatchPolicy,
    /// Rows waiting to be written, per table.
    ///
    /// Keyed by TABLE because a batch belongs to one, and Arrow
    /// concatenation requires a single schema — two tables' rows
    /// could never be one write.
    pending: std::collections::BTreeMap<TableName, Pending>,
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
    /// Child table → parent table, from the Delta records (delta precedes a
    /// table's first batch, so the chain exists before it is walked). What
    /// resolves a written table to the ROOT its stream owns — the names alone
    /// cannot (`child_table_name` truncates and hash-suffixes long names).
    parents: std::collections::BTreeMap<TableName, TableName>,
    /// Root tables with rows written since their own stream's last
    /// checkpoint. A commit issued while this is non-empty would publish
    /// rows NO cursor covers: after a crash the recovered state cannot
    /// advance those streams, re-extraction re-delivers the rows, and an
    /// append destination has nothing to dedup on. Mid-run commits wait for
    /// this to drain (042 T7E — the loader half of per-stream coverage).
    uncovered_roots: std::collections::BTreeSet<TableName>,
    /// How many times the deferral advisory fired. The warn site fires
    /// only at 0, so the count is the exactly-once-per-run pin: a
    /// blocked commit trigger is worth ONE operator warning, not one
    /// per checkpoint.
    deferred_commit_warnings: u32,
}

/// The two cadences the loader obeys, passed together because they
/// are read together and interact: a batch never spans a commit.
pub(crate) struct Policies {
    /// When a commit unit closes.
    pub(crate) commit: CommitPolicy,
    /// How much accumulates before each destination write.
    pub(crate) batch: rdlt_core::BatchPolicy,
}

/// Rows accumulated for one table, waiting for a threshold.
struct Pending {
    batches: Vec<RecordBatch>,
    rows: u64,
    bytes: u64,
}

impl Loader {
    pub(crate) fn new(
        sink: Sink,
        report: RunReport,
        base_state: StateDoc,
        load_id: LoadId,
        policies: Policies,
        wal: Option<Wal>,
        events: tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
    ) -> Self {
        Self {
            sink,
            report,
            state: base_state,
            load_id,
            policy: policies.commit,
            batch_policy: policies.batch,
            pending: std::collections::BTreeMap::new(),
            counters: CommitCounters::default(),
            commit_seq: 0,
            checkpoints_since_commit: 0,
            bytes_since_commit: 0,
            last_commit_at: Instant::now(),
            dirty: false,
            wal,
            events,
            structured_merge_keys: std::collections::BTreeMap::new(),
            parents: std::collections::BTreeMap::new(),
            uncovered_roots: std::collections::BTreeSet::new(),
            deferred_commit_warnings: 0,
        }
    }

    /// A written table's ROOT, along the parent links its Deltas recorded; a
    /// table with no recorded parent is its own root. Bounded walk — more
    /// hops than known links would mean a cycle, which no shred produces.
    fn root_of(&self, table: &TableName) -> TableName {
        let mut current = table;
        for _ in 0..=self.parents.len() {
            match self.parents.get(current) {
                Some(parent) => current = parent,
                None => break,
            }
        }
        current.clone()
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
                    &self.sink.capabilities,
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
                if let Some(link) = &schema.parent {
                    self.parents
                        .insert(schema.table.clone(), link.parent.clone());
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
                if self.batch_policy.accumulates() {
                    self.accumulate(&table, batch).await?;
                } else {
                    apply::apply_batch(
                        &mut *self.sink.session,
                        &self.sink.capabilities,
                        &table,
                        &batch,
                    )
                    .await?;
                }
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
                self.uncovered_roots.insert(self.root_of(&table));
                self.dirty = true;
            }
            LoadItem::Checkpoint { stream, cursor } => {
                self.state.cursors.insert(stream.clone(), cursor.clone());
                // The stream's root table is `normalize_ident(stream)` — the
                // same mapping `runtime::validate`'s `root_table` builds the
                // run on and proves injective across its streams.
                self.uncovered_roots
                    .remove(&TableName::new(rdlt_core::naming::normalize_ident(
                        stream.as_str(),
                        self.sink.capabilities.ident_rules,
                    )));
                self.report.cursors.insert(stream, cursor);
                self.checkpoints_since_commit += 1;
                self.dirty = true;
                // Commit decisions are made only here — a checkpoint boundary —
                // and only a COVERED one: with uncovered co-stream rows in the
                // unit, the commit defers to a later checkpoint (the policy's
                // counters keep accumulating, so the trigger holds until then).
                // The deferral is the exactly-once trade taken deliberately:
                // committing rows no cursor covers is unrecoverable
                // duplication after a crash (restart-from-zero re-extraction
                // re-delivers them — T7E), so the gate stays. What it must
                // not be is SILENT: a co-stream that never checkpoints (a
                // snapshot stream) suspends the mid-run commit cadence for
                // the whole run, so the first deferred trigger warns the
                // operator once, naming the blocking roots.
                if self.policy_triggers() {
                    if self.uncovered_roots.is_empty() {
                        self.commit().await?;
                    } else if self.deferred_commit_warnings == 0 {
                        self.deferred_commit_warnings += 1;
                        let roots = self
                            .uncovered_roots
                            .iter()
                            .map(|t| t.as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        tracing::warn!(
                            uncovered_roots = %roots,
                            "mid-run commits are deferred: these tables hold rows whose own \
                             streams have not checkpointed — the commit waits for their \
                             checkpoints (for snapshot streams, until the run's end)"
                        );
                    }
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

    /// Hold a batch until a threshold is reached.
    ///
    /// A SCHEMA CHANGE forces the buffer out first: Arrow can only
    /// concatenate batches that share a schema, and mid-stream
    /// evolution is exactly when they stop doing so.
    async fn accumulate(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), RdltError> {
        let rows = batch.num_rows() as u64;
        let bytes = batch.get_array_memory_size() as u64;
        let schema_changed = self
            .pending
            .get(table)
            .and_then(|pending| pending.batches.first())
            .is_some_and(|held| held.schema() != batch.schema());
        if schema_changed {
            self.flush_table(table).await?;
        }
        let pending = self.pending.entry(table.clone()).or_insert(Pending {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
        });
        pending.batches.push(batch);
        pending.rows += rows;
        pending.bytes += bytes;
        if self.batch_policy.triggers(pending.rows, pending.bytes) {
            self.flush_table(table).await?;
        }
        Ok(())
    }

    /// Write one table's accumulated rows as a SINGLE batch.
    async fn flush_table(&mut self, table: &TableName) -> Result<(), RdltError> {
        let Some(pending) = self.pending.remove(table) else {
            return Ok(());
        };
        if pending.batches.is_empty() {
            return Ok(());
        }
        let schema = pending.batches[0].schema();
        let batch = if pending.batches.len() == 1 {
            // The common case once a threshold is small relative to
            // the source's batches: no copy at all.
            pending.batches.into_iter().next().expect("one batch")
        } else {
            arrow::compute::concat_batches(&schema, pending.batches.iter())
                .map_err(|e| RdltError::config(format!("coalescing batches for `{table}`: {e}")))?
        };
        apply::apply_batch(
            &mut *self.sink.session,
            &self.sink.capabilities,
            table,
            &batch,
        )
        .await
    }

    /// Write EVERY table's accumulated rows.
    ///
    /// Called before a commit closes and before the run ends, so a
    /// commit is always made of whole writes and nothing is left
    /// buffered when the loader stops.
    async fn flush_all(&mut self) -> Result<(), RdltError> {
        let tables: Vec<TableName> = self.pending.keys().cloned().collect();
        for table in tables {
            self.flush_table(&table).await?;
        }
        Ok(())
    }

    /// Whichever threshold is reached FIRST ends the commit unit; the
    /// policy owns that rule, so the three counters this loader keeps
    /// are all it has to supply.
    fn policy_triggers(&self) -> bool {
        self.policy.triggers(
            self.checkpoints_since_commit,
            self.bytes_since_commit,
            self.last_commit_at.elapsed().as_secs(),
        )
    }

    /// Trailing work (rows after the last checkpoint, or a run that never
    /// checkpointed) gets one final commit; a clean no-op run still commits once so a
    /// fresh pipeline's state document exists.
    pub(crate) async fn finish(&mut self) -> Result<(), RdltError> {
        // Rows can be accumulating without the run being `dirty` in
        // the commit sense, so the flush is unconditional: leaving
        // buffered rows unwritten would lose them silently.
        self.flush_all().await?;
        if self.dirty || self.commit_seq == 0 {
            self.commit().await?;
        }
        Ok(())
    }

    /// The session's orderly end on the SUCCESS path (037 US2 T7 fix
    /// round 1; semantics corrected in fix round 2, M4) — called by
    /// `drain_loader` exactly once, after [`Loader::finish`]'s last
    /// commit has already succeeded. Every commit is ALREADY durable by
    /// the time this runs, so a close failure here can never mean lost
    /// data — it means some OTHER resource (a lock, a lease document,
    /// ...) failed to release, and the prefixed message says so
    /// explicitly rather than leaving the operator to wonder if the
    /// run's data survived. Classified NON-RETRYABLE unconditionally
    /// (`RdltError::destination`, never `classify_dest_error`, which
    /// would trust the destination's OWN transient/fatal classification
    /// — a destination has no way to know this specific failure can
    /// never be helped by re-running the WHOLE load from committed
    /// state, since retrying would re-execute a commit that already
    /// landed). The error still propagates, never swallowed: an
    /// operator should know close failed even though the run itself
    /// did not. The ABANDONMENT path (a failed or cancelled run) never
    /// calls this — see [`Loader::close_best_effort`].
    pub(crate) async fn close(&mut self) -> Result<(), RdltError> {
        self.sink.session.close().await.map_err(|e| {
            RdltError::destination(format!(
                "session close failed AFTER all commits were durable (the data is committed): {e}"
            ))
        })
    }

    /// Best-effort close on an ABANDONMENT path (037 US2 fix round 2,
    /// I1) — a failed or cancelled run whose session would otherwise
    /// simply be dropped. The lease (or whatever a destination's close
    /// releases) protects CONCURRENT sessions, not dead ones: once this
    /// run will write no more, holding it protects nothing, and the
    /// next session's own reclaim runs under ITS OWN lease regardless.
    /// The close error is deliberately swallowed and never returned —
    /// the run's REAL error must not be masked by a cleanup failure on
    /// the way out; a caller calls this and then still returns the
    /// error that made it abandon the session in the first place.
    pub(crate) async fn close_best_effort(&mut self) {
        let _ = self.sink.session.close().await;
    }

    async fn commit(&mut self) -> Result<(), RdltError> {
        // A BATCH NEVER SPANS A COMMIT. Whatever is still accumulating
        // is written first, so the commit unit is made of whole
        // writes and a resume never has to reason about half a batch.
        self.flush_all().await?;
        self.commit_seq += 1;
        self.emit(rdlt_core::PipelineEvent::CommitStarted {
            commit_seq: self.commit_seq,
        });
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
        // Mid-run commits only fire with this empty; `finish`'s trailing
        // commit is the one path that commits through a non-empty set (rows
        // of a stream that ended without checkpointing — full-refresh
        // semantics, re-delivered by design). Either way the new unit
        // starts with nothing owed.
        self.uncovered_roots.clear();
        Ok(())
    }
}
// Inline rather than in `tests/`: a child module can read `Loader`'s private
// fields directly, which these pins need.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rdlt_connector::RecordBatch;
    use rdlt_core::{PipelineId, TableSchema, WriteMode};

    use super::*;

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
                capabilities: DestinationCapabilities::default(),
            },
            RunReport::new(pipeline.clone(), load_id.clone()),
            StateDoc::new(pipeline, "test"),
            load_id,
            Policies {
                commit: policy,
                batch: rdlt_core::BatchPolicy::default(),
            },
            None,
            events,
        )
    }

    /// Records every commit; accepts everything else. The per-stream coverage
    /// pins below only need to observe WHEN a commit was issued and with what
    /// state.
    struct RecordingSession {
        commits: std::sync::Arc<std::sync::Mutex<Vec<CommitMeta>>>,
    }

    #[async_trait::async_trait]
    impl LoadSession for RecordingSession {
        async fn ensure_table(
            &mut self,
            _: &TableSchema,
            _: &WriteMode,
        ) -> Result<(), rdlt_connector::DestinationError> {
            Ok(())
        }
        async fn write(
            &mut self,
            _: &TableName,
            _: RecordBatch,
        ) -> Result<(), rdlt_connector::DestinationError> {
            Ok(())
        }
        async fn commit(
            &mut self,
            meta: CommitMeta,
        ) -> Result<rdlt_core::CommitReceipt, rdlt_connector::DestinationError> {
            let receipt = rdlt_core::CommitReceipt {
                load_id: meta.load_id.clone(),
                commit_seq: meta.commit_seq,
            };
            self.commits.lock().expect("lock").push(meta);
            Ok(receipt)
        }
        async fn read_state(
            &mut self,
            _: &PipelineId,
        ) -> Result<Option<StateDoc>, rdlt_connector::DestinationError> {
            Ok(None)
        }
    }

    fn recording_loader(
        policy: CommitPolicy,
    ) -> (Loader, std::sync::Arc<std::sync::Mutex<Vec<CommitMeta>>>) {
        let commits = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (events, _rx) = tokio::sync::broadcast::channel(16);
        let pipeline = PipelineId::new("p");
        let load_id = LoadId::new("l");
        let loader = Loader::new(
            Sink {
                session: Box::new(RecordingSession {
                    commits: std::sync::Arc::clone(&commits),
                }),
                capabilities: DestinationCapabilities::default(),
            },
            RunReport::new(pipeline.clone(), load_id.clone()),
            StateDoc::new(pipeline, "test"),
            load_id,
            Policies {
                commit: policy,
                batch: rdlt_core::BatchPolicy::default(),
            },
            None,
            events,
        );
        (loader, commits)
    }

    fn delta_item(table: &str, parent: Option<&str>) -> LoadItem {
        let schema = TableSchema {
            table: TableName::new(table),
            parent: parent.map(|p| rdlt_core::ParentLink {
                parent: TableName::new(p),
                depth: 1,
            }),
            columns: vec![],
        };
        LoadItem::Delta {
            delta: rdlt_core::SchemaDelta {
                table: schema.table.clone(),
                from: None,
                to: schema.content_hash(),
                changes: vec![],
            },
            schema,
            mode: WriteMode::Append,
        }
    }

    fn batch_item(table: &str) -> LoadItem {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        LoadItem::Batch {
            table: TableName::new(table),
            batch: RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![1i64, 2]))],
            )
            .expect("batch"),
        }
    }

    fn checkpoint_item(stream: &str) -> LoadItem {
        LoadItem::Checkpoint {
            stream: rdlt_core::StreamName::new(stream),
            cursor: rdlt_core::Cursor::new(serde_json::json!(1)),
        }
    }

    /// THE T7E loader half: this module's own header promises commits happen
    /// only at boundaries whose cursors cover the published rows — but a
    /// commit issued at `events`' checkpoint while `orders` has rows and no
    /// checkpoint publishes rows NO cursor covers. Once such a commit lands
    /// and the run dies, recovery cannot help: the recovered state has no
    /// `orders` cursor, re-extraction re-delivers the rows, and an append
    /// destination has nothing to dedup on (proven live — the multi-table
    /// crash sweep's `ice.receipt.visible` cell). The commit must WAIT for
    /// coverage.
    #[tokio::test]
    async fn a_commit_waits_for_every_written_streams_own_checkpoint() {
        let (mut loader, commits) = recording_loader(CommitPolicy::every_checkpoints(1));
        for item in [
            delta_item("events", None),
            batch_item("events"),
            delta_item("orders", None),
            batch_item("orders"),
            checkpoint_item("events"),
        ] {
            loader.process(item).await.expect("process");
        }
        assert_eq!(
            commits.lock().expect("lock").len(),
            0,
            "`orders` has rows no cursor covers — a commit here would re-extract \
             them as duplicates after a crash"
        );
        loader
            .process(checkpoint_item("orders"))
            .await
            .expect("process");
        let committed = commits.lock().expect("lock");
        assert_eq!(
            committed.len(),
            1,
            "with every written stream covered, the deferred commit fires"
        );
        let cursors = &committed[0].state.cursors;
        assert!(
            cursors.contains_key(&rdlt_core::StreamName::new("events"))
                && cursors.contains_key(&rdlt_core::StreamName::new("orders")),
            "the deferred commit carries BOTH cursors: {cursors:?}"
        );
    }

    /// The deferral is correct but must not be SILENT (042 fix wave): the
    /// first policy trigger blocked by an uncovered co-stream sets the
    /// one-time advisory — and only the first, so an operator gets one
    /// warning per run, not one per checkpoint. Driven twice past the
    /// blocked trigger to pin the guard.
    #[tokio::test]
    async fn a_blocked_trigger_warns_the_operator_exactly_once() {
        let (mut loader, commits) = recording_loader(CommitPolicy::every_checkpoints(1));
        for item in [
            delta_item("events", None),
            batch_item("events"),
            delta_item("orders", None),
            batch_item("orders"),
            checkpoint_item("events"),
        ] {
            loader.process(item).await.expect("process");
        }
        assert_eq!(
            loader.deferred_commit_warnings, 1,
            "the first blocked trigger warns that mid-run commits are deferred"
        );
        // A second blocked trigger at the next covered-less boundary
        // stays quiet: the condition, not each occurrence, is the news.
        for item in [batch_item("events"), checkpoint_item("events")] {
            loader.process(item).await.expect("process");
        }
        assert_eq!(
            loader.deferred_commit_warnings, 1,
            "later blocked triggers do not repeat the advisory"
        );
        assert_eq!(
            commits.lock().expect("lock").len(),
            0,
            "the advisory never weakens the gate — the commit still waits"
        );
    }

    /// The advisory NEVER fires in the all-cursored shape: when every
    /// stream checkpoints before another writes, no trigger is ever
    /// blocked and the cadence needs no warning.
    #[tokio::test]
    async fn covered_commits_never_warn() {
        let (mut loader, commits) = recording_loader(CommitPolicy::every_checkpoints(1));
        for item in [
            delta_item("events", None),
            batch_item("events"),
            checkpoint_item("events"),
            delta_item("orders", None),
            batch_item("orders"),
            checkpoint_item("orders"),
        ] {
            loader.process(item).await.expect("process");
        }
        assert_eq!(commits.lock().expect("lock").len(), 2);
        assert_eq!(
            loader.deferred_commit_warnings, 0,
            "an all-cursored run never sees the deferral advisory"
        );
    }

    /// The single-stream cadence is unchanged: a stream's own checkpoint
    /// covers everything it wrote, so the commit fires right there.
    #[tokio::test]
    async fn a_single_stream_still_commits_at_its_own_checkpoint() {
        let (mut loader, commits) = recording_loader(CommitPolicy::every_checkpoints(1));
        for item in [
            delta_item("events", None),
            batch_item("events"),
            checkpoint_item("events"),
        ] {
            loader.process(item).await.expect("process");
        }
        assert_eq!(commits.lock().expect("lock").len(), 1);
    }

    /// Coverage follows the recorded parent chain: a child table's rows are
    /// covered by its ROOT stream's checkpoint, whatever the child is named
    /// (`child_table_name` truncates and hash-suffixes long names).
    #[tokio::test]
    async fn a_child_tables_rows_are_covered_by_its_root_streams_checkpoint() {
        let (mut loader, commits) = recording_loader(CommitPolicy::every_checkpoints(1));
        for item in [
            delta_item("orders", None),
            delta_item("itm_4f2a9c1b", Some("orders")),
            batch_item("itm_4f2a9c1b"),
            checkpoint_item("orders"),
        ] {
            loader.process(item).await.expect("process");
        }
        assert_eq!(
            commits.lock().expect("lock").len(),
            1,
            "the child's rows belong to `orders`' stream, whose checkpoint this is"
        );
    }

    /// The time-based policy compares ELAPSED seconds against the threshold, so
    /// it is pinned by back-dating `last_commit_at` rather than by sleeping:
    /// wide margins on both sides, no clock control, no flakiness. `>=` → `<`
    /// inverts the comparison, which either commits on every checkpoint or never
    /// commits at all — both caught here.
    #[test]
    fn every_seconds_boundary_fires_only_past_the_threshold() {
        let mut loader = loader_with(CommitPolicy::every_seconds(30));
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
        let mut loader = loader_with(CommitPolicy::every_checkpoints(2));
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
        let mut loader = loader_with(CommitPolicy::every_checkpoints(0));
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

        let mut loader = loader_with(CommitPolicy::every_bytes(100));
        loader.bytes_since_commit = 99;
        assert!(!loader.policy_triggers(), "one byte short");
        loader.bytes_since_commit = 100;
        assert!(loader.policy_triggers(), "at the threshold");
    }
}

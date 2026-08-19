//! The loader: drives one `LoadSession` through ensure → write → commit, owns the
//! run's accounting (no silent failures), and applies the `CommitPolicy`.
//!
//! Commits happen ONLY at COVERED checkpoint boundaries (plus one final commit
//! for trailing work): a boundary where every stream with rows in the unit has
//! a checkpoint of its own. Committing anything less would publish rows the
//! committed cursors don't cover, and a crash would then re-extract them as
//! duplicates — a checkpoint of ANOTHER stream covers nothing, so a trigger
//! that fires there defers to a later, covered one.

use std::time::Instant;

use rdlt_connector::arrow::RecordBatch;

use rdlt_connector::destination::{Capabilities, LoadSession};
use rdlt_core::commit::{self, CommitMeta, CommitPolicy};
use rdlt_core::crash_point;
use rdlt_core::error::Error;
use rdlt_core::id::{LoadId, TableName};
use rdlt_core::report;
use rdlt_core::state::StateDoc;

use super::{apply, item::LoadItem};
use crate::wal::writer::Wal;

/// The destination and how to lower for it — the two are always used together at
/// the write seam (`apply_delta`/`apply_batch` take exactly this pair), so they
/// travel as one.
pub(crate) struct Sink {
    pub(crate) session: Box<dyn LoadSession>,
    pub(crate) capabilities: Capabilities,
}

pub(crate) struct Loader {
    sink: Sink,
    pub(crate) report: report::Run,
    /// The evolving pipeline state; every commit persists a snapshot of it.
    state: StateDoc,
    load_id: LoadId,
    policy: CommitPolicy,
    /// How much to accumulate before each destination write. The
    /// default writes straight through.
    batch_policy: rdlt_core::commit::BatchPolicy,
    /// The per-write cell ceiling the accumulator flushes at.
    max_batch_cells: usize,
    /// Rows waiting to be written, per table.
    ///
    /// Keyed by TABLE because a batch belongs to one, and Arrow
    /// concatenation requires a single schema — two tables' rows
    /// could never be one write.
    pending: std::collections::BTreeMap<TableName, Pending>,
    counters: commit::Counters,
    commit_seq: u64,
    checkpoints_since_commit: u32,
    bytes_since_commit: u64,
    last_commit_at: Instant,
    /// Anything (rows, cursors, schemas) not yet covered by a commit.
    dirty: bool,
    /// Write-ahead log; `None` when no workdir is configured (recovery then always
    /// degrades to cursor re-extraction — slower, never wrong).
    wal: Option<Wal>,
    events: tokio::sync::broadcast::Sender<rdlt_core::event::PipelineEvent>,
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
    /// this to drain — the loader half of per-stream coverage.
    uncovered_roots: std::collections::BTreeSet<TableName>,
    /// Whether the deferral advisory has fired — set exactly at the
    /// warn site, so a blocked commit trigger is worth ONE operator
    /// warning per run, not one per checkpoint.
    warned_deferred_commit: bool,
    /// Memoized table→root resolutions (`root_of`'s cache; see its doc
    /// for why no invalidation exists).
    root_cache: crate::lineage::Chain,
}

/// The two cadences the loader obeys, passed together because they
/// are read together and interact: a batch never spans a commit.
pub(crate) struct Policies {
    /// When a commit unit closes.
    pub(crate) commit: CommitPolicy,
    /// How much accumulates before each destination write.
    pub(crate) batch: rdlt_core::commit::BatchPolicy,
    /// The same per-batch cell ceiling the assembly seats enforce:
    /// the coalescer's `concat_batches` is downstream of every
    /// per-batch gate, so without this the accumulator could fuse
    /// individually-legal batches into one destination write far past
    /// the budget an operator set. The accumulator FLUSHES at the
    /// bound — coalescing is its job — where the assembly seats refuse.
    pub(crate) max_batch_cells: usize,
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
        report: report::Run,
        base_state: StateDoc,
        load_id: LoadId,
        policies: Policies,
        wal: Option<Wal>,
        events: tokio::sync::broadcast::Sender<rdlt_core::event::PipelineEvent>,
    ) -> Self {
        Self {
            sink,
            report,
            state: base_state,
            load_id,
            policy: policies.commit,
            batch_policy: policies.batch,
            max_batch_cells: policies.max_batch_cells,
            pending: std::collections::BTreeMap::new(),
            counters: commit::Counters::default(),
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
            warned_deferred_commit: false,
            root_cache: crate::lineage::Chain::default(),
        }
    }

    /// A written table's ROOT, along the parent links its Deltas recorded; a
    /// table with no recorded parent is its own root. The shared memoized
    /// walk ([`crate::lineage::Chain`], which the recovery scan's covered
    /// filter also resolves roots through — the coverage rule's two halves
    /// stay one rule). A cycle is unreachable from any shred — its
    /// appearance means a rogue's declared lineage or an engine defect —
    /// and an unterminated chain is never memoized, so the old degrade
    /// (attribute the table to itself) both misattributed coverage AND
    /// re-ran the full walk on every batch; refusing typed is honest and
    /// O(1) after the first refusal ends the run. Memoized per table
    /// because the walk would otherwise run per BATCH with per-hop clones;
    /// the memo's own doc says why no invalidation is needed.
    fn root_of(&mut self, table: &TableName) -> Result<TableName, Error> {
        let parents = &self.parents;
        self.root_cache
            .resolve(table, parents.len(), |current| {
                Ok::<_, std::convert::Infallible>(parents.get(current).cloned())
            })
            .unwrap_or_else(|infallible| match infallible {})
            .map(|chain| chain.root().clone())
            .ok_or_else(|| {
                Error::internal(format!(
                    "table `{table}`'s recorded parent chain does not terminate — a cyclic \
                     lineage is a defect, refused rather than misattributed to the table itself"
                ))
            })
    }

    fn emit(&self, event: rdlt_core::event::PipelineEvent) {
        let _ = self.events.send(event); // no listeners is fine
    }

    pub(crate) async fn process(&mut self, item: LoadItem) -> Result<(), Error> {
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
                // A SCHEMA CHANGE forces the buffer out FIRST — here, at
                // the Delta, not one batch later: the pending batches
                // carry the pre-delta schema, and ensuring the widened
                // table before writing them hands old-shape rows to an
                // already-widened destination (or, when the run ends
                // right after the Delta, has `finish` write them after
                // the new ensure). `flush_all` over the one table's
                // flush, deliberately: a recorded child table's shape
                // can ride the same delta, and over-flushing costs a
                // smaller write, never correctness.
                self.flush_all().await?;
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
                    Err(Error::config(
                        "injected crash after ensure_table (failpoint)",
                    ))
                );
                // Track keyed STRUCTURED merges (no `_rdlt_id` column ⇒ the
                // stream is structured): batches must carry non-NULL keys.
                if let rdlt_core::commit::WriteMode::Merge { key } = &mode
                    && !schema
                        .columns
                        .iter()
                        .any(|c| c.name == rdlt_core::schema::system::ID)
                {
                    self.structured_merge_keys
                        .insert(schema.table.clone(), key.clone());
                }
                if let Some(link) = &schema.parent {
                    // A delta that RE-PARENTS an already-linked table is
                    // refused: parent links are append-only, and a chain
                    // resolved through the OLD link may already be
                    // memoized with its tail shared by later chains —
                    // accepting the rewrite would silently split lineage
                    // between what was walked and what is recorded. The
                    // same link re-recorded (schema evolution re-emits
                    // deltas) is idempotent and passes.
                    if let Some(existing) = self.parents.get(&schema.table)
                        && existing != &link.parent
                    {
                        return Err(Error::internal(format!(
                            "table `{}`'s delta re-parents it from `{existing}` to `{}` —                              parent links are append-only; a re-parenting delta is a defect,                              refused rather than splitting recorded lineage",
                            schema.table, link.parent
                        )));
                    }
                    self.parents
                        .insert(schema.table.clone(), link.parent.clone());
                }
                self.emit(rdlt_core::event::PipelineEvent::SchemaEvolved {
                    delta: delta.clone(),
                });
                self.report.schema_migrations.push(delta);
                self.dirty = true;
            }
            LoadItem::Batch {
                table,
                batch,
                bytes,
            } => {
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
                            Error::config(format!(
                                "merge key `{key}` is not a column of table `{table}`"
                            ))
                        })?;
                        if column.null_count() > 0 {
                            return Err(Error::config(format!(
                                "merge key `{key}` contains NULLs in table `{table}` — \
                                 merge keys are identities"
                            )));
                        }
                    }
                }
                let rows = batch.num_rows() as u64;
                // The footprint travels ON the item, computed once at
                // construction — the same number the stage channel
                // already charged.
                let bytes = bytes as u64;
                if self.batch_policy.accumulates() {
                    self.accumulate(&table, batch, bytes).await?;
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
                    Err(Error::config("injected crash after write (failpoint)",))
                );
                self.emit(rdlt_core::event::PipelineEvent::BatchLoaded {
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
                let root = self.root_of(&table)?;
                self.uncovered_roots.insert(root);
                self.dirty = true;
            }
            LoadItem::Checkpoint { stream, cursor } => {
                self.state.cursors.insert(stream.clone(), cursor.clone());
                // The stream's root table via the crate's ONE attribution
                // mapping — the same call `run::validate` proves
                // injective and the recovery scan joins checkpoints on.
                self.uncovered_roots.remove(&crate::lineage::root_table(
                    &stream,
                    self.sink.capabilities.ident_rules,
                ));
                self.report.cursors.insert(stream, cursor);
                self.checkpoints_since_commit += 1;
                self.dirty = true;
                // Commit decisions are made only here — a checkpoint boundary —
                // and only a COVERED one: a commit publishes the whole staged
                // unit atomically (the session's `commit` takes no table
                // subset), so with uncovered co-stream rows in the unit it
                // would publish rows no committed cursor covers, and a crash
                // after it re-extracts them as permanent duplicates (the
                // multi-table crash sweep shows it live). The trigger therefore
                // waits for coverage — a snapshot co-stream suspends the
                // mid-run cadence for the whole run, only `finish`'s trailing
                // commit breaks the cycle — and never SILENTLY: the first
                // deferred trigger warns once, naming the blocking roots, and
                // `run::validate` warns at plan time when the stream set mixes
                // snapshot and cursored streams.
                if self.policy_triggers() {
                    if self.uncovered_roots.is_empty() {
                        self.commit().await?;
                    } else if !self.warned_deferred_commit {
                        self.warned_deferred_commit = true;
                        let roots = self
                            .uncovered_roots
                            .iter()
                            .map(|t| t.as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        tracing::warn!(
                            uncovered_roots = %roots,
                            "mid-run commit deferred: these tables hold rows written since \
                             their own streams' last checkpoints. Commits resume at a \
                             checkpoint boundary where every busy stream has checkpointed \
                             since its last rows — under continuously interleaving busy \
                             streams such boundaries can be rare; for a snapshot stream, \
                             which never checkpoints, none arrives before the run's end"
                        );
                    }
                }
            }
            LoadItem::Discarded {
                table,
                rows,
                values,
            } => {
                self.emit(rdlt_core::event::PipelineEvent::Discarded {
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
    async fn accumulate(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
        bytes: u64,
    ) -> Result<(), Error> {
        let rows = batch.num_rows() as u64;
        let schema_changed = self
            .pending
            .get(table)
            .and_then(|pending| pending.batches.first())
            .is_some_and(|held| held.schema() != batch.schema());
        // Flush BEFORE the accumulation would cross the cell
        // ceiling — each incoming batch is already under it (the
        // assembly seats refuse over-ceiling batches), so every flushed
        // write stays under it too. The budget counts THIS batch's
        // width: coalesced batches share one schema, so the width of
        // the incoming batch is the width of the fused write.
        let cells_after_crossing = self.pending.get(table).is_some_and(|pending| {
            (pending.rows + rows).saturating_mul(batch.num_columns() as u64)
                > self.max_batch_cells as u64
        });
        if schema_changed || cells_after_crossing {
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
    async fn flush_table(&mut self, table: &TableName) -> Result<(), Error> {
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
                .map_err(|e| Error::config(format!("coalescing batches for `{table}`: {e}")))?
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
    async fn flush_all(&mut self) -> Result<(), Error> {
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
    pub(crate) async fn finish(&mut self) -> Result<(), Error> {
        // Rows can be accumulating without the run being `dirty` in
        // the commit sense, so the flush is unconditional: leaving
        // buffered rows unwritten would lose them silently.
        self.flush_all().await?;
        if self.dirty || self.commit_seq == 0 {
            self.commit().await?;
        }
        Ok(())
    }

    /// The session's orderly end on the SUCCESS path — called by
    /// the run's loader drive exactly once, after [`Loader::finish`]'s last
    /// commit has already succeeded. Every commit is ALREADY durable by
    /// the time this runs, so a close failure here can never mean lost
    /// data — it means some OTHER resource (a lock, a lease document,
    /// ...) failed to release, and the prefixed message says so
    /// explicitly rather than leaving the operator to wonder if the
    /// run's data survived. Classified NON-RETRYABLE unconditionally
    /// (`Error::destination`, never `classify_dest_error`, which
    /// would trust the destination's OWN transient/fatal classification
    /// — a destination has no way to know this specific failure can
    /// never be helped by re-running the WHOLE load from committed
    /// state, since retrying would re-execute a commit that already
    /// landed). The error still propagates, never swallowed: an
    /// operator should know close failed even though the run itself
    /// did not. The ABANDONMENT path (a failed or cancelled run) never
    /// calls this — see [`Loader::close_best_effort`].
    pub(crate) async fn close(&mut self) -> Result<(), Error> {
        self.sink.session.close().await.map_err(|e| {
            Error::destination(format!(
                "session close failed AFTER all commits were durable (the data is committed): {e}"
            ))
        })
    }

    /// Best-effort close on an ABANDONMENT path — a failed or cancelled
    /// run whose session would otherwise
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

    async fn commit(&mut self) -> Result<(), Error> {
        // A BATCH NEVER SPANS A COMMIT. Whatever is still accumulating
        // is written first, so the commit unit is made of whole
        // writes and a resume never has to reason about half a batch.
        self.flush_all().await?;
        self.commit_seq += 1;
        self.emit(rdlt_core::event::PipelineEvent::CommitStarted {
            commit_seq: self.commit_seq,
        });
        self.state.last_commit = Some(rdlt_core::state::LastCommit {
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
            .map_err(|e| crate::classify::classify_dest_error(&e))?;
        // The canonical redelivery window: destination acknowledged, WAL not yet
        // marked — a crash here MUST replay idempotently.
        crash_point!(
            "session.after_commit",
            Err(Error::config(
                "injected crash after destination commit (failpoint)",
            ))
        );
        // Step 3: receipt in hand — mark and reclaim covered segments.
        if let Some(wal) = &mut self.wal {
            wal.mark_committed(self.commit_seq).await?;
        }
        self.emit(rdlt_core::event::PipelineEvent::Committed {
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

    use rdlt_core::commit::WriteMode;
    use rdlt_core::id::PipelineId;
    use rdlt_core::schema::TableSchema;

    use super::*;
    use crate::testing::{FakeSession, int_batch, ipc_round_trip};

    /// A loader over `session` under `policies`, with a throwaway pipeline
    /// identity and no WAL.
    fn loader_over(session: FakeSession, policies: Policies) -> Loader {
        let (events, _rx) = tokio::sync::broadcast::channel(16);
        let pipeline = PipelineId::new("p");
        let load_id = LoadId::new("l");
        Loader::new(
            Sink {
                session: Box::new(session),
                capabilities: Capabilities::default(),
            },
            report::Run::new(pipeline.clone(), load_id.clone()),
            StateDoc::new(pipeline, "test"),
            load_id,
            policies,
            None,
            events,
        )
    }

    fn policies(commit: CommitPolicy) -> Policies {
        Policies {
            commit,
            batch: rdlt_core::commit::BatchPolicy::default(),
            max_batch_cells: crate::config::Config::DEFAULT_MAX_BATCH_CELLS,
        }
    }

    /// `policy_triggers` is a pure function of the loader's counters and clock;
    /// no destination call is reachable from it, and the session asserts that
    /// by refusing every call.
    fn loader_with(policy: CommitPolicy) -> Loader {
        loader_over(FakeSession::unreachable(), policies(policy))
    }

    /// A loader whose session records every commit; the per-stream coverage
    /// pins only need to observe WHEN a commit was issued and with what state.
    fn recording_loader(
        policy: CommitPolicy,
    ) -> (Loader, std::sync::Arc<std::sync::Mutex<Vec<CommitMeta>>>) {
        let session = FakeSession::default();
        let commits = std::sync::Arc::clone(&session.commits);
        (loader_over(session, policies(policy)), commits)
    }

    fn delta_item(table: &str, parent: Option<&str>) -> LoadItem {
        let schema = TableSchema {
            table: TableName::new(table),
            parent: parent.map(|p| rdlt_core::schema::ParentLink {
                parent: TableName::new(p),
                depth: 1,
            }),
            columns: vec![],
        };
        LoadItem::Delta {
            delta: rdlt_core::schema::Delta {
                table: schema.table.clone(),
                from: None,
                to: schema.content_hash(),
                changes: vec![],
            },
            schema,
            mode: WriteMode::Append,
        }
    }

    /// A cyclic parent chain refuses TYPED at the first batch that
    /// resolves through it — never the old degrade, which silently
    /// attributed the table to itself AND, because an unterminated
    /// chain is never memoized, re-ran the full walk on every batch
    /// (measured pre-fix on this exact fixture: 4 hops per call, every
    /// call — O(M·N)). The hops meter pins the refusal's cost as one
    /// walk, not one per batch: repeated resolutions of a refused
    /// chain stay bounded because the RUN ends at the first refusal.
    #[tokio::test]
    async fn a_cyclic_parent_chain_refuses_typed_at_first_resolution() {
        let (mut loader, _commits) = recording_loader(CommitPolicy::default());
        for (t, p) in [("t1", "t2"), ("t2", "t3"), ("t3", "t1")] {
            loader.process(delta_item(t, Some(p))).await.expect("delta");
        }
        let links = loader.parents.len();

        let refused = loader
            .process(batch_item("t1"))
            .await
            .expect_err("a batch resolving through a cycle refuses");
        let rendered = refused.to_string();
        assert!(
            rendered.contains("recorded parent chain does not terminate")
                && rendered.contains("refused rather than misattributed"),
            "the refusal names the cycle and the disposition: {rendered}"
        );
        assert!(
            loader.root_cache.hops() <= (links as u64) + 1,
            "the refusal costs ONE bounded walk, not one per batch: {} hops",
            loader.root_cache.hops()
        );
    }

    /// A delta that RE-PARENTS an already-linked table refuses typed —
    /// parent links are append-only, and a chain resolved through the
    /// old link may already be memoized with its tail shared. The SAME
    /// link re-recorded (schema evolution re-emits deltas) is
    /// idempotent and passes.
    #[tokio::test]
    async fn a_reparenting_delta_refuses_and_an_idempotent_one_passes() {
        let (mut loader, _commits) = recording_loader(CommitPolicy::default());
        loader
            .process(delta_item("child", Some("first_parent")))
            .await
            .expect("the first link records");
        loader
            .process(delta_item("child", Some("first_parent")))
            .await
            .expect("the same link re-recorded is idempotent");

        let refused = loader
            .process(delta_item("child", Some("second_parent")))
            .await
            .expect_err("a re-parenting delta refuses");
        let rendered = refused.to_string();
        assert!(
            rendered.contains("re-parents it from `first_parent` to `second_parent`")
                && rendered.contains("parent links are append-only"),
            "the refusal names both links and the rule: {rendered}"
        );
    }

    fn batch_item(table: &str) -> LoadItem {
        LoadItem::batch(TableName::new(table), int_batch(2))
    }

    fn checkpoint_item(stream: &str) -> LoadItem {
        LoadItem::Checkpoint {
            stream: rdlt_core::id::StreamName::new(stream),
            cursor: rdlt_core::cursor::Cursor::new(serde_json::json!(1)),
        }
    }

    /// THE loader half of per-stream coverage: this module's own header
    /// promises commits happen only at boundaries whose cursors cover the
    /// published rows — but a commit issued at `events`' checkpoint while
    /// `orders` has rows and no checkpoint publishes rows NO cursor covers.
    /// Once such a commit lands and the run dies, recovery cannot help: the
    /// recovered state has no `orders` cursor, re-extraction re-delivers the
    /// rows, and an append destination has nothing to dedup on (the
    /// multi-table crash sweep shows it live).
    /// The commit must WAIT for coverage.
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
            cursors.contains_key(&rdlt_core::id::StreamName::new("events"))
                && cursors.contains_key(&rdlt_core::id::StreamName::new("orders")),
            "the deferred commit carries BOTH cursors: {cursors:?}"
        );
    }

    /// The loader's own byte accounting (report totals, commit-policy
    /// counters) meters an IPC-decoded batch near the ONE message body its
    /// buffers slice — the same footprint rule the stage channel applies.
    /// Capacity-summing here charged the body once per buffer (three
    /// buffers in this shape, ≈3x), publishing inflated report bytes and
    /// firing byte-based commit policies that many times early.
    #[tokio::test]
    async fn loader_byte_counters_meter_an_ipc_decoded_batch_by_footprint() {
        let (stream_len, decoded, _row_payload) = ipc_round_trip();

        let (mut loader, _commits) = recording_loader(CommitPolicy::every_checkpoints(10));
        for item in [
            delta_item("events", None),
            LoadItem::batch(TableName::new("events"), decoded),
        ] {
            loader.process(item).await.expect("process");
        }
        let metered = loader.bytes_since_commit;
        assert!(
            metered > 0 && metered <= 2 * stream_len as u64,
            "the loader's byte counters must meter the decoded batch near its \
             {stream_len}-byte body, not capacity-sum it: metered {metered}"
        );
        assert_eq!(
            loader
                .report
                .tables
                .get(&TableName::new("events"))
                .map(|t| t.bytes),
            Some(metered),
            "the report's table bytes are the same meter"
        );
    }

    /// The deferral is correct but must not be SILENT: the
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
        assert!(
            loader.warned_deferred_commit,
            "the first blocked trigger warns that mid-run commits are deferred"
        );
        // A second blocked trigger at the next covered-less boundary
        // stays quiet: the condition, not each occurrence, is the news.
        for item in [batch_item("events"), checkpoint_item("events")] {
            loader.process(item).await.expect("process");
        }
        assert!(
            loader.warned_deferred_commit,
            "the guard stays set — later blocked triggers do not repeat the advisory \
             (the warn site fires only on the false→true transition)"
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
        assert!(
            !loader.warned_deferred_commit,
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

    /// The coalescer flushes at the cell ceiling — the same knob the
    /// assembly seats enforce. With a batch policy that would otherwise fuse
    /// the whole run (`every_rows` far above it) and a two-cell budget, three
    /// one-cell pushes must arrive as THREE writes, never one fused write
    /// past the ceiling.
    #[tokio::test]
    async fn the_accumulator_flushes_at_the_cell_ceiling() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let session = FakeSession::default();
        let writes = Arc::clone(&session.writes);
        let mut loader = loader_over(
            session,
            Policies {
                commit: CommitPolicy::default(),
                // Would fuse the whole run into one write…
                batch: rdlt_core::commit::BatchPolicy::every_rows(1_000_000),
                // …except the cell ceiling flushes every second cell.
                max_batch_cells: 2,
            },
        );

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .expect("one-cell batch");
        let table = TableName::new("t");
        for _ in 0..3 {
            loader
                .process(LoadItem::batch(table.clone(), batch.clone()))
                .await
                .expect("each push lands");
        }
        loader.finish().await.expect("finish flushes the tail");
        // The ceiling is INCLUSIVE: the accumulator holds up to two
        // cells, so three one-cell pushes flush once mid-run and once
        // at finish — two writes of at most two cells each, never the
        // one fused write the row policy alone would have produced.
        assert_eq!(
            *writes.lock().expect("count lock"),
            2,
            "flushes at the ceiling (2 cells) then the tail — never fused past it"
        );
    }
}

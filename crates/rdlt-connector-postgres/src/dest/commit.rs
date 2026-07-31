//! The load-session protocol: staging COPY, the publish transaction, merge
//! arms, receipts, state.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::SinkExt;
use rdlt_connector::{
    CommitMeta, CommitReceipt, DestinationError, LoadSession, RecordBatch, WriteMode,
    core::{LoadId, PipelineId, StateDoc, TableName, TableSchema, crash_point},
};
use tokio_postgres::Client;

use rdlt_connector_sqlcore::plan::scope_replace_sql;
use rdlt_connector_sqlcore::protocol::unit;
use rdlt_connector_sqlcore::{
    CommitContext, FullLoadPublish, MergeDialect, Step, build_merge_plan, plan_commit,
    prepare_target, render_arm, staged_probe_targets,
};

use super::dialect::PgDialect;
use super::{classify_stmt, encode, fatal, quote, transient};

/// The 11-byte binary-COPY file signature. The `\r\n` / `\n` pair and the
/// high 0xff byte are a deliberate corruption detector: a stream mangled by
/// a text-mode or non-8-bit-clean transport no longer matches, so the server
/// rejects it up front instead of misreading the first tuple.
const COPY_BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// The unit transaction's three literal statements. Named (and pinned in
/// `tests/golden_unit_sql.rs`) because the isolation level is load-bearing:
/// the "a reload is never observed empty" guarantee is a property of the
/// transaction, and a silent edit here would weaken it without failing
/// anything else.
pub const UNIT_BEGIN: &str = "BEGIN ISOLATION LEVEL READ COMMITTED";
pub const UNIT_COMMIT: &str = "COMMIT";
pub const UNIT_ROLLBACK: &str = "ROLLBACK";
/// Transaction-scoped sort memory for the merge dedup — see `begin_unit`.
pub const UNIT_WORK_MEM: &str = "SET LOCAL work_mem = '64MB'";

/// Arrival-order column on STAGE tables only: makes merge dedup deterministic
/// ("last wins" for real). Excluded from publish column lists because it
/// is not part of the logical schema.
pub use rdlt_connector_sqlcore::names::ARRIVAL_COL;

/// Stage names are pipeline-scoped and hashed: scoping stops one pipeline's `open`
/// from truncating another's live staged rows in a shared schema, and
/// hashing bounds the identifier under Postgres's 63-byte limit, where silent
/// truncation would otherwise cut off exactly the disambiguation suffix.
pub(super) fn stage_prefix(pipeline: &PipelineId) -> String {
    format!(
        "{}{}_",
        rdlt_connector_sqlcore::names::STAGE_PREFIX,
        rdlt_connector::core::naming::ident_hash(pipeline.as_str(), 8)
    )
}

pub(super) fn stage_name(pipeline: &PipelineId, table: &TableName) -> String {
    rdlt_connector_sqlcore::names::stage_table(pipeline.as_str(), table.as_str())
}

/// Quoted, comma-joined logical columns — the shared sqlcore rule; publishes
/// are ALWAYS by name.
pub(super) use rdlt_connector_sqlcore::column_list;

/// The facts an open unit transaction carries. There is deliberately no
/// `tokio_postgres::Transaction` here: it borrows `&'a mut Client`, so a
/// session that owns its client cannot also hold one across `write` calls.
/// The transaction is therefore driven by literal `BEGIN`/`COMMIT`/`ROLLBACK`
/// through the same connection — a borrow fact, not a preference, and not
/// worth buying a self-referential-struct dependency to defeat.
pub(super) struct Unit {
    /// Whether any unit of this load already committed — read once, as the
    /// first statement after `BEGIN`, because it is the durable half of the
    /// Replace once-per-load guard and every write of this unit consults it.
    load_committed_before: bool,
    /// Targets THIS unit cleared. Promoted into the session's set on COMMIT
    /// and discarded on ROLLBACK, because a rolled-back clear did not happen.
    cleared: BTreeSet<TableName>,
}

pub(super) struct PgSession {
    pub(super) client: Client,
    pub(super) pipeline: PipelineId,
    /// The load this session belongs to. It is what scopes the Replace
    /// once-per-load clear guard, so replaying a crashed load's batches must
    /// happen through a session opened under THAT load's id — which is why
    /// the engine gives WAL recovery its own session.
    pub(super) load_id: LoadId,
    pub(super) tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    pub(super) options: super::config::DestinationOptions,
    /// The unit transaction, opened lazily at the first `write` of a unit and
    /// closed by `commit`. `None` between units.
    pub(super) unit: Option<Unit>,
    /// Targets a COMMITTED unit of this load has already cleared — the
    /// in-process half of the once-per-load guard, which stops units 2..N of a
    /// live load re-clearing what unit 1 published.
    pub(super) cleared_targets: BTreeSet<TableName>,
    /// Single-unit discipline, PER TABLE: tables whose stage has already
    /// published non-empty in an earlier commit unit of THIS load.
    /// Session-scoped is load-scoped: a session spans one engine run = one
    /// load; a crash starts both afresh. Marked only AFTER the unit's
    /// transaction commits (a rolled-back unit never counts), and re-marked
    /// on the replay branch (a committed unit whose outcome the client never
    /// learned still counts).
    pub(super) single_unit_done: BTreeSet<TableName>,
}

impl PgSession {
    fn root_of(&self, table: &TableName) -> TableName {
        rdlt_connector_sqlcore::root_of(&self.tables, table)
    }

    /// The planning facts that are known WITHOUT touching the server. The two
    /// probe-derived fields are supplied by the caller, which is why this
    /// takes them: `write` knows neither and needs neither.
    fn ctx<'a>(
        &'a self,
        replayed: bool,
        staged_nonempty: &'a BTreeSet<TableName>,
        cleared: &'a BTreeSet<TableName>,
    ) -> CommitContext<'a> {
        CommitContext {
            replayed,
            load_committed_before: self.unit.as_ref().is_some_and(|u| u.load_committed_before),
            single_unit_done: &self.single_unit_done,
            staged_nonempty,
            full_load_publish: FullLoadPublish::DirectToTarget,
            cleared_targets: cleared,
        }
    }

    /// Open the unit transaction if it is not already open. Idempotent, so
    /// both `write` and a write-less `commit` can call it unconditionally.
    ///
    /// `load_committed_before` is read as the FIRST statement inside the
    /// transaction: it is the durable half of the Replace once-per-load guard,
    /// so reading it inside the same transaction that will do the clearing is
    /// what makes "clear at most once per load" hold across a crash.
    async fn begin_unit(&mut self, load_id: &str) -> Result<(), DestinationError> {
        if self.unit.is_some() {
            return Ok(());
        }
        crash_point!(
            "pg.unit.begin",
            Err(DestinationError::fatal("injected crash at pg.unit.begin"))
        );
        // READ COMMITTED is stated rather than inherited: the atomicity this
        // unit relies on (a reader never sees a cleared-but-unfilled target)
        // is a property of the isolation level, so it is not left to a
        // server-side default a deployment could change.
        self.client
            .batch_execute(UNIT_BEGIN)
            .await
            .map_err(transient)?;
        // Merge dedup sorts the staged rows, and Postgres's default work_mem
        // of 4 MB makes that spill to disk on any load worth benchmarking.
        //
        // 64 MB does NOT stop the spill at a million rows — measured, not
        // assumed: the dedup sort reports `external merge Disk: 169832kB`
        // there. Raising it was A/B'd on that exact shape and is deliberately
        // not done. Going to 128 MB keeps the sort in memory and buys about
        // 45 ms against a 4.8 s load — under 1% — and past 128 MB the number
        // stops moving at all. That measurement also flatters the change,
        // because the probe planned with parallel workers while the real sort
        // runs inside an INSERT and cannot. Under 1% is not worth doubling a
        // per-sort memory reservation on a server this destination may be
        // sharing.
        //
        // `SET LOCAL` is the whole reason this is safe to do unasked: it is
        // scoped to THIS transaction and reverts at COMMIT or ROLLBACK, so it
        // cannot affect another session, another pipeline sharing the
        // database, or even this connection's next unit. A bare `SET` would
        // leak into all three.
        //
        // The value is per sort operation, not per transaction, so it is kept
        // modest rather than generous: a unit runs a bounded number of sorts,
        // and this is a destination that may be one of several against the
        // same server.
        self.client
            .batch_execute(UNIT_WORK_MEM)
            .await
            .map_err(transient)?;
        let load_committed_before = self
            .client
            .query_one(
                &format!(
                    "SELECT count(*) FROM {} WHERE load_id = $1",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                &[&load_id],
            )
            .await
            .map_err(transient)?
            .get::<_, i64>(0)
            > 0;
        self.unit = Some(Unit {
            load_committed_before,
            cleared: BTreeSet::new(),
        });
        Ok(())
    }

    /// Abandon the open unit. Called on every error path out of `write` and
    /// `commit`: a failed statement leaves the connection in an aborted
    /// transaction where every later statement fails with 25P02, so a retry
    /// that did not roll back could never succeed. The in-unit clear record
    /// is discarded with it — a rolled-back TRUNCATE did not happen, and a
    /// retry must clear again.
    async fn rollback_unit(&mut self) {
        if self.unit.take().is_none() {
            return;
        }
        if let Err(e) = self.client.batch_execute(UNIT_ROLLBACK).await {
            // The unit is already failing; this is the diagnostic, not the
            // outcome. A connection too broken to ROLLBACK is one the driver
            // will drop, which aborts the transaction anyway.
            tracing::warn!(error = %super::describe(&e), "unit rollback failed");
        }
    }

    /// Everything `write` must do to a target before its first row lands:
    /// open the unit, then run whatever the planner says has to precede the
    /// data (for Replace, exactly one `TRUNCATE` per load).
    async fn prepare_for_write(
        &mut self,
        table: &TableName,
        load_id: &str,
    ) -> Result<(), DestinationError> {
        self.begin_unit(load_id).await?;
        // Only Replace has anything to prepare. Checked before building the
        // union below, so the common path (Append, and every merge table)
        // allocates nothing.
        if !matches!(self.tables.get(table), Some((_, WriteMode::Replace))) {
            return Ok(());
        }
        // Seed the guard from the DURABLE record before asking the planner.
        // A crash-recovery session has fresh memory and would otherwise
        // re-clear a target an earlier unit of this same load already cleared
        // and filled.
        if !self.cleared_targets.contains(table)
            && !self
                .unit
                .as_ref()
                .expect("unit open")
                .cleared
                .contains(table)
            && self.target_cleared_durably(load_id, table).await?
        {
            self.cleared_targets.insert(table.clone());
        }
        let cleared: BTreeSet<TableName> = self
            .cleared_targets
            .union(&self.unit.as_ref().expect("unit open").cleared)
            .cloned()
            .collect();
        let steps = prepare_target(
            &self.tables,
            &self.ctx(false, &BTreeSet::new(), &cleared),
            table,
        );
        for step in steps {
            let Step::ClearTarget { table } = step else {
                // `prepare_target` emits nothing else; anything else would be
                // a planner change that this executor has not been taught.
                return Err(fatal(
                    "internal: unexpected step planned before a direct write",
                ));
            };
            crash_point!(
                "pg.target.clear",
                Err(DestinationError::fatal("injected crash at pg.target.clear"))
            );
            self.client
                .batch_execute(&PgDialect.clear_table(&quote(table.as_str())))
                .await
                .map_err(classify_stmt)?;
            // Recorded in the SAME transaction as the TRUNCATE it describes,
            // so the two cannot disagree: a rolled-back clear leaves no record
            // claiming it happened, and a durable record always has the empty
            // target behind it.
            self.client
                .execute(
                    &format!(
                        "INSERT INTO {} VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        rdlt_connector_sqlcore::names::CLEARED_TABLE
                    ),
                    &[&load_id, &table.as_str()],
                )
                .await
                .map_err(classify_stmt)?;
            self.unit.as_mut().expect("unit open").cleared.insert(table);
        }
        Ok(())
    }

    /// Whether a COMMITTED unit of this load already cleared this target.
    ///
    /// The durable half of the once-per-(load, target) guard. It replaces the
    /// old per-LOAD test, which answered the wrong question: a Replace table
    /// registered in one unit and first written in a later one found the load
    /// already committed and skipped its clear entirely.
    async fn target_cleared_durably(
        &self,
        load_id: &str,
        table: &TableName,
    ) -> Result<bool, DestinationError> {
        let cleared: bool = self
            .client
            .query_one(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM {} WHERE load_id = $1 AND table_name = $2)",
                    rdlt_connector_sqlcore::names::CLEARED_TABLE
                ),
                &[&load_id, &table.as_str()],
            )
            .await
            .map_err(transient)?
            .get(0);
        Ok(cleared)
    }

    /// Where this table's rows land: its stage if it merges, the target itself
    /// otherwise.
    fn write_target(&self, table: &TableName) -> String {
        match self.tables.get(table) {
            Some((_, WriteMode::Merge { .. })) => stage_name(&self.pipeline, table),
            // A table nothing ensured cannot merge, so the target is right.
            _ => table.as_str().to_owned(),
        }
    }
}

/// Execute one planned [`Step`] inside the OPEN unit transaction.
///
/// Free-standing so it can borrow the session fields disjointly from `client`.
/// Every decision and the ordering come from the planner; this renders each
/// step's SQL through the PgDialect seam and the shared renderers.
///
/// Failures CLASSIFY by SQLSTATE (the shared rule with the duckdb executor):
/// environmental errors ride the engine's retry budget, while deterministic
/// classes (22/23/42) — a duplicate receipt's unique violation included, which
/// is the idempotence-anomaly signal — fail loudly instead of burning
/// retries.
async fn execute_step(
    tx: &Client,
    pipeline: &PipelineId,
    tables: &BTreeMap<TableName, (TableSchema, WriteMode)>,
    options: &super::config::DestinationOptions,
    roots: &BTreeMap<TableName, TableName>,
    meta: &CommitMeta,
    step: &Step,
) -> Result<(), DestinationError> {
    match step {
        // Unreachable in THIS executor and deliberately not carried as dead
        // SQL (greenfield: the superseded path is deleted, not kept warm).
        // Postgres always plans `FullLoadPublish::DirectToTarget`, so a full
        // load's rows are already in the target and its clear happened at the
        // first write. Reaching either arm means the planner changed without
        // this executor being taught, which is worth saying loudly rather than
        // silently re-running a publish that already happened.
        Step::ClearTarget { table } | Step::InsertSelect { table } => {
            return Err(fatal(format!(
                "internal: staged publish step planned for `{table}`, but this \
                 destination writes full loads directly"
            )));
        }
        Step::ScopeReplace { table, scope } => {
            let target = quote(table.as_str());
            let stage = quote(&stage_name(pipeline, table));
            tx.batch_execute(&scope_replace_sql(&PgDialect, &target, &stage, scope))
                .await
                .map_err(classify_stmt)?;
        }
        Step::MergeArm { table, arm } => {
            let (schema, mode) = &tables[table];
            let WriteMode::Merge { key } = mode else {
                // The planner emits MergeArm only for merge tables.
                return Err(fatal(format!(
                    "internal: merge arm planned for non-merge table `{table}`"
                )));
            };
            let root = &roots[table];
            let target = quote(table.as_str());
            let stage = quote(&stage_name(pipeline, table));
            let cols = column_list(schema);
            let root_stage = quote(&stage_name(pipeline, root));
            let root_schema = tables.get(root).map(|(s, _)| s);
            let dialect = PgDialect;
            let plan = build_merge_plan(
                &dialect,
                options,
                table,
                schema,
                key,
                &target,
                &stage,
                &cols,
                root,
                root_stage,
                root_schema,
            );
            for sql in render_arm(&plan, arm) {
                tx.batch_execute(&sql).await.map_err(classify_stmt)?;
            }
        }
        Step::TruncateStage { table } => {
            tx.batch_execute(&PgDialect.clear_table(&quote(&stage_name(pipeline, table))))
                .await
                .map_err(classify_stmt)?;
        }
        Step::UpsertState => {
            // State travels in the SAME transaction as the data.
            let doc = serde_json::to_string(&meta.state).map_err(fatal)?;
            tx.execute(
                &format!(
                    "INSERT INTO {} VALUES ($1, $2)
             ON CONFLICT (pipeline) DO UPDATE SET doc = EXCLUDED.doc",
                    rdlt_connector_sqlcore::names::STATE_TABLE
                ),
                &[&meta.state.pipeline.as_str(), &doc],
            )
            .await
            .map_err(classify_stmt)?;
        }
        Step::InsertReceipt => {
            tx.execute(
                &format!(
                    "INSERT INTO {} VALUES ($1, $2)",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                &[&meta.load_id.as_str(), &(meta.commit_seq as i64)],
            )
            .await
            .map_err(classify_stmt)?;
        }
    }
    Ok(())
}

#[async_trait]
impl LoadSession for PgSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        // Same discipline as `write`: this runs DDL, and once a unit is open
        // that DDL joins the unit transaction. A statement failing inside a
        // transaction poisons the connection until ROLLBACK, so an error here
        // must abandon the unit rather than leave every later statement
        // failing with 25P02.
        match self.ensure_table_inner(schema, mode).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.rollback_unit().await;
                Err(e)
            }
        }
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        // Every exit from here must leave the connection usable: a statement
        // that fails mid-transaction poisons it until ROLLBACK, and the engine
        // may retry a transient failure on this same session.
        match self.write_inner(table, batch).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.rollback_unit().await;
                Err(e)
            }
        }
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        match self.commit_inner(&meta).await {
            Ok(receipt) => Ok(receipt),
            Err(e) => {
                self.rollback_unit().await;
                Err(e)
            }
        }
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let row = self
            .client
            .query_opt(
                &format!(
                    "SELECT doc FROM {} WHERE pipeline = $1",
                    rdlt_connector_sqlcore::names::STATE_TABLE
                ),
                &[&pipeline.as_str()],
            )
            .await
            .map_err(transient)?;
        match row {
            Some(row) => {
                let doc: String = row.get(0);
                Ok(Some(serde_json::from_str(&doc).map_err(fatal)?))
            }
            None => Ok(None),
        }
    }
}

impl PgSession {
    async fn ensure_table_inner(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        // Rendering lives in `ddl`; this drives it. The split is what makes
        // the emitted sequence testable without a server — the statements are
        // compared as data, and only their execution needs a connection.
        let previous = self.tables.get(&schema.table).map(|(s, _)| s.clone());
        for sql in super::ddl::table_ddl_stmts(&self.pipeline, schema, mode, previous.as_ref()) {
            self.client
                .batch_execute(&sql)
                .await
                .map_err(classify_stmt)?;
        }
        for stmt in super::ddl::merge_ensure_stmts(&self.options, schema, mode).map_err(fatal)? {
            if let Err(e) = self.client.batch_execute(&stmt.sql).await {
                // Pre-existing duplicate keys under upsert — typed, naming
                // the key columns.
                if let Some(columns) = &stmt.unique_index
                    && e.as_db_error()
                        .is_some_and(|db| db.code().code() == "23505")
                {
                    return Err(fatal(
                        rdlt_connector_sqlcore::names::duplicate_merge_key_diagnosis(
                            schema.table.as_str(),
                            columns,
                            &super::describe(&e),
                        ),
                    ));
                }
                return Err(classify_stmt(e));
            }
        }
        self.tables
            .insert(schema.table.clone(), (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write_inner(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let load_id = self.load_id.as_str().to_owned();
        self.prepare_for_write(table, &load_id).await?;
        crash_point!(
            "pg.unit.write",
            Err(DestinationError::fatal("injected crash at pg.unit.write"))
        );
        let stage = self.write_target(table);
        let arrow_schema = batch.schema();
        let column_names = arrow_schema
            .fields()
            .iter()
            .map(|f| quote(f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        // Wire decisions come from the ENSURED schema's logical types;
        // arrow representation only fills in where the schema is silent.
        let table_schema = self.tables.get(table).map(|(s, _)| s);
        let wires: Vec<encode::ColumnWire> = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let logical = table_schema
                    .and_then(|s| s.column(f.name()))
                    .map(|c| &c.column_type);
                encode::column_wire(logical, f.data_type())
            })
            .collect::<Result<_, _>>()?;

        // Downcast ONCE per column, not once per cell.
        let encoders: Vec<encode::ColumnEncoder<'_>> = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                encode::ColumnEncoder::new(wires[idx], batch.column(idx).as_ref(), f.name())
            })
            .collect::<Result<_, _>>()?;
        let names: Vec<&str> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let field_count = i16::try_from(batch.num_columns())
            .map_err(|_| fatal("a COPY tuple cannot carry more than 32767 columns"))?;

        let sink = self
            .client
            .copy_in::<_, Bytes>(&format!(
                "COPY {} ({column_names}) FROM STDIN BINARY",
                quote(&stage)
            ))
            .await
            .map_err(classify_stmt)?;
        futures::pin_mut!(sink);

        // One reused buffer for the whole batch, handed to the sink in ~64 KiB
        // slices. The driver's own `BinaryCopyInWriter` flushes every 4096
        // bytes into an `mpsc::channel(1)`, so each flush is a wakeup and a
        // task handoff — the mechanism behind the six-figure voluntary
        // context-switch count on a 1M-row load.
        //
        // 64 KiB is the measured knee, not a guess. Swept on pg-to-pg-1m
        // (medians of 5, same binary, flush size the only variable):
        //
        //     flush     cpu s    vol ctxsw
        //       4 KiB    0.61        68497
        //      16 KiB    0.53        42774
        //      64 KiB    0.49        27013
        //     256 KiB    0.48        22467
        //       1 MiB    0.48        21836
        //
        // CPU is flat from 64 KiB on, so everything past it buys only context
        // switches, and buys them badly: 4x the buffer for 17% fewer, then 4x
        // again for 3% fewer. Wall was flat across the whole sweep — this
        // cell is not CPU-bound, so the win here is headroom, not seconds.
        const FLUSH_AT: usize = 64 * 1024;
        let mut buf = BytesMut::with_capacity(FLUSH_AT * 2);
        buf.put_slice(COPY_BINARY_SIGNATURE);
        buf.put_i32(0); // flags: no OIDs
        buf.put_i32(0); // header extension area: empty

        for row in 0..batch.num_rows() {
            buf.put_i16(field_count);
            for (col, encoder) in encoders.iter().enumerate() {
                // On error the sink is dropped WITHOUT `finish()`, which is
                // tokio-postgres' abort protocol: the server sees CopyFail and
                // the staged rows never become visible. There is deliberately
                // no cleanup path here that would finish the copy instead.
                encoder.encode_field(row, names[col], &mut buf)?;
            }
            if buf.len() >= FLUSH_AT {
                let chunk = buf.split().freeze();
                sink.as_mut().feed(chunk).await.map_err(classify_stmt)?;
                // `split` hands the used capacity to the chunk, so without
                // this the buffer is near-empty from the third flush on and
                // every later chunk is rebuilt by repeated growth inside the
                // per-cell loop — the opposite of reusing one buffer.
                buf.reserve(FLUSH_AT);
            }
        }
        buf.put_i16(-1); // file trailer
        sink.as_mut()
            .feed(buf.split().freeze())
            .await
            .map_err(classify_stmt)?;
        sink.as_mut().flush().await.map_err(classify_stmt)?;
        sink.finish().await.map_err(classify_stmt)?;
        Ok(())
    }

    async fn commit_inner(&mut self, meta: &CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        // The clear guard is scoped by the SESSION's load, so a unit committed
        // under a different one would consult the wrong guard. The engine
        // gives WAL recovery its own session precisely so this holds; check it
        // rather than assume it.
        if meta.load_id != self.load_id {
            return Err(fatal(format!(
                "internal: session opened for load `{}` asked to commit load `{}`",
                self.load_id, meta.load_id
            )));
        }
        let roots: BTreeMap<TableName, TableName> = self
            .tables
            .keys()
            .map(|t| (t.clone(), self.root_of(t)))
            .collect();

        // A unit with no writes still has to publish state and a receipt, so
        // the transaction is opened here if `write` never did.
        let load_id = self.load_id.as_str().to_owned();
        self.begin_unit(&load_id).await?;
        let tx = &self.client;
        // Idempotence by (load_id, commit_seq).
        let replayed = tx
            .query_one(
                &unit::receipt_exists_sql(|n| format!("${n}")),
                &[&meta.load_id.as_str(), &(meta.commit_seq as i64)],
            )
            .await
            .map_err(transient)?
            .get::<_, i64>(0)
            > 0;
        // Probe the full-feed stages the planner needs. Staged-row counts are
        // INVARIANT across the publish (no stage is written during it — merges
        // read stages, publishes write targets), so probing up front matches the
        // former lazy per-table check.
        let mut staged_nonempty = std::collections::BTreeSet::new();
        for table in staged_probe_targets(&self.tables, &self.options) {
            let stage = quote(&stage_name(&self.pipeline, table));
            let staged: bool = tx
                .query_one(&unit::stage_nonempty_sql(&stage), &[])
                .await
                .map_err(transient)?
                .get(0);
            if staged {
                staged_nonempty.insert(table.clone());
            }
        }

        // The planner owns every decision + the ordering; this session executes.
        let cleared: BTreeSet<TableName> = self
            .cleared_targets
            .union(&self.unit.as_ref().expect("unit open").cleared)
            .cloned()
            .collect();
        let script = plan_commit(
            &self.tables,
            &self.options,
            &self.ctx(replayed, &staged_nonempty, &cleared),
        )
        .map_err(fatal)?;

        // A REDELIVERED unit must be thrown away, not published.
        //
        // What a redelivered unit owes depends on WHERE its rows are, and the
        // answer is inverted between publish paths — so it is the shared
        // planner's to state, not this executor's to remember.
        //
        // On this path `write` COPYed the redelivered rows straight into the
        // target (or, for merge, into the stage) inside the transaction still
        // open, so committing would land them a SECOND time. Rolling back
        // discards exactly what this unit wrote — target rows and staged rows
        // alike, since both went through the one transaction — and leaves the
        // earlier commit standing. The receipt is returned as success, because
        // from the caller's side the unit did commit; it just committed the
        // first time.
        //
        // The single-unit marks are still applied: a full-feed unit whose
        // outcome the client never learned still counts against the
        // discipline.
        if replayed
            && unit::replay_disposition(FullLoadPublish::DirectToTarget)
                == unit::ReplayDisposition::DiscardUnit
        {
            self.rollback_unit().await;
            self.single_unit_done.extend(script.marks);
            return Ok(receipt);
        }

        // Narrowed from "before BEGIN" to "before the first publish step":
        // the unit transaction is already open and already holds this unit's
        // rows, so this is the edge where a crash must leave them invisible.
        crash_point!(
            "pg.publish.begin",
            Err(DestinationError::fatal(
                "injected crash at pg.publish.begin"
            ))
        );
        for step in &script.steps {
            execute_step(
                &self.client,
                &self.pipeline,
                &self.tables,
                &self.options,
                &roots,
                meta,
                step,
            )
            .await?;
        }

        // The canonical redelivery window: on a fresh unit everything is
        // published in ONE server-side transaction, so a crash at either edge of
        // tx.commit() must replay idempotently — the injected error models the
        // client dying without learning the outcome. A replay unit only
        // truncated stages and carried no receipt/state edge (it was never
        // instrumented), so the crash point stays confined to the fresh path.
        if !replayed {
            crash_point!(
                "pg.tx.commit",
                Err(DestinationError::fatal("injected crash at pg.tx.commit"))
            );
        }
        self.client
            .batch_execute(UNIT_COMMIT)
            .await
            .map_err(transient)?;
        // The other half of the redelivery window, and the sharper half: the
        // transaction is COMMITTED and durable, and the client dies before it
        // can act on that fact. `pg.tx.commit` above covers "the commit may or
        // may not have landed"; here it definitely landed, so recovery MUST
        // find the published unit and return its receipt instead of
        // re-publishing. Nothing below this line has run yet — the unit is
        // still open and the clear/mark promotions have not applied — so replay
        // has to reconstruct them from the durable state alone, which is
        // exactly the property exactly-once rests on.
        crash_point!(
            "pg.tx.acked",
            Err(DestinationError::fatal("injected crash at pg.tx.acked"))
        );
        // Applied only after the unit's transaction committed (a rolled-back
        // unit never counts). The clears promote for the same reason.
        let unit = self.unit.take().expect("unit open");
        self.cleared_targets.extend(unit.cleared);
        self.single_unit_done.extend(script.marks);
        Ok(receipt)
    }
}

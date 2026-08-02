//! One load session's system IO — the [`Backend`] behind the sdk
//! choreography, mapping generation 1's ONE-transaction commit onto
//! the framework hooks.
//!
//! Staging: rows land in TEMP tables on THIS session's connection.
//! Temp tables die with the connection, so a fresh open reclaims a
//! crashed predecessor's stage for free — that is the whole orphan
//! story. The commit is one DuckDB transaction that publishes staged
//! rows, persists state, and inserts the receipt together; the
//! PLANNER (sqlcore `plan_commit`) owns every decision and the
//! ordering.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use duckdb::params;
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::{DestinationError, RecordBatch};
use rdlt_connector_sqlcore::protocol::{
    CommitContext, FullLoadPublish, Step, build_merge_plan, plan_commit, render_arm,
    staged_probe_targets, unit,
};
use rdlt_connector_sqlcore::{DestinationOptions, MergeDialect as _, column_list, names, root_of};

use super::client::{Db, classify, fatal, is_constraint_violation};
use super::dialect::DuckDialect;
use super::schema::{merge_ddl, quote, stage_name, table_ddl};

/// The meta tables, ensured at open. BYTE-FROZEN: this DDL is a
/// persisted format shared with every database generation 1 wrote.
const META_DDL: &str = "CREATE TABLE IF NOT EXISTS _rdlt_state (pipeline VARCHAR PRIMARY KEY, doc VARCHAR);\nCREATE TABLE IF NOT EXISTS _rdlt_commits (\n    load_id VARCHAR, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));";

/// The session state.
pub struct Load {
    conn: duckdb::Connection,
    options: DestinationOptions,
    /// Everything this session ensured — the planner's world.
    tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    /// The schema each table LAST ensured this session — what makes
    /// the widen a within-run rule.
    previous: BTreeMap<TableName, TableSchema>,
    /// Scoped tables whose single commit unit has run.
    single_unit_done: BTreeSet<TableName>,
}

impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Load").finish_non_exhaustive()
    }
}

impl Load {
    /// Open one session: a cloned connection (its own temp-table
    /// catalog — a dead session's stages are unreachable), the meta
    /// tables ensured.
    pub(super) fn open(db: &Db, options: DestinationOptions) -> Result<Self, DestinationError> {
        let conn = db.session()?;
        conn.execute_batch(META_DDL).map_err(classify)?;
        Ok(Self {
            conn,
            options,
            tables: BTreeMap::new(),
            previous: BTreeMap::new(),
            single_unit_done: BTreeSet::new(),
        })
    }

    /// Probe whether a `(load, seq)` receipt exists — runs on
    /// whatever connection view the caller holds (in or out of a
    /// transaction).
    fn receipt_recorded(
        conn: &duckdb::Connection,
        load: &LoadId,
        seq: u64,
    ) -> Result<bool, DestinationError> {
        let sql = unit::receipt_exists_sql(|_| "?".into());
        let count: u64 = conn
            .query_row(&sql, params![load.as_str(), seq as i64], |row| row.get(0))
            .map_err(fatal)?;
        Ok(count > 0)
    }

    /// The full-feed stage probes the planner consumes, evaluated
    /// up front — nothing writes a stage after this point in a
    /// commit, so eager equals the old lazy per-table check.
    fn staged_nonempty(&self) -> Result<BTreeSet<TableName>, DestinationError> {
        let mut nonempty = BTreeSet::new();
        for table in staged_probe_targets(&self.tables, &self.options) {
            let sql = unit::stage_nonempty_sql(&quote(&stage_name(table.as_str())));
            let filled: bool = self
                .conn
                .query_row(&sql, [], |row| row.get(0))
                .map_err(classify)?;
            if filled {
                nonempty.insert(table.clone());
            }
        }
        Ok(nonempty)
    }

    /// Run one planned step. Every statement classifies; the merge
    /// arms re-render from the plan each time.
    fn run_step(&self, step: &Step, meta: &CommitMeta) -> Result<(), DestinationError> {
        match step {
            Step::ClearTarget { table } => {
                let sql = DuckDialect.clear_table(&quote(table.as_str()));
                self.conn.execute_batch(&sql).map_err(classify)
            }
            Step::InsertSelect { table } => {
                let (schema, _) = &self.tables[table];
                let sql = rdlt_connector_sqlcore::protocol::insert_select_sql(
                    &quote(table.as_str()),
                    &column_list(schema),
                    &quote(&stage_name(table.as_str())),
                );
                self.conn.execute_batch(&sql).map_err(classify)
            }
            Step::ScopeReplace { table, scope } => {
                let sql = rdlt_connector_sqlcore::plan::scope_replace_sql(
                    &DuckDialect,
                    &quote(table.as_str()),
                    &quote(&stage_name(table.as_str())),
                    scope,
                );
                self.conn.execute_batch(&sql).map_err(classify)
            }
            Step::MergeArm { table, arm } => {
                let (schema, mode) = &self.tables[table];
                let WriteMode::Merge { key } = mode else {
                    return Err(DestinationError::fatal(format!(
                        "internal: merge arm planned for non-merge table `{table}`"
                    )));
                };
                let root = root_of(&self.tables, table);
                let root_schema = self.tables.get(&root).map(|(s, _)| s);
                let target_sql = quote(table.as_str());
                let stage_sql = quote(&stage_name(table.as_str()));
                let cols_sql = column_list(schema);
                let plan = build_merge_plan(
                    &DuckDialect,
                    &self.options,
                    table,
                    schema,
                    key,
                    &target_sql,
                    &stage_sql,
                    &cols_sql,
                    &root,
                    quote(&stage_name(root.as_str())),
                    root_schema,
                );
                for sql in render_arm(&plan, arm) {
                    self.conn.execute_batch(&sql).map_err(classify)?;
                }
                Ok(())
            }
            Step::TruncateStage { table } => {
                let sql = format!("DELETE FROM {}", quote(&stage_name(table.as_str())));
                self.conn.execute_batch(&sql).map_err(classify)
            }
            Step::UpsertState => {
                let doc = serde_json::to_string(&meta.state)
                    .map_err(|e| DestinationError::fatal(e.to_string()))?;
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO _rdlt_state VALUES (?, ?)",
                        params![meta.state.pipeline.as_str(), doc],
                    )
                    .map(|_| ())
                    .map_err(classify)
            }
            // A duplicate receipt here is the idempotence-anomaly
            // signal — fail loudly, never absorb.
            Step::InsertReceipt => self
                .conn
                .execute(
                    "INSERT INTO _rdlt_commits VALUES (?, ?)",
                    params![meta.load_id.as_str(), meta.commit_seq as i64],
                )
                .map(|_| ())
                .map_err(classify),
        }
    }

    /// Plan and execute one commit unit inside ONE transaction.
    fn run_commit(&mut self, meta: &CommitMeta, replayed: bool) -> Result<(), DestinationError> {
        let load_committed_before = {
            let sql = unit::load_committed_sql(|_| "?".into());
            let count: u64 = self
                .conn
                .query_row(&sql, params![meta.load_id.as_str()], |row| row.get(0))
                .map_err(fatal)?;
            count > 0
        };
        let staged_nonempty = self.staged_nonempty()?;
        let empty = BTreeSet::new();
        let script = plan_commit(
            &self.tables,
            &self.options,
            &CommitContext {
                replayed,
                load_committed_before,
                single_unit_done: &self.single_unit_done,
                staged_nonempty: &staged_nonempty,
                full_load_publish: FullLoadPublish::Staged,
                cleared_targets: &empty,
            },
        )
        .map_err(|e| DestinationError::fatal(e.to_string()))?;

        self.conn.execute_batch("BEGIN").map_err(classify)?;
        let run = || -> Result<(), DestinationError> {
            for step in &script.steps {
                self.run_step(step, meta)?;
            }
            if !replayed {
                crash_point!(
                    "duck.tx.commit",
                    Err(DestinationError::fatal("injected crash at duck.tx.commit"))
                );
            }
            Ok(())
        };
        match run() {
            Ok(()) => {
                self.conn.execute_batch("COMMIT").map_err(classify)?;
                // Marks apply ONLY after the transaction committed; the
                // planner re-emits them on replay, so a crash-recovery
                // replay re-marks the single-unit discipline.
                self.single_unit_done.extend(script.marks.iter().cloned());
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }
}

#[async_trait]
impl Backend for Load {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        for sql in table_ddl(schema, self.previous.get(&schema.table)) {
            self.conn.execute_batch(&sql).map_err(classify)?;
        }
        let statements = merge_ddl(&self.options, schema, mode)
            .map_err(|e| DestinationError::fatal(e.to_string()))?;
        for (sql, unique_index) in statements {
            if let Err(e) = self.conn.execute_batch(&sql) {
                // The duplicate-key diagnosis: only a genuine
                // constraint violation on the arbiter index gets the
                // guidance wording; anything else classifies.
                if let Some(columns) = &unique_index
                    && is_constraint_violation(&e)
                {
                    return Err(DestinationError::fatal(
                        names::duplicate_merge_key_diagnosis(
                            schema.table.as_str(),
                            columns,
                            &e.to_string(),
                        ),
                    ));
                }
                return Err(classify(e));
            }
        }
        self.previous.insert(schema.table.clone(), schema.clone());
        self.tables
            .insert(schema.table.clone(), (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        crash_point!(
            "duck.append",
            Err(DestinationError::fatal("injected crash at duck.append"))
        );
        let stage = stage_name(table.as_str());
        let mut appender = self.conn.appender(&stage).map_err(classify)?;
        appender.append_record_batch(batch).map_err(classify)?;
        // Appender drop swallows errors; flush explicitly so failures
        // surface as DestinationError instead of silently losing
        // staged rows.
        appender.flush().map_err(classify)
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        Ok(
            Self::receipt_recorded(&self.conn, load_id, commit_seq)?.then(|| CommitReceipt {
                load_id: load_id.clone(),
                commit_seq,
            }),
        )
    }

    async fn replay(
        &mut self,
        meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // The staged path's replay disposition is RunScript: for a
        // replay the script is stage truncation and nothing else —
        // the redelivered rows reached no reader, so there is nothing
        // to roll back — plus the re-marking of the single-unit
        // discipline.
        debug_assert_eq!(
            unit::replay_disposition(FullLoadPublish::Staged),
            unit::ReplayDisposition::RunScript
        );
        self.run_commit(meta, true)
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // Defense in depth: the choreography probed the receipt before
        // calling publish, but the durable truth lives in THIS
        // database — re-probe so a racing writer cannot double-publish.
        let replayed = Self::receipt_recorded(&self.conn, &meta.load_id, meta.commit_seq)?;
        self.run_commit(&meta, replayed)?;
        Ok(CommitReceipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
        })
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let doc: Result<String, _> = self.conn.query_row(
            "SELECT doc FROM _rdlt_state WHERE pipeline = ?",
            params![pipeline.as_str()],
            |row| row.get(0),
        );
        match doc {
            Ok(doc) => serde_json::from_str(&doc)
                .map(Some)
                .map_err(|e| DestinationError::fatal(e.to_string())),
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(fatal(e)),
        }
    }
}

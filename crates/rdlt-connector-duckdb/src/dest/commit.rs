//! The load-session protocol: strategy arms run through the shared sqlcore
//! shapes — this destination owns SQL execution and DDL text only.
//!
//! Staging model: writes land in TEMP tables (`_rdlt_stage_*`) on the
//! session's connection. Temp tables die with the connection, so a fresh
//! `open` tears down any orphaned stage for free. `commit` moves
//! stage → target through the strategy arms, upserts the state document, and
//! records the `(load_id, commit_seq)` receipt — all in ONE DuckDB
//! transaction, so state and data become visible atomically.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;
use duckdb::Connection;
use rdlt_connector::{
    CommitMeta, CommitReceipt, DestError, LoadSession, RecordBatch, WriteMode,
    core::{PipelineId, StateDoc, TableName, TableSchema, crash_point, schema::system_columns},
};
use rdlt_connector_sqlcore::plan::{self as sqlplan, IndexSpec, TableFacts, scope_replace_sql};
use rdlt_connector_sqlcore::{
    CommitCtx, DestOptions, MergeDialect, MergeStrategy, Step, build_merge_plan, commit_script,
    insert_select_sql, render_arm, staged_probe_targets,
};

use super::dialect::DuckDialect;
use super::{
    classify, column_list, create_table_sql, fatal, is_constraint_violation, quote, sql_type,
    stage_name,
};

pub(super) struct DuckDbSession {
    /// DuckDB connections are Send but not Sync; the mutex makes the session
    /// Send while every call still runs on &mut self.
    pub(super) conn: Mutex<Connection>,
    pub(super) tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    pub(super) options: DestOptions,
    /// Single-unit discipline, PER TABLE — the rule and its message live in
    /// sqlcore; the bookkeeping mirrors the postgres session. Marked only
    /// AFTER the unit's transaction commits, and re-marked when a committed
    /// unit is replayed.
    pub(super) single_unit_done: BTreeSet<TableName>,
}

impl DuckDbSession {
    fn with_conn<T>(
        &mut self,
        f: impl FnOnce(&mut Connection) -> Result<T, DestError>,
    ) -> Result<T, DestError> {
        let mut guard = self.conn.lock().map_err(|_| fatal("connection poisoned"))?;
        f(&mut guard)
    }

    fn root_of(&self, table: &TableName) -> TableName {
        rdlt_connector_sqlcore::root_of(&self.tables, table)
    }
}

/// The old spelling of a unique merge-identity index: databases written
/// before the unique prefix was introduced named unique indexes with the
/// plain `rdlt_ix_` prefix (the shared formula's non-unique name). The old
/// name is dropped before creating the correctly-prefixed one, so such a
/// database doesn't carry two identical unique ART indexes forever.
fn legacy_unique_index_name(table: &str, columns: &[String]) -> String {
    rdlt_connector_sqlcore::names::index_name(false, table, columns)
}

fn create_index_sql(unique: bool, table: &str, columns: &[String]) -> String {
    let name = rdlt_connector_sqlcore::names::index_name(unique, table, columns);
    let unique = if unique { "UNIQUE " } else { "" };
    let cols = columns
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE {unique}INDEX IF NOT EXISTS {} ON {} ({cols})",
        quote(&name),
        quote(table)
    )
}

fn staged_nonempty(tx: &duckdb::Transaction<'_>, table: &TableName) -> Result<bool, DestError> {
    tx.query_row(
        &format!(
            "SELECT EXISTS (SELECT 1 FROM {})",
            quote(&stage_name(table))
        ),
        [],
        |row| row.get(0),
    )
    .map_err(fatal)
}

/// Execute one planned [`Step`] in the publish transaction. Every decision +
/// the order come from the planner; this renders each step's SQL through the
/// DuckDialect seam + shared renderers and runs it on the session's connection.
fn execute_step(
    tx: &duckdb::Transaction<'_>,
    tables: &BTreeMap<TableName, (TableSchema, WriteMode)>,
    options: &DestOptions,
    roots: &BTreeMap<TableName, TableName>,
    meta: &CommitMeta,
    state_json: &str,
    step: &Step,
) -> Result<(), DestError> {
    match step {
        Step::ClearTarget { table } => {
            tx.execute_batch(&DuckDialect.clear_table(&quote(table.as_str())))
                .map_err(fatal)?;
        }
        Step::InsertSelect { table } => {
            let (schema, _) = &tables[table];
            let target = quote(table.as_str());
            let stage = quote(&stage_name(table));
            tx.execute_batch(&insert_select_sql(&target, &column_list(schema), &stage))
                .map_err(fatal)?;
        }
        Step::ScopeReplace { table, scope } => {
            let target = quote(table.as_str());
            let stage = quote(&stage_name(table));
            tx.execute_batch(&scope_replace_sql(&DuckDialect, &target, &stage, scope))
                .map_err(fatal)?;
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
            let stage = quote(&stage_name(table));
            let cols = column_list(schema);
            let root_stage = quote(&stage_name(root));
            let root_schema = tables.get(root).map(|(s, _)| s);
            let dialect = DuckDialect;
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
                tx.execute_batch(&sql).map_err(fatal)?;
            }
        }
        Step::TruncateStage { table } => {
            tx.execute_batch(&DuckDialect.clear_table(&quote(&stage_name(table))))
                .map_err(fatal)?;
        }
        Step::UpsertState => {
            // State persists in the SAME transaction as the data.
            tx.execute(
                &format!(
                    "INSERT OR REPLACE INTO {} VALUES (?, ?)",
                    rdlt_connector_sqlcore::names::STATE_TABLE
                ),
                duckdb::params![meta.state.pipeline.as_str(), state_json],
            )
            .map_err(fatal)?;
        }
        Step::InsertReceipt => {
            tx.execute(
                &format!(
                    "INSERT INTO {} VALUES (?, ?)",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                duckdb::params![meta.load_id.as_str(), meta.commit_seq as i64],
            )
            .map_err(fatal)?;
        }
    }
    Ok(())
}

#[async_trait]
impl LoadSession for DuckDbSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestError> {
        let table = schema.table.clone();
        let stage = stage_name(&table);
        let previous = self.tables.get(&table).map(|(s, _)| s.clone());
        {
            let schema = schema.clone();
            self.with_conn(move |conn| {
                conn.execute_batch(&create_table_sql(schema.table.as_str(), &schema, false))
                    .map_err(fatal)?;
                conn.execute_batch(&create_table_sql(&stage, &schema, true))
                    .map_err(fatal)?;
                // Additive schema migration: add new columns; widen changed
                // ones with a cast — DuckDB's ALTER … SET DATA TYPE migrates
                // existing rows.
                for (target, is_stage) in [
                    (schema.table.as_str().to_owned(), false),
                    (stage.clone(), true),
                ] {
                    for column in &schema.columns {
                        let add = format!(
                            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                            quote(&target),
                            quote(&column.name),
                            sql_type(&column.ty, is_stage)
                        );
                        conn.execute_batch(&add).map_err(fatal)?;
                        if let Some(prev) = &previous
                            && let Some(old) = prev.column(&column.name)
                            && old.ty != column.ty
                        {
                            let widen = format!(
                                "ALTER TABLE {} ALTER COLUMN {} SET DATA TYPE {}",
                                quote(&target),
                                quote(&column.name),
                                sql_type(&column.ty, is_stage)
                            );
                            conn.execute_batch(&widen).map_err(fatal)?;
                        }
                    }
                }
                Ok(())
            })?;
        }
        // The option-vs-mode rules live in sqlcore — one rule set, identical
        // typed errors on every SQL destination.
        if !matches!(mode, WriteMode::Merge { .. }) {
            sqlplan::validate_non_merge(&self.options, schema.table.as_str()).map_err(fatal)?;
        }
        if let WriteMode::Merge { key } = mode {
            let table_str = schema.table.as_str().to_owned();
            let strategy = self.options.strategy_for(&table_str);
            let has_identity = schema.columns.iter().any(|c| c.name == system_columns::ID);
            let is_child = schema.parent.is_some();
            sqlplan::validate_merge(
                &self.options,
                &table_str,
                key,
                &TableFacts {
                    schema,
                    has_identity,
                    is_child,
                },
            )
            .map_err(fatal)?;
            if strategy == MergeStrategy::Scd2 {
                let scd2 = self.options.scd2_for(&table_str);
                // Validity columns on the TARGET only (the stage carries the
                // stream's shape); additive for pre-existing scd2 tables.
                // DDL difference vs postgres: DuckDB rejects ADD COLUMN with a
                // NOT NULL constraint. The insert arm always supplies the
                // boundary value, so the constraint was belt only; DEFAULT
                // now() still backfills pre-existing rows on a table adopting
                // scd2.
                let stmts: Vec<String> = [
                    (&scd2.valid_from, "TIMESTAMPTZ DEFAULT now()"),
                    (&scd2.valid_to, "TIMESTAMPTZ"),
                ]
                .into_iter()
                .map(|(col, extra)| {
                    format!(
                        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {extra}",
                        quote(&table_str),
                        quote(col)
                    )
                })
                .collect();
                self.with_conn(|conn| {
                    for sql in &stmts {
                        conn.execute_batch(sql).map_err(fatal)?;
                    }
                    Ok(())
                })?;
            }
            // Index plan (identity indexes + scope index) — shared shape;
            // this destination owns only the SQL text. Pre-existing duplicate
            // keys under upsert surface as the typed naming error.
            let index_stmts: Vec<(bool, Vec<String>, String)> =
                sqlplan::index_plan(&self.options, &table_str, key, has_identity, is_child)
                    .into_iter()
                    .map(|IndexSpec { unique, columns }| {
                        let sql = create_index_sql(unique, &table_str, &columns);
                        (unique, columns, sql)
                    })
                    .collect();
            for (unique, columns, sql) in index_stmts {
                if unique {
                    let legacy = legacy_unique_index_name(&table_str, &columns);
                    self.with_conn(|conn| {
                        conn.execute_batch(&format!("DROP INDEX IF EXISTS {}", quote(&legacy)))
                            .map_err(fatal)
                    })?;
                }
                // Only an actual constraint violation gets the
                // duplicate-keys diagnosis — classified on the library
                // error BEFORE wrapping, so an unrelated failure whose
                // message merely mentions violations (a table name, a
                // quoted value) can never be misdiagnosed; anything else
                // (locks, disk, I/O) surfaces as itself.
                self.with_conn(|conn| {
                    conn.execute_batch(&sql).map_err(|e| {
                        if unique && is_constraint_violation(&e) {
                            fatal(format!(
                                "table `{table_str}`: cannot create the unique index the upsert \
                                 strategy requires — existing rows duplicate the merge key \
                                 ({}); deduplicate the table or use delete_insert: {e}",
                                columns.join(", ")
                            ))
                        } else {
                            fatal(e)
                        }
                    })
                })?;
            }
        }
        self.tables.insert(table, (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError> {
        crash_point!(
            "duck.append",
            Err(DestError::fatal("injected crash at duck.append"))
        );
        let stage = stage_name(table);
        // Staging I/O is environmental: lock/disk failures classify
        // transient so the engine can retry the load instead of aborting.
        self.with_conn(move |conn| {
            let mut appender = conn.appender(&stage).map_err(classify)?;
            appender.append_record_batch(batch).map_err(classify)?;
            // Appender drop swallows errors; flush explicitly so failures surface
            // as DestError instead of silently losing staged rows.
            appender.flush().map_err(classify)?;
            Ok(())
        })
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let tables = self.tables.clone();
        let options = self.options.clone();
        let single_unit_done = self.single_unit_done.clone();
        let roots: BTreeMap<TableName, TableName> = tables
            .keys()
            .map(|t| (t.clone(), self.root_of(t)))
            .collect();
        let state_json = serde_json::to_string(&meta.state).map_err(fatal)?;

        let marks = self.with_conn(move |conn| {
            let tx = conn.transaction().map_err(classify)?;
            // Idempotence key: (load_id, commit_seq).
            let already: u64 = tx
                .query_row(
                    &format!(
                        "SELECT count(*) FROM {} WHERE load_id = ? AND commit_seq = ?",
                        rdlt_connector_sqlcore::names::COMMITS_TABLE
                    ),
                    duckdb::params![meta.load_id.as_str(), meta.commit_seq as i64],
                    |row| row.get(0),
                )
                .map_err(fatal)?;
            // Replace truncates at most once per LOAD, guarded DURABLY from
            // the receipt log — a crash-recovery session (fresh memory, same
            // load) must never re-truncate rows an earlier commit already
            // published.
            let load_committed_before: u64 = tx
                .query_row(
                    &format!(
                        "SELECT count(*) FROM {} WHERE load_id = ?",
                        rdlt_connector_sqlcore::names::COMMITS_TABLE
                    ),
                    duckdb::params![meta.load_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(fatal)?;
            let load_committed_before = load_committed_before > 0;
            let replayed = already > 0;

            // Probe the full-feed stages the planner needs. Staged-row counts
            // are INVARIANT across the publish (no stage is written during it),
            // so probing up front matches the former lazy per-table check.
            let mut staged_nonempty_set = BTreeSet::new();
            for table in staged_probe_targets(&tables, &options) {
                if staged_nonempty(&tx, table)? {
                    staged_nonempty_set.insert(table.clone());
                }
            }

            // The planner owns every decision + the ordering; this session
            // executes.
            let script = commit_script(
                &tables,
                &options,
                &CommitCtx {
                    replayed,
                    load_committed_before,
                    single_unit_done: &single_unit_done,
                    staged_nonempty: &staged_nonempty_set,
                },
            )
            .map_err(fatal)?;

            for step in &script.steps {
                execute_step(&tx, &tables, &options, &roots, &meta, &state_json, step)?;
            }

            // The redelivery window: on a fresh unit everything is published in
            // ONE transaction, so a crash at this edge must replay
            // idempotently. A replay unit only truncated stages and carried no
            // receipt/state edge (never instrumented), so the crash point stays
            // confined to the fresh path.
            if !replayed {
                crash_point!(
                    "duck.tx.commit",
                    Err(DestError::fatal("injected crash at duck.tx.commit"))
                );
            }
            tx.commit().map_err(classify)?;
            Ok(script.marks)
        })?;

        // Applied only after the unit's transaction committed (the rolled-
        // back path never reaches here).
        self.single_unit_done.extend(marks);
        Ok(receipt)
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        let pipeline = pipeline.as_str().to_owned();
        self.with_conn(move |conn| {
            let doc: Option<String> = conn
                .query_row(
                    &format!(
                        "SELECT doc FROM {} WHERE pipeline = ?",
                        rdlt_connector_sqlcore::names::STATE_TABLE
                    ),
                    duckdb::params![pipeline],
                    |row| row.get(0),
                )
                .map(Some)
                .or_else(|e| match e {
                    duckdb::Error::QueryReturnedNoRows => Ok(None),
                    other => Err(fatal(other)),
                })?;
            match doc {
                Some(json) => Ok(Some(serde_json::from_str(&json).map_err(fatal)?)),
                None => Ok(None),
            }
        })
    }
}

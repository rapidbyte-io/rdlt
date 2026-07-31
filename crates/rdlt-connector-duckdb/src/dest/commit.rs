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
    CommitMeta, CommitReceipt, DestinationError, LoadSession, RecordBatch, WriteMode,
    core::{PipelineId, StateDoc, TableName, TableSchema, crash_point},
};
use rdlt_connector_sqlcore::ensure::{self, EnsureStep, Leg, Validity};
use rdlt_connector_sqlcore::plan::{ValidateError, scope_replace_sql};
use rdlt_connector_sqlcore::protocol::unit;
use rdlt_connector_sqlcore::{
    CommitContext, DestinationOptions, FullLoadPublish, MergeDialect, Step, build_merge_plan,
    commit_script, insert_select_sql, render_arm, staged_probe_targets,
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
    pub(super) options: DestinationOptions,
    /// Single-unit discipline, PER TABLE — the rule and its message live in
    /// sqlcore; the bookkeeping mirrors the postgres session. Marked only
    /// AFTER the unit's transaction commits, and re-marked when a committed
    /// unit is replayed.
    pub(super) single_unit_done: BTreeSet<TableName>,
}

impl DuckDbSession {
    fn with_conn<T>(
        &mut self,
        f: impl FnOnce(&mut Connection) -> Result<T, DestinationError>,
    ) -> Result<T, DestinationError> {
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
    // The statement is sqlcore's; only the quoting is this destination's.
    rdlt_connector_sqlcore::names::create_index_sql(unique, table, columns, quote)
}

// ---- Ensure: rendering, separated from execution ----
//
// These build the exact statement sequence `ensure_table` runs, in order, and
// touch no connection. Separating them is what makes the emitted DDL testable
// at all: executing it needs a live database, but comparing it needs nothing.

/// One ensure statement plus what the caller must know to classify its
/// failure. A unique-index failure is the only one carrying a special
/// diagnosis, so it is the only kind distinguished here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnsureStmt {
    pub(crate) sql: String,
    pub(crate) unique_index: Option<Vec<String>>,
}

impl EnsureStmt {
    fn plain(sql: String) -> Self {
        Self {
            sql,
            unique_index: None,
        }
    }
}

/// Phase 1 — the table statements: create BOTH legs, then add and widen their
/// columns. Unlike the postgres destination this always builds a stage leg,
/// because every write mode here publishes through one.
///
/// `previous` is the schema THIS SESSION last ensured, never the live catalog
/// — that is what makes the widen a within-run rule.
pub(crate) fn table_ddl_stmts(schema: &TableSchema, previous: Option<&TableSchema>) -> Vec<String> {
    // Every write mode here publishes through a stage, which is what
    // `Staged` says — so both legs exist regardless of mode, unlike the
    // postgres destination.
    let plan = ensure::table_plan(
        schema,
        &WriteMode::Append,
        rdlt_connector_sqlcore::FullLoadPublish::Staged,
        previous,
    );
    let stage = stage_name(&schema.table);
    let leg_name = |leg: Leg| match leg {
        Leg::Target => schema.table.as_str().to_owned(),
        Leg::Stage => stage.clone(),
    };
    let mut out = Vec::new();
    for step in plan {
        match step {
            EnsureStep::Table { leg } => {
                out.push(create_table_sql(&leg_name(leg), schema, leg == Leg::Stage))
            }
            // Additive schema migration: add new columns; widen changed ones
            // with a cast — DuckDB's ALTER … SET DATA TYPE migrates existing
            // rows, so no USING clause is needed or accepted.
            EnsureStep::Column { leg, column } => out.push(format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                quote(&leg_name(leg)),
                quote(&schema.columns[column].name),
                sql_type(&schema.columns[column].column_type, leg == Leg::Stage)
            )),
            EnsureStep::Widen { leg, column } => out.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} SET DATA TYPE {}",
                quote(&leg_name(leg)),
                quote(&schema.columns[column].name),
                sql_type(&schema.columns[column].column_type, leg == Leg::Stage)
            )),
            _ => unreachable!("phase 1 plans relations and columns only"),
        }
    }
    out
}

/// Phase 2 — option validation, then the scd2 validity columns and the index
/// plan. Runs AFTER phase 1 has been applied, preserving today's failure
/// point. Non-merge modes validate and return nothing to execute.
pub(crate) fn merge_ensure_stmts(
    options: &DestinationOptions,
    schema: &TableSchema,
    mode: &WriteMode,
) -> Result<Vec<EnsureStmt>, ValidateError> {
    let table = schema.table.as_str();
    let scd2 = options.scd2_for(table);
    let mut out = Vec::new();
    for step in ensure::merge_plan(options, schema, mode)? {
        match step {
            // Validity columns on the TARGET only (the stage carries the
            // stream's shape); additive for pre-existing scd2 tables. DDL
            // difference vs postgres: DuckDB rejects ADD COLUMN with a NOT
            // NULL constraint. The insert arm always supplies the boundary
            // value, so the constraint was belt only; DEFAULT now() still
            // backfills pre-existing rows on a table adopting scd2.
            EnsureStep::Validity(which) => {
                let (col, extra) = match which {
                    Validity::From => (&scd2.valid_from, "TIMESTAMPTZ DEFAULT now()"),
                    Validity::To => (&scd2.valid_to, "TIMESTAMPTZ"),
                };
                out.push(EnsureStmt::plain(format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {extra}",
                    quote(table),
                    quote(col)
                )));
            }
            EnsureStep::Index(spec) => {
                if spec.unique {
                    // A unique index this destination created under an older
                    // naming scheme would otherwise survive beside the current
                    // one.
                    out.push(EnsureStmt::plain(format!(
                        "DROP INDEX IF EXISTS {}",
                        quote(&legacy_unique_index_name(table, &spec.columns))
                    )));
                }
                out.push(EnsureStmt {
                    sql: create_index_sql(spec.unique, table, &spec.columns),
                    unique_index: spec.unique.then_some(spec.columns),
                });
            }
            _ => unreachable!("phase 2 plans validity columns and indexes only"),
        }
    }
    Ok(out)
}

fn staged_nonempty(
    tx: &duckdb::Transaction<'_>,
    table: &TableName,
) -> Result<bool, DestinationError> {
    tx.query_row(
        &unit::stage_nonempty_sql(&quote(&stage_name(table))),
        [],
        |row| row.get(0),
    )
    .map_err(classify)
}

/// Execute one planned [`Step`] in the publish transaction. Every decision +
/// the order come from the planner; this renders each step's SQL through the
/// DuckDialect seam + shared renderers and runs it on the session's connection.
/// Execute one planned step. Failures CLASSIFY (shared rule with the
/// postgres executor): environmental errors ride the engine's retry
/// budget; deterministic ones — constraint violations included, e.g. a
/// duplicate receipt, the idempotence-anomaly signal — fail loudly.
fn execute_step(
    tx: &duckdb::Transaction<'_>,
    tables: &BTreeMap<TableName, (TableSchema, WriteMode)>,
    options: &DestinationOptions,
    roots: &BTreeMap<TableName, TableName>,
    meta: &CommitMeta,
    state_json: &str,
    step: &Step,
) -> Result<(), DestinationError> {
    match step {
        Step::ClearTarget { table } => {
            tx.execute_batch(&DuckDialect.clear_table(&quote(table.as_str())))
                .map_err(classify)?;
        }
        Step::InsertSelect { table } => {
            let (schema, _) = &tables[table];
            let target = quote(table.as_str());
            let stage = quote(&stage_name(table));
            tx.execute_batch(&insert_select_sql(&target, &column_list(schema), &stage))
                .map_err(classify)?;
        }
        Step::ScopeReplace { table, scope } => {
            let target = quote(table.as_str());
            let stage = quote(&stage_name(table));
            tx.execute_batch(&scope_replace_sql(&DuckDialect, &target, &stage, scope))
                .map_err(classify)?;
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
                tx.execute_batch(&sql).map_err(classify)?;
            }
        }
        Step::TruncateStage { table } => {
            tx.execute_batch(&DuckDialect.clear_table(&quote(&stage_name(table))))
                .map_err(classify)?;
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
            .map_err(classify)?;
        }
        Step::InsertReceipt => {
            tx.execute(
                &format!(
                    "INSERT INTO {} VALUES (?, ?)",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                duckdb::params![meta.load_id.as_str(), meta.commit_seq as i64],
            )
            .map_err(classify)?;
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
    ) -> Result<(), DestinationError> {
        // Rendering lives beside the other DDL helpers; this drives it. The
        // split is what makes the emitted sequence testable without a
        // database — the statements are compared as data, and only their
        // execution needs a connection.
        let table = schema.table.clone();
        let previous = self.tables.get(&table).map(|(s, _)| s.clone());
        let table_stmts = table_ddl_stmts(schema, previous.as_ref());
        self.with_conn(move |conn| {
            for sql in &table_stmts {
                conn.execute_batch(sql).map_err(classify)?;
            }
            Ok(())
        })?;
        let table_str = table.as_str().to_owned();
        for stmt in merge_ensure_stmts(&self.options, schema, mode).map_err(fatal)? {
            let table_str = table_str.clone();
            self.with_conn(move |conn| {
                conn.execute_batch(&stmt.sql).map_err(|e| {
                    // Only an actual constraint violation gets the
                    // duplicate-keys diagnosis — classified on the library
                    // error BEFORE wrapping, so an unrelated failure whose
                    // message merely mentions violations (a table name, a
                    // quoted value) can never be misdiagnosed; anything else
                    // (locks, disk, I/O) surfaces as itself.
                    match &stmt.unique_index {
                        Some(columns) if is_constraint_violation(&e) => fatal(
                            rdlt_connector_sqlcore::names::duplicate_merge_key_diagnosis(
                                &table_str,
                                columns,
                                &e.to_string(),
                            ),
                        ),
                        _ => fatal(e),
                    }
                })
            })?;
        }
        self.tables.insert(table, (schema.clone(), mode.clone()));
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
        let stage = stage_name(table);
        // Staging I/O is environmental: lock/disk failures classify
        // transient so the engine can retry the load instead of aborting.
        self.with_conn(move |conn| {
            let mut appender = conn.appender(&stage).map_err(classify)?;
            appender.append_record_batch(batch).map_err(classify)?;
            // Appender drop swallows errors; flush explicitly so failures surface
            // as DestinationError instead of silently losing staged rows.
            appender.flush().map_err(classify)?;
            Ok(())
        })
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
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
                    &unit::receipt_exists_sql(|_| "?".to_owned()),
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
                    &unit::load_committed_sql(|_| "?".to_owned()),
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
                &CommitContext {
                    replayed,
                    load_committed_before,
                    single_unit_done: &single_unit_done,
                    staged_nonempty: &staged_nonempty_set,
                    // DuckDB stays STAGED. Direct-to-target needs the writes
                    // and the clear inside one transaction the session holds
                    // open across `write` calls; this session appends through
                    // an Appender opened per write instead, so the guarantee
                    // is not available here without a separate redesign.
                    // Recorded as a deferral, not an oversight — the emitted
                    // program is byte-identical to before this option existed.
                    full_load_publish: FullLoadPublish::Staged,
                    // Unused on the staged path: the planner emits ClearTarget
                    // itself, inside the publish transaction.
                    cleared_targets: &BTreeSet::new(),
                },
            )
            .map_err(fatal)?;

            // A redelivered unit on the STAGED path runs the planner's
            // program — which for a replay is stage truncation and nothing
            // else — and commits it. That is what reclaims the redelivered
            // rows; they reached no reader, so there is nothing to roll back.
            // The inverse choice belongs to direct-publish destinations, and
            // the shared planner is what keeps the two from being confused.
            debug_assert_eq!(
                unit::replay_disposition(FullLoadPublish::Staged),
                unit::ReplayDisposition::RunScript
            );
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
                    Err(DestinationError::fatal("injected crash at duck.tx.commit"))
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

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
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

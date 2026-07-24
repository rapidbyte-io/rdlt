//! The load-session protocol (feature 013: strategy arms via the shared
//! sqlcore shapes — this destination owns SQL execution and DDL text only).
//!
//! Staging model: writes land in TEMP tables (`_rdlt_stage_*`) on the
//! session's connection. Temp tables die with the connection, which yields
//! clause D4 (staged teardown on a fresh `open`) for free. `commit` moves
//! stage → target through the strategy arms, upserts the state document, and
//! records the `(load_id, commit_seq)` receipt — all in ONE DuckDB
//! transaction (clauses D1–D3).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;
use duckdb::Connection;
use rdlt_connector::{
    CommitMeta, CommitReceipt, DestError, LoadSession, RecordBatch, WriteMode,
    core::{PipelineId, StateDoc, TableName, TableSchema, crash_point, schema::system_columns},
};
use rdlt_connector_sqlcore::plan::{
    self as sqlplan, TableFacts, identity_delete_insert_sql, keyed_delete_insert_sql,
    keyed_upsert_sql, scd2_merge_sql, scope_replace_sql,
};
use rdlt_connector_sqlcore::{DestOptions, HardDelete, MergePlan, MergeStrategy};

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
    /// Single-unit discipline, PER TABLE (MR5 / scd2 S6 — the rule and its
    /// message live in sqlcore; the bookkeeping mirrors the postgres session
    /// clause for clause). Marked only AFTER the unit's transaction commits;
    /// re-marked on the D3 replay branch.
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
        let mut current = table.clone();
        for _ in 0..64 {
            match self
                .tables
                .get(&current)
                .and_then(|(s, _)| s.parent.as_ref())
            {
                Some(link) => current = link.parent.clone(),
                None => break,
            }
        }
        current
    }
}

/// `CREATE [UNIQUE] INDEX IF NOT EXISTS` with the same `rdlt_ix_` naming
/// convention as postgres (M5).
/// The pre-rename spelling of a unique merge-identity index (this branch's
/// earlier commits used `rdlt_ix_` for unique too) — dropped before creating
/// the correctly-prefixed one so a database written mid-branch doesn't carry
/// two identical unique ART indexes forever (013 review finding 7).
fn legacy_unique_index_name(table: &str, columns: &[String]) -> String {
    format!(
        "{}_{}",
        rdlt_connector_sqlcore::names::INDEX_PREFIX,
        rdlt_connector::core::naming::ident_hash(&format!("{table}:{}", columns.join(",")), 16)
    )
}

fn create_index_sql(unique: bool, table: &str, columns: &[String]) -> String {
    let prefix = if unique {
        rdlt_connector_sqlcore::names::UNIQUE_INDEX_PREFIX
    } else {
        rdlt_connector_sqlcore::names::INDEX_PREFIX
    };
    let name = format!(
        "{prefix}_{}",
        rdlt_connector::core::naming::ident_hash(&format!("{table}:{}", columns.join(",")), 16)
    );
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
                // Migrations (clause D5): add new columns; widen changed ones with a
                // cast — DuckDB's ALTER … SET DATA TYPE migrates existing rows.
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
        // Feature 013: the option-vs-mode rules live in sqlcore (SM1) — one
        // rule set, identical typed errors on every SQL destination.
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
                // DDL difference vs postgres (destination-owned, recorded in
                // the 013 matrix): DuckDB rejects ADD COLUMN with a NOT NULL
                // constraint. The insert arm always supplies the boundary
                // value, so the constraint was belt only; DEFAULT now() still
                // backfills pre-existing rows on a table adopting scd2.
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
            // Index plan (008 data-model + 010 scope index) — shared shape;
            // this destination owns only the SQL text. M3: pre-existing
            // duplicate keys under upsert get the typed naming error.
            let index_stmts: Vec<(bool, Vec<String>, String)> =
                sqlplan::index_plan(&self.options, &table_str, key, has_identity, is_child)
                    .into_iter()
                    .map(|(unique, columns)| {
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
            // Clause D3: idempotence by (load_id, commit_seq).
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
            let staged_nonempty =
                |tx: &duckdb::Transaction<'_>, table: &TableName| -> Result<bool, DestError> {
                    tx.query_row(
                        &format!(
                            "SELECT EXISTS (SELECT 1 FROM {})",
                            quote(&stage_name(table))
                        ),
                        [],
                        |row| row.get(0),
                    )
                    .map_err(fatal)
                };
            let mut marks: Vec<TableName> = Vec::new();
            if already > 0 {
                // D3 replay of a unit that DID commit server-side: the merge
                // SQL never re-runs, but the single-unit discipline must
                // still count this unit — the redelivered stage carries the
                // same rows the committed one did.
                for (table, (_, mode)) in &tables {
                    if !matches!(mode, WriteMode::Merge { .. }) {
                        continue;
                    }
                    let scoped = options.merge_key_for(table.as_str()).is_some();
                    let retire = options.strategy_for(table.as_str()) == MergeStrategy::Scd2
                        && options.scd2_for(table.as_str()).absent
                            == rdlt_connector_sqlcore::AbsentPolicy::Retire;
                    if (scoped || retire)
                        && !single_unit_done.contains(table)
                        && staged_nonempty(&tx, table)?
                    {
                        marks.push(table.clone());
                    }
                }
                for table in tables.keys() {
                    tx.execute_batch(&format!("DELETE FROM {}", quote(&stage_name(table))))
                        .map_err(fatal)?;
                }
                tx.commit().map_err(classify)?;
                return Ok(marks);
            }

            for (table, (schema, mode)) in &tables {
                // Feature 006: a schema without the per-row identity column
                // is a STRUCTURED stream's table — merge (if requested) goes
                // by key.
                let schema_has_identity =
                    schema.columns.iter().any(|c| c.name == system_columns::ID);
                let target = quote(table.as_str());
                let stage = quote(&stage_name(table));
                // Publishes are ALWAYS by name (finding #4).
                let cols = column_list(schema);
                match mode {
                    WriteMode::Append => {
                        tx.execute_batch(&format!(
                            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}"
                        ))
                        .map_err(fatal)?;
                    }
                    WriteMode::Replace => {
                        if load_committed_before == 0 {
                            tx.execute_batch(&format!("DELETE FROM {target}"))
                                .map_err(fatal)?;
                        }
                        tx.execute_batch(&format!(
                            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}"
                        ))
                        .map_err(fatal)?;
                    }
                    WriteMode::Merge { key } => {
                        // Feature 013: the strategy arms are the SHARED
                        // sqlcore shapes through the DuckDB dialect — the
                        // same plan the postgres destination executes.
                        let strategy = options.strategy_for(table.as_str());
                        let scoped = options.merge_key_for(table.as_str());
                        let scd2 = (strategy == MergeStrategy::Scd2)
                            .then(|| options.scd2_for(table.as_str()));
                        let retire = scd2.as_ref().is_some_and(|s| {
                            s.absent == rdlt_connector_sqlcore::AbsentPolicy::Retire
                        });
                        // Single-unit discipline, PER TABLE (MR5 + scd2 S6):
                        // scope replacement and absent-retire interpret the
                        // stage as "the complete truth" — sound only when
                        // THIS TABLE's full feed arrives in one commit unit.
                        if scoped.is_some() || retire {
                            if !staged_nonempty(&tx, table)? {
                                continue; // nothing delivered for THIS table this unit
                            }
                            if single_unit_done.contains(table) {
                                // 013 review finding 9: cite the rule that
                                // FIRED — under scd2 the retire rule (S6)
                                // governs even when a merge_key scopes it.
                                return Err(fatal(sqlplan::single_unit_violation(
                                    table.as_str(),
                                    scoped.is_some() && !retire,
                                )));
                            }
                            marks.push(table.clone());
                        }
                        // Feature 010 (MR3/MR4): scope replacement runs
                        // BEFORE the strategy arm, inside the same
                        // transaction. NOT for scd2 (013 G1): there the
                        // merge_key scopes RETIREMENT inside the arm —
                        // deleting scope rows would destroy history.
                        if let Some(scope) = scoped
                            && strategy != MergeStrategy::Scd2
                        {
                            tx.execute_batch(&scope_replace_sql(
                                &DuckDialect,
                                &target,
                                &stage,
                                scope,
                            ))
                            .map_err(fatal)?;
                        }
                        let root = &roots[table];
                        let plan = MergePlan {
                            dialect: &DuckDialect,
                            target: &target,
                            stage: &stage,
                            cols: &cols,
                            schema,
                            key,
                            root_stage: quote(&stage_name(root)),
                            is_child: table != root,
                            hard_delete: options.hard_delete_for(root.as_str()).and_then(|col| {
                                let root_schema = tables.get(root).map(|(s, _)| s)?;
                                Some(HardDelete::new(col, root_schema, &DuckDialect))
                            }),
                            dedup_sort: options.dedup_sort_for(table.as_str()),
                            merge_scope: scoped,
                        };
                        let stmts = match (schema_has_identity, strategy) {
                            (false, MergeStrategy::DeleteInsert) => keyed_delete_insert_sql(&plan),
                            (false, MergeStrategy::Upsert) => keyed_upsert_sql(&plan),
                            (true, MergeStrategy::DeleteInsert) => {
                                identity_delete_insert_sql(&plan)
                            }
                            (false, MergeStrategy::Scd2) => {
                                let scd2 = scd2
                                    .as_ref()
                                    .expect("scd2 options resolved with the strategy");
                                scd2_merge_sql(&plan, scd2)
                            }
                            (true, MergeStrategy::Upsert) => {
                                // Unreachable: ensure_table rejected it (M7).
                                return Err(fatal(format!(
                                    "table `{table}`: upsert on a shredded stream"
                                )));
                            }
                            (true, MergeStrategy::Scd2) => {
                                // Unreachable: ensure_table rejected it (S1).
                                return Err(fatal(format!(
                                    "table `{table}`: scd2 on a shredded stream"
                                )));
                            }
                        };
                        for sql in stmts {
                            tx.execute_batch(&sql).map_err(fatal)?;
                        }
                    }
                }
            }
            // Truncate stages only after ALL tables published: child-table
            // merges read the ROOT's stage for their delete-by-root-id
            // subquery.
            for table in tables.keys() {
                tx.execute_batch(&format!("DELETE FROM {}", quote(&stage_name(table))))
                    .map_err(fatal)?;
            }

            // Clause D2: state persists in the SAME transaction as the data.
            tx.execute(
                &format!(
                    "INSERT OR REPLACE INTO {} VALUES (?, ?)",
                    rdlt_connector_sqlcore::names::STATE_TABLE
                ),
                duckdb::params![meta.state.pipeline.as_str(), state_json],
            )
            .map_err(fatal)?;
            tx.execute(
                &format!(
                    "INSERT INTO {} VALUES (?, ?)",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                duckdb::params![meta.load_id.as_str(), meta.commit_seq as i64],
            )
            .map_err(fatal)?;
            // The canonical redelivery window (pg-mirroring placement, 013
            // review finding 5): everything published in ONE transaction; a
            // crash at this edge must replay idempotently (D3).
            crash_point!(
                "duck.tx.commit",
                Err(DestError::fatal("injected crash at duck.tx.commit"))
            );
            tx.commit().map_err(classify)?;
            Ok(marks)
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

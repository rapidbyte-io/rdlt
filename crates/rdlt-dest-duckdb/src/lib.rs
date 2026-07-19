//! # rdlt-dest-duckdb — DuckDB destination
//!
//! Arrow-native ingestion with struct-preserving lowering (real STRUCT/LIST columns),
//! temp-table staging, and transactional commits that persist state atomically with
//! data. Depends on the SPI only.
//!
//! Staging model: writes land in TEMP tables (`_rdlt_stage_*`) on the session's
//! connection. Temp tables die with the connection, which yields clause D4 (staged
//! teardown on a fresh `open`) for free. `commit` moves stage → target, upserts the
//! state document, and records the `(load_id, commit_seq)` receipt — all in ONE
//! DuckDB transaction (clauses D1–D3).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use duckdb::Connection;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, DestCapabilities, DestError, Destination,
    LoadSession, OpenCtx, RecordBatch, WriteMode,
    core::{
        ColumnType, LoadId, LogicalType, PipelineId, StateDoc, TableName, TableSchema,
        naming::IdentRules, schema::system_columns,
    },
};

/// One shared database instance; sessions and probes clone connections from it.
/// (Two independent `Connection::open`s on the same FILE are two database
/// instances — the second cannot see the first's un-checkpointed catalog.)
#[derive(Clone)]
pub struct DuckDb {
    db: std::sync::Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for DuckDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDb").finish_non_exhaustive()
    }
}

impl DuckDb {
    /// Open (or create) a DuckDB database file as a destination.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, DestError> {
        let conn = Connection::open(path.into()).map_err(fatal)?;
        Ok(Self {
            db: std::sync::Arc::new(Mutex::new(conn)),
        })
    }

    fn clone_conn(&self) -> Result<Connection, DestError> {
        let guard = self.db.lock().map_err(|_| fatal("connection poisoned"))?;
        guard.try_clone().map_err(fatal)
    }

    /// Test/inspection helper: count reader-visible rows.
    pub fn count_rows(&self, table: &str) -> Result<u64, DestError> {
        let conn = self.clone_conn()?;
        let count: u64 = conn
            .query_row(
                &format!("SELECT count(*) FROM {}", quote(table)),
                [],
                |row| row.get(0),
            )
            .map_err(fatal)?;
        Ok(count)
    }

    /// Test/inspection helper: run a scalar query.
    pub fn query_string(&self, sql: &str) -> Result<String, DestError> {
        let conn = self.clone_conn()?;
        conn.query_row(sql, [], |row| row.get::<_, String>(0))
            .map_err(fatal)
    }
}

fn fatal(e: impl std::fmt::Display) -> DestError {
    DestError::fatal(e.to_string())
}

fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn stage_name(table: &TableName) -> String {
    // Hashed: bounded length regardless of table-name length, collision-safe.
    format!(
        "_rdlt_stage_{}",
        rdlt_connector::core::naming::ident_hash(table.as_str(), 16)
    )
}

/// Quoted, comma-joined column list from the session schema — publishes are ALWAYS
/// by name: the persistent target's column order is historical while the temp stage
/// uses this run's order, so positional `SELECT *` corrupts or breaks on drift
/// (review finding #4).
fn column_list(schema: &TableSchema) -> String {
    schema
        .columns
        .iter()
        .map(|c| quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// DuckDB SQL type for a logical column type — struct-native lowering.
fn sql_type(ty: &ColumnType) -> String {
    match ty {
        ColumnType::Scalar { scalar } => match scalar {
            LogicalType::Bool => "BOOLEAN".into(),
            LogicalType::Int64 => "BIGINT".into(),
            LogicalType::Float64 => "DOUBLE".into(),
            LogicalType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
            // Json lowers to text (declared via json_type: false); Uuid as text for
            // portability with the hex `_rdlt_id` convention.
            LogicalType::Utf8 | LogicalType::Uuid | LogicalType::Json => "VARCHAR".into(),
            LogicalType::Binary => "BLOB".into(),
            LogicalType::TimestampTz => "TIMESTAMP WITH TIME ZONE".into(),
            LogicalType::TimestampNaive => "TIMESTAMP".into(),
            LogicalType::Date => "DATE".into(),
            LogicalType::Time => "TIME".into(),
        },
        ColumnType::Struct { fields } => {
            let inner = fields
                .iter()
                .map(|f| format!("{} {}", quote(&f.name), sql_type(&f.ty)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT({inner})")
        }
        ColumnType::ScalarList { item } => {
            format!("{}[]", sql_type(&ColumnType::scalar(*item)))
        }
    }
}

fn create_table_sql(name: &str, schema: &TableSchema, temp: bool) -> String {
    let columns = schema
        .columns
        .iter()
        .map(|c| format!("{} {}", quote(&c.name), sql_type(&c.ty)))
        .collect::<Vec<_>>()
        .join(", ");
    let temp = if temp { "TEMP " } else { "" };
    format!(
        "CREATE {temp}TABLE IF NOT EXISTS {} ({columns})",
        quote(name)
    )
}

#[async_trait]
impl Destination for DuckDb {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("duckdb", env!("CARGO_PKG_VERSION"))
    }

    fn capabilities(&self) -> DestCapabilities {
        DestCapabilities {
            merge: true,
            structs: true,
            scalar_lists: true,
            json_type: false, // Json lowers to VARCHAR in v1 — declared honestly
            decimal: true,
            ident_rules: IdentRules::default(),
        }
    }

    async fn open(&self, _ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError> {
        // A cloned connection shares the database instance but has its OWN temp-table
        // catalog — a dead session's staged temp tables are unreachable (clause D4).
        let conn = self.clone_conn()?;
        // Meta tables carry the correctness protocol (contracts/persisted-formats §1).
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _rdlt_state (pipeline VARCHAR PRIMARY KEY, doc VARCHAR);
             CREATE TABLE IF NOT EXISTS _rdlt_commits (
                 load_id VARCHAR, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));",
        )
        .map_err(fatal)?;
        Ok(Box::new(DuckDbSession {
            conn: Mutex::new(conn),
            tables: BTreeMap::new(),
            replaced: BTreeSet::new(),
            last_replace_load: None,
        }))
    }
}

struct DuckDbSession {
    /// DuckDB connections are Send but not Sync; the mutex makes the session Send
    /// while every call still runs on &mut self.
    conn: Mutex<Connection>,
    tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    replaced: BTreeSet<TableName>,
    last_replace_load: Option<LoadId>,
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
                for target in [schema.table.as_str().to_owned(), stage.clone()] {
                    for column in &schema.columns {
                        let add = format!(
                            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                            quote(&target),
                            quote(&column.name),
                            sql_type(&column.ty)
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
                                sql_type(&column.ty)
                            );
                            conn.execute_batch(&widen).map_err(fatal)?;
                        }
                    }
                }
                Ok(())
            })?;
        }
        self.tables.insert(table, (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(&mut self, table: &TableName, batch: RecordBatch) -> Result<(), DestError> {
        let stage = stage_name(table);
        self.with_conn(move |conn| {
            let mut appender = conn.appender(&stage).map_err(fatal)?;
            appender.append_record_batch(batch).map_err(fatal)?;
            // Appender drop swallows errors; flush explicitly so failures surface
            // as DestError instead of silently losing staged rows.
            appender.flush().map_err(fatal)?;
            Ok(())
        })
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestError> {
        // Replace bookkeeping is per load.
        if self.last_replace_load.as_ref() != Some(&meta.load_id) {
            self.replaced.clear();
            self.last_replace_load = Some(meta.load_id.clone());
        }

        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let tables = self.tables.clone();
        let roots: BTreeMap<TableName, TableName> = tables
            .keys()
            .map(|t| (t.clone(), self.root_of(t)))
            .collect();
        let mut replaced = std::mem::take(&mut self.replaced);
        let state_json = serde_json::to_string(&meta.state).map_err(fatal)?;

        let result = self.with_conn(move |conn| {
            let tx = conn.transaction().map_err(fatal)?;
            // Clause D3: idempotence by (load_id, commit_seq).
            let already: u64 = tx
                .query_row(
                    "SELECT count(*) FROM _rdlt_commits WHERE load_id = ? AND commit_seq = ?",
                    duckdb::params![meta.load_id.as_str(), meta.commit_seq as i64],
                    |row| row.get(0),
                )
                .map_err(fatal)?;
            if already > 0 {
                // Prior receipt: publish nothing, discard the staged span.
                for table in tables.keys() {
                    tx.execute_batch(&format!("DELETE FROM {}", quote(&stage_name(table))))
                        .map_err(fatal)?;
                }
                tx.commit().map_err(fatal)?;
                return Ok((replaced, true));
            }

            for (table, (schema, mode)) in &tables {
                let target = quote(table.as_str());
                let stage = quote(&stage_name(table));
                // Publishes are ALWAYS by name: the persistent target's column order
                // is historical while the temp stage uses this run's order, so a
                // positional `SELECT *` corrupts or breaks on drift (finding #4).
                let cols = column_list(schema);
                match mode {
                    WriteMode::Append => {
                        tx.execute_batch(&format!(
                            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}"
                        ))
                        .map_err(fatal)?;
                    }
                    WriteMode::Replace => {
                        if replaced.insert(table.clone()) {
                            tx.execute_batch(&format!("DELETE FROM {target}"))
                                .map_err(fatal)?;
                        }
                        tx.execute_batch(&format!(
                            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}"
                        ))
                        .map_err(fatal)?;
                    }
                    WriteMode::Merge { .. } => {
                        let root = &roots[table];
                        let root_stage = quote(&stage_name(root));
                        let id_col = if table == root {
                            system_columns::ID
                        } else {
                            system_columns::ROOT_ID
                        };
                        // Subtree replacement by root id (design doc §5.4), then
                        // insert with DETERMINISTIC in-batch dedup: rowid reflects
                        // append order in the stage, so "last wins" is real
                        // (finding #7 — no ORDER BY meant "arbitrary wins").
                        tx.execute_batch(&format!(
                            "DELETE FROM {target} WHERE {} IN \
                             (SELECT {} FROM {root_stage})",
                            quote(id_col),
                            quote(system_columns::ID),
                        ))
                        .map_err(fatal)?;
                        tx.execute_batch(&format!(
                            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage} \
                             QUALIFY row_number() OVER \
                             (PARTITION BY {} ORDER BY rowid DESC) = 1",
                            quote(system_columns::ID),
                        ))
                        .map_err(fatal)?;
                    }
                }
            }
            // Truncate stages only after ALL tables published: child-table merges
            // read the ROOT's stage for their delete-by-root-id subquery.
            for table in tables.keys() {
                tx.execute_batch(&format!("DELETE FROM {}", quote(&stage_name(table))))
                    .map_err(fatal)?;
            }

            // Clause D2: state persists in the SAME transaction as the data.
            tx.execute(
                "INSERT OR REPLACE INTO _rdlt_state VALUES (?, ?)",
                duckdb::params![meta.state.pipeline.as_str(), state_json],
            )
            .map_err(fatal)?;
            tx.execute(
                "INSERT INTO _rdlt_commits VALUES (?, ?)",
                duckdb::params![meta.load_id.as_str(), meta.commit_seq as i64],
            )
            .map_err(fatal)?;
            tx.commit().map_err(fatal)?;
            Ok((replaced, false))
        });

        match result {
            Ok((replaced, _idempotent_hit)) => {
                self.replaced = replaced;
                Ok(receipt)
            }
            Err(e) => Err(e),
        }
    }

    async fn read_state(&mut self, pipeline: &PipelineId) -> Result<Option<StateDoc>, DestError> {
        let pipeline = pipeline.as_str().to_owned();
        self.with_conn(move |conn| {
            let doc: Option<String> = conn
                .query_row(
                    "SELECT doc FROM _rdlt_state WHERE pipeline = ?",
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

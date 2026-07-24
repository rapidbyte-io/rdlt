//! # DuckDB destination
//!
//! Arrow-native ingestion with struct-preserving lowering (real STRUCT/LIST
//! columns), temp-table staging, and transactional commits that persist state
//! atomically with data. Depends on the SPI + the shared merge core
//! (rdlt-connector-sqlcore) only.
//!
//! The full destination-options vocabulary (merge strategies, hard_delete,
//! dedup_sort, merge_key, scd2) executes through the SHARED sqlcore shapes via
//! [`dialect::DuckDialect`] — the same plans, validation, and typed errors as
//! the postgres destination. `Json` columns land as native DuckDB JSON
//! (probe-verified, tests/probes.rs).
//!
//! Module layout: [`commit`] the load-session protocol + strategy execution,
//! [`dialect`] the SQL-text seam.

mod commit;
mod dialect;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use duckdb::Connection;
use rdlt_connector::{
    ConnectorSpec, DestCapabilities, DestError, Destination, LoadSession, OpenCtx,
    core::{ColumnType, LogicalType, TableName, TableSchema, naming::IdentRules},
};
pub use rdlt_connector_sqlcore::{
    AbsentPolicy, DedupSort, DestOptions, MergeStrategy, Scd2Options, SortOrder, TableOptions,
};

/// One shared database instance; sessions and probes clone connections from it.
/// (Two independent `Connection::open`s on the same FILE are two database
/// instances — the second cannot see the first's un-checkpointed catalog.)
#[derive(Clone)]
pub struct DuckDb {
    db: std::sync::Arc<Mutex<Connection>>,
    options: DestOptions,
    /// Settings/extensions, REPLAYED on every session connection:
    /// `try_clone` opens a NEW DuckDB session that inherits neither
    /// session-scoped SETs nor LOADs — applying them only on the builder
    /// connection would leave the session that actually writes silently
    /// unconfigured.
    session_setup: Vec<SetupStmt>,
}

#[derive(Debug, Clone)]
enum SetupStmt {
    Setting { key: String, value: String },
    Extension { name: String },
}

impl std::fmt::Debug for DuckDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDb").finish_non_exhaustive()
    }
}

impl DuckDb {
    /// Open (or create) a DuckDB database file as a destination.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, DestError> {
        // A locked or I/O-pressured file is recoverable — classified
        // transient so the engine retries instead of aborting the run.
        let conn = Connection::open(path.into()).map_err(classify)?;
        Ok(Self {
            db: std::sync::Arc::new(Mutex::new(conn)),
            options: DestOptions::default(),
            session_setup: Vec::new(),
        })
    }

    /// Strategy/hard-delete/refinement options — the SAME vocabulary as the
    /// postgres destination. Validated here; errors name the field.
    pub fn options(mut self, options: DestOptions) -> Result<Self, DestError> {
        options.validate().map_err(DestError::fatal)?;
        self.options = options;
        Ok(self)
    }

    /// Cap DuckDB's own buffer/cache memory (e.g. `"512MB"`). DuckDB's default
    /// is a fraction of SYSTEM RAM, which dominates pipeline RSS on large-memory
    /// machines; ingestion workloads rarely need it.
    pub fn memory_limit(self, limit: &str) -> Result<Self, DestError> {
        self.setting("memory_limit", limit)
    }

    /// Apply one DuckDB setting (`SET key = 'value'`) — the passthrough for
    /// threads, temp_directory, TimeZone, and the like. Validated + applied
    /// eagerly (a bad key/value errors HERE), and replayed on every session
    /// connection the destination opens (cloned connections are fresh
    /// sessions). The key must be a bare identifier; the value is escaped as a
    /// literal.
    pub fn setting(mut self, key: &str, value: &str) -> Result<Self, DestError> {
        if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(fatal(format!(
                "duckdb setting `{key}`: keys must be bare identifiers \
                 ([A-Za-z0-9_]) — refusing to interpolate"
            )));
        }
        let stmt = SetupStmt::Setting {
            key: key.to_owned(),
            value: value.to_owned(),
        };
        {
            let guard = self.db.lock().map_err(|_| fatal("connection poisoned"))?;
            apply_setup(&guard, &stmt)?;
        }
        self.session_setup.push(stmt);
        Ok(self)
    }

    /// LOAD a DuckDB extension by name (bundled builds carry the core
    /// extensions statically — LOAD activates, no network install).
    /// Applied eagerly and replayed per session connection.
    pub fn extension(mut self, name: &str) -> Result<Self, DestError> {
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(fatal(format!(
                "duckdb extension `{name}`: names must be bare identifiers \
                 ([A-Za-z0-9_]) — refusing to interpolate"
            )));
        }
        let stmt = SetupStmt::Extension {
            name: name.to_owned(),
        };
        {
            let guard = self.db.lock().map_err(|_| fatal("connection poisoned"))?;
            apply_setup(&guard, &stmt)?;
        }
        self.session_setup.push(stmt);
        Ok(self)
    }

    fn clone_conn(&self) -> Result<Connection, DestError> {
        let guard = self.db.lock().map_err(|_| fatal("connection poisoned"))?;
        let conn = guard.try_clone().map_err(fatal)?;
        // Fresh session: replay the declared settings/extensions.
        for stmt in &self.session_setup {
            apply_setup(&conn, stmt)?;
        }
        Ok(conn)
    }

    /// Test/inspection helper: count reader-visible rows. Exists for the
    /// crate's tests and the differential/bench harnesses (which consume it
    /// cross-crate); not part of the public destination API — hence hidden.
    #[doc(hidden)]
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

    /// Test/inspection helper: run a scalar query and return its first
    /// column as text. Exists for the crate's tests and the
    /// differential/bench harnesses (which consume it cross-crate); it
    /// executes raw SQL, so it is deliberately NOT part of the public
    /// destination API — hence hidden.
    #[doc(hidden)]
    pub fn query_string(&self, sql: &str) -> Result<String, DestError> {
        let conn = self.clone_conn()?;
        conn.query_row(sql, [], |row| row.get::<_, String>(0))
            .map_err(fatal)
    }
}

pub(crate) fn fatal(e: impl std::fmt::Display) -> DestError {
    DestError::fatal(e.to_string())
}

/// Classify environmental failures as recoverable: DuckDB's "IO Error"
/// family (file locks — another process holding the database — and disk
/// pressure) heals on retry, so it rides the engine's backoff budget.
/// Everything else (parser/binder/catalog/constraint) is deterministic
/// and the operator's to fix. DuckDB exposes no structured error
/// category, so the stable message prefix is the classification key —
/// both facts are pinned by tests/error_codes.rs.
pub(crate) fn classify(e: duckdb::Error) -> DestError {
    match &e {
        duckdb::Error::DuckDBFailure(_, Some(msg)) if msg.starts_with("IO Error") => {
            DestError::transient(e.to_string())
        }
        _ => DestError::fatal(e.to_string()),
    }
}

/// The duplicate-merge-key diagnosis key: DuckDB renders constraint
/// violations with this stable message prefix (pinned by
/// tests/error_codes.rs — a rewording upstream fails that probe loudly
/// instead of silently losing this diagnosis). Public only for the
/// crate's own classification tests.
#[doc(hidden)]
pub fn is_constraint_violation(e: &duckdb::Error) -> bool {
    matches!(e, duckdb::Error::DuckDBFailure(_, Some(msg)) if msg.starts_with("Constraint Error"))
}

fn apply_setup(conn: &Connection, stmt: &SetupStmt) -> Result<(), DestError> {
    match stmt {
        SetupStmt::Setting { key, value } => conn
            .execute_batch(&format!("SET {key}='{}'", value.replace('\'', "''")))
            .map_err(|e| fatal(format!("duckdb setting `{key}`: {e}"))),
        SetupStmt::Extension { name } => conn
            .execute_batch(&format!("LOAD {name}"))
            .map_err(|e| fatal(format!("duckdb extension `{name}`: {e}"))),
    }
}

/// Fail-point registry for crash-sweep tests; coarse by design — DuckDB's own
/// transaction is one atomic step. Macro defined once in `rdlt_core::failpoint`.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &["duck.append", "duck.tx.commit"];

pub(crate) fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub(crate) fn stage_name(table: &TableName) -> String {
    // Hashed: bounded length regardless of table-name length, collision-safe.
    format!(
        "{}{}",
        rdlt_connector_sqlcore::names::STAGE_PREFIX,
        rdlt_connector::core::naming::ident_hash(table.as_str(), 16)
    )
}

/// Quoted, comma-joined column list from the session schema — publishes are ALWAYS
/// by name: the persistent target's column order is historical while the temp stage
/// uses this run's order, so positional `SELECT *` corrupts or breaks on
/// drift.
pub(crate) fn column_list(schema: &TableSchema) -> String {
    schema
        .columns
        .iter()
        .map(|c| quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// DuckDB SQL type for a logical column type — struct-native lowering.
///
/// `is_stage`: stages carry the ARROW shape (Json stays VARCHAR — the
/// appender writes Utf8); targets carry the LOGICAL shape (Json = native
/// JSON). The stage→target `INSERT … SELECT` applies DuckDB's implicit
/// VARCHAR→JSON cast (probe-verified, incl. validation).
pub(crate) fn sql_type(ty: &ColumnType, is_stage: bool) -> String {
    match ty {
        ColumnType::Scalar { scalar } => match scalar {
            LogicalType::Bool => "BOOLEAN".into(),
            LogicalType::Int64 => "BIGINT".into(),
            LogicalType::Float64 => "DOUBLE".into(),
            LogicalType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
            LogicalType::Json if !is_stage => "JSON".into(),
            // Uuid as text for portability with the hex `_rdlt_id` convention.
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
                .map(|f| format!("{} {}", quote(&f.name), sql_type(&f.ty, is_stage)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT({inner})")
        }
        ColumnType::ScalarList { item } => {
            format!("{}[]", sql_type(&ColumnType::scalar(*item), is_stage))
        }
    }
}

pub(crate) fn create_table_sql(name: &str, schema: &TableSchema, temp: bool) -> String {
    let columns = schema
        .columns
        .iter()
        .map(|c| format!("{} {}", quote(&c.name), sql_type(&c.ty, temp)))
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
            // Json lands as native DuckDB JSON — proven by a probe and a
            // round-trip test (tests/probes.rs, tests/json.rs).
            json_type: true,
            decimal: true,
            ident_rules: IdentRules::default(),
        }
    }

    async fn open(&self, _ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError> {
        // A cloned connection shares the database instance but has its OWN temp-table
        // catalog — a dead session's staged temp tables are unreachable.
        let conn = self.clone_conn()?;
        // Meta tables carry the state + commit-receipt correctness protocol.
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS {state} (pipeline VARCHAR PRIMARY KEY, doc VARCHAR);
             CREATE TABLE IF NOT EXISTS {commits} (
                 load_id VARCHAR, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));",
            state = rdlt_connector_sqlcore::names::STATE_TABLE,
            commits = rdlt_connector_sqlcore::names::COMMITS_TABLE
        ))
        .map_err(fatal)?;
        Ok(Box::new(commit::DuckDbSession {
            conn: Mutex::new(conn),
            tables: BTreeMap::new(),
            options: self.options.clone(),
            single_unit_done: std::collections::BTreeSet::new(),
        }))
    }
}

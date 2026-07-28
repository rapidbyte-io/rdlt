//! The Snowflake destination.

use rdlt_connector::{
    ConnectorSpec, Destination, DestinationCapabilities, DestinationError, LoadSession, OpenCtx,
    core::naming::IdentRules,
};


pub(crate) mod client;
mod config;
mod ddl;
mod dialect;
mod encode;
mod session;
mod stage;

pub use config::{
    Auth, ConfigError, KeyPair, Password, S3Stage, SnowflakeConfig, Stage, TableType, config_schema,
};

/// Client seam, exposed ONLY for the live cells: they must provoke real
/// service errors to check how this crate classifies them, and a mock cannot
/// say what Snowflake actually returns. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use rdlt_connector::DestinationError;

    use rdlt_connector::core::{TableSchema, WriteMode};

    use super::client::{self, DmlOnly, Executor};
    use super::config::{SnowflakeConfig, TableType};

    /// Connect and run one statement, returning its first scalar as text.
    pub async fn connect_and_run(
        config: &SnowflakeConfig,
        sql: &str,
    ) -> Result<String, DestinationError> {
        let executor = client::connect(config).await?;
        if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
            Ok(executor.scalar_u64(sql, &[]).await?.to_string())
        } else {
            executor.execute(sql).await.map(|()| String::new())
        }
    }

    /// Run several statements on ONE session, returning the last scalar.
    ///
    /// A session per statement — which is what running them separately gives —
    /// cannot observe anything transactional, so any property that only holds
    /// WITHIN a transaction is invisible to it.
    pub async fn connect_and_run_script(
        config: &SnowflakeConfig,
        script: &[&str],
    ) -> Result<String, DestinationError> {
        let executor = client::connect(config).await?;
        let mut last = String::new();
        for sql in script {
            if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
                last = executor.scalar_u64(sql, &[]).await?.to_string();
            } else {
                executor.execute(sql).await?;
            }
        }
        Ok(last)
    }

    /// Read a query's first column as a set of strings.
    ///
    /// The scalar hook parses its answer as a count, which silently limits it
    /// to numeric columns — fine for probes, useless for reading rows back.
    pub async fn read_column(
        config: &SnowflakeConfig,
        sql: &str,
    ) -> Result<std::collections::BTreeSet<String>, DestinationError> {
        client::connect(config).await?.column_names(sql, &[]).await
    }

    /// The INSERT statements for a batch at a chosen chunk size.
    ///
    /// The measurement seam for the rows-per-statement constant: varying it
    /// here keeps the shipped value a decision someone measured rather than a
    /// knob every user has to hold.
    pub fn insert_statements_chunked(
        qualified_target: &str,
        schema: &TableSchema,
        batch: &rdlt_connector::RecordBatch,
        rows_per_statement: usize,
    ) -> Result<Vec<String>, DestinationError> {
        super::encode::insert_statements_chunked(qualified_target, schema, batch, rows_per_statement)
    }

    /// Run one statement through the UNIT executor — the one that refuses DDL.
    pub async fn run_in_unit(config: &SnowflakeConfig, sql: &str) -> Result<(), DestinationError> {
        let executor = client::connect(config).await?;
        DmlOnly(executor.as_ref()).execute(sql).await
    }

    /// The structured Snowflake code carried by a classified error, if any.
    pub fn classify_live_error(err: &DestinationError) -> Option<String> {
        client::code_in(err)
    }

    /// The code a duplicate merge key surfaces as, so the cell asserting it
    /// names the same constant the merge path will key on rather than
    /// repeating a magic number.
    pub const DUPLICATE_ROW_IN_DML: &str = client::DUPLICATE_ROW_IN_DML;

    /// What the catalog was observed to hold, so a cell can drive the
    /// describe-once decision without a service.
    pub use super::ddl::Catalog;

    /// The ensure statements for a schema, given what the catalog holds.
    ///
    /// The pin seam: the emitted DDL is compared as data, exactly as the other
    /// SQL destinations' ensure suites do, because executing it needs an
    /// account but comparing it needs nothing.
    pub fn ensure_table_sql(
        pipeline: &str,
        schema: &TableSchema,
        mode: &WriteMode,
        table_type: TableType,
        previous: Option<&TableSchema>,
        catalog: &Catalog,
    ) -> Vec<String> {
        super::ddl::table_ddl_stmts(pipeline, schema, mode, table_type, previous, catalog)
    }

    /// The post-table ensure statements (scd2 validity columns; no indexes).
    pub fn ensure_merge_sql(
        options: &rdlt_connector_sqlcore::options::DestOptions,
        schema: &TableSchema,
        mode: &WriteMode,
        catalog: &Catalog,
    ) -> Result<Vec<String>, rdlt_connector_sqlcore::plan::ValidateError> {
        super::ddl::merge_ensure_stmts(options, schema, mode, catalog)
    }

    /// The catalog read for one table, and the columns it reports.
    ///
    /// Live cells use this to prove the read-then-emit-nothing loop against a
    /// real account rather than against a hand-built catalog image.
    pub async fn read_catalog(
        config: &SnowflakeConfig,
        table: &str,
    ) -> Result<std::collections::BTreeSet<String>, DestinationError> {
        let executor = client::connect(config).await?;
        super::ddl::observe_table(executor.as_ref(), &config.database, &config.schema, table).await
    }

    /// Run one ensure statement against the account.
    pub async fn apply(config: &SnowflakeConfig, sql: &str) -> Result<(), DestinationError> {
        client::connect(config).await?.execute(sql).await
    }
}

/// The crash points this destination arms, as a pinned registry.
///
/// Exported so the sweep iterates exactly this list rather than a copy of it:
/// a point added to the code and not to the sweep is a protocol edge nobody
/// ever crashes at, and the failure mode of that is silence.
pub const FAIL_POINTS: &[&str] = &[
    // A staged part is being written to the bucket — before the service has
    // been told anything about it.
    "sf.stage.write",
    // The unit's rows, receipt and state are written and none are durable.
    "sf.unit.publish",
    // Durable, and the caller is about to be told otherwise.
    "sf.receipt.visible",
];

/// The Snowflake destination.
#[derive(Debug, Clone)]
pub struct Snowflake {
    config: SnowflakeConfig,
}

impl Snowflake {
    /// Build a destination from a validated configuration.
    pub fn new(config: SnowflakeConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Build from a YAML document.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        Ok(Self {
            config: SnowflakeConfig::from_yaml(yaml)?,
        })
    }
}

#[async_trait::async_trait]
impl Destination for Snowflake {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec {
            name: "snowflake".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            config_schema: Some(config_schema()),
        }
    }

    fn capabilities(&self) -> DestinationCapabilities {
        DestinationCapabilities {
            merge: true,
            // Nested shapes are lowered before they arrive: VARIANT could
            // carry them, but claiming so without the read-back proof would
            // be a capability this crate has not verified.
            structs: false,
            scalar_lists: false,
            json_type: true,
            decimal: true,
            ident_rules: IdentRules::default(),
        }
    }

    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestinationError> {
        let executor = client::connect(&self.config).await?;

        // The bookkeeping tables, created OUTSIDE any unit — they are DDL, and
        // DDL here commits whatever transaction is open.
        let qualified = |table: &str| {
            format!(
                "{}.{}.{}",
                ddl::quote(&self.config.database),
                ddl::quote(&self.config.schema),
                ddl::quote(table)
            )
        };
        let transient = match self.config.table_type {
            TableType::Transient => "TRANSIENT ",
            TableType::Permanent => "",
        };
        for sql in [
            format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}",
                ddl::quote(&self.config.database),
                ddl::quote(&self.config.schema)
            ),
            format!(
                "CREATE {transient}TABLE IF NOT EXISTS {} ({} VARCHAR PRIMARY KEY, {} VARCHAR)",
                qualified(rdlt_connector_sqlcore::names::STATE_TABLE),
                ddl::quote("pipeline"),
                ddl::quote("doc")
            ),
            format!(
                "CREATE {transient}TABLE IF NOT EXISTS {} ({} VARCHAR, {} NUMBER, \
                 PRIMARY KEY ({}, {}))",
                qualified(rdlt_connector_sqlcore::names::COMMITS_TABLE),
                ddl::quote("load_id"),
                ddl::quote("commit_seq"),
                ddl::quote("load_id"),
                ddl::quote("commit_seq")
            ),
        ] {
            executor.execute(&sql).await?;
        }

        // The bulk path, when a bucket is configured. Both steps are DDL or
        // object-store work and belong here, outside any unit.
        let stage = match self.config.s3_stage() {
            None => None,
            Some(options) => {
                let stage = stage::Stage::connect(options, ctx.pipeline.as_str(), ctx.load_id.as_str())?;
                // Parts staged by a session that died are unreachable — no
                // receipt names them — so a fresh load reclaims its own scope
                // before writing into it. Scoped to THIS pipeline: a sibling
                // sharing the bucket keeps its live parts.
                stage.clear_scope().await?;
                let (sql, secrets) = stage.create_sql(&qualified(stage.name()));
                stage::execute_redacted(executor.as_ref(), &sql, &secrets).await?;
                Some(stage)
            }
        };

        Ok(Box::new(session::SnowflakeSession {
            config: self.config.clone(),
            executor,
            pipeline: ctx.pipeline,
            load_id: ctx.load_id,
            catalog: ddl::Catalog::default(),
            tables: std::collections::BTreeMap::new(),
            cleared: std::collections::BTreeSet::new(),
            unit_open: false,
            single_unit_done: std::collections::BTreeSet::new(),
            stage,
            pending: std::collections::BTreeMap::new(),
            spent: Vec::new(),
        }))
    }
}

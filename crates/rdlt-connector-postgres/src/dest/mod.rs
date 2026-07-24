//! # PostgreSQL destination
//!
//! Binary-protocol COPY into unlogged staging tables; publication is one transaction
//! moving stage → target, upserting the state document, and recording the commit
//! receipt. Receives FLATTENED schemas — `structs: false` makes the
//! engine lower nested objects at the seam. Depends on the SPI only.
//!
//! Module layout (source-mirroring): [`config`] the handle/builder, [`ddl`]
//! type mapping + table DDL, [`encode`] the binary-COPY wire encoding,
//! [`commit`] the load-session protocol.

mod commit;
mod config;
mod ddl;
mod dialect;
mod encode;

use async_trait::async_trait;
use rdlt_connector::{
    ConnectorSpec, Destination, DestinationCapabilities, DestinationError, LoadSession, OpenCtx,
    core::naming::IdentRules,
};

pub use config::{
    AbsentPolicy, DedupSort, DestOptions, MergeStrategy, Postgres, Scd2Options, SortOrder,
    TableOptions,
};

/// SQL-generation seam, exposed ONLY for the golden-SQL pin suite: the pins
/// bind the exact statement text across the sqlcore extraction. Not a public
/// API.
#[doc(hidden)]
pub mod sqlgen {
    pub use super::commit::ARRIVAL_COL;
    pub use super::dialect::PgDialect;
    pub use rdlt_connector_sqlcore::plan::{
        identity_delete_insert_sql, keyed_delete_insert_sql, keyed_upsert_sql, scd2_merge_sql,
        scope_replace_sql,
    };
    pub use rdlt_connector_sqlcore::{HardDelete, MergePlan};
}

/// Fail-point registry: every `crash_point!` site in this crate — the
/// ENGINE-OWNED protocol boundaries (stage writes, the publish transaction
/// edges, the redelivery window). Postgres' internal transaction atomicity
/// is the database's own guarantee and is deliberately NOT instrumented.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &["pg.stage.copy", "pg.publish.begin", "pg.tx.commit"];

/// Render a driver error with its server message + SQLSTATE — the shared
/// rendering both connectors use (tokio-postgres's own Display for a db error
/// is just "db error"; non-db errors render their full source chain).
pub(crate) fn describe(e: &tokio_postgres::Error) -> String {
    crate::pgerror::pg_error_detail(e)
}

pub(crate) fn transient(e: tokio_postgres::Error) -> DestinationError {
    DestinationError::transient(describe(&e))
}

/// Statement-error classification shared by the COPY write path AND table DDL:
/// data-shaped SQLSTATE classes (22 data exception, 23 integrity, 42
/// syntax/access) are PERMANENT — a poisoned batch or an unwinnable 42xxx DDL
/// statement must not burn the engine's retry budget on retries that cannot
/// win. Everything else stays transient (connection-shaped).
pub(crate) fn classify_stmt(e: tokio_postgres::Error) -> DestinationError {
    match e.as_db_error() {
        Some(db) if crate::pgerror::is_permanent_statement_sqlstate(db.code().code()) => {
            DestinationError::fatal(describe(&e))
        }
        _ => DestinationError::transient(describe(&e)),
    }
}

pub(crate) fn fatal(e: impl std::fmt::Display) -> DestinationError {
    DestinationError::fatal(e.to_string())
}

pub(crate) fn quote(ident: &str) -> String {
    // The one injection-safe quoting rule, shared with every SQL destination
    // (and the dialect seam's default). Kept as a thin local alias so the many
    // DDL/publish call sites read `quote(...)`.
    rdlt_connector_sqlcore::quote_ident(ident)
}

#[async_trait]
impl Destination for Postgres {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("postgres", env!("CARGO_PKG_VERSION"))
    }

    fn capabilities(&self) -> DestinationCapabilities {
        DestinationCapabilities {
            merge: true,
            structs: false,      // → engine flattens collision-safely at the seam
            scalar_lists: false, // → scalar lists become child tables at shred planning
            // Native JSONB + NUMERIC(p,s) — engine lowering passes
            // Json/Decimal128 through untouched. These are CODE-LEVEL
            // declarations; no user configuration exists.
            json_type: true,
            decimal: true,
            ident_rules: IdentRules { max_len: 63 },
        }
    }

    async fn open(&self, _ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestinationError> {
        let client = self.client().await?;
        let schema = quote(&self.schema);
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema};
                 CREATE TABLE IF NOT EXISTS {state} (pipeline TEXT PRIMARY KEY, doc TEXT);
                 CREATE TABLE IF NOT EXISTS {commits} (
                     load_id TEXT, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));",
                state = rdlt_connector_sqlcore::names::STATE_TABLE,
                commits = rdlt_connector_sqlcore::names::COMMITS_TABLE
            ))
            .await
            .map_err(transient)?;

        // Staged data from THIS PIPELINE's dead sessions becomes
        // invisible/reclaimable. Scoped by pipeline-hash prefix: other pipelines
        // sharing the schema keep their live staged rows.
        let prefix_pattern = format!(
            "{}%",
            commit::stage_prefix(&_ctx.pipeline).replace('_', "\\_")
        );
        let stale: Vec<String> = client
            .query(
                "SELECT tablename FROM pg_tables
                 WHERE schemaname = $1 AND tablename LIKE $2",
                &[&self.schema, &prefix_pattern],
            )
            .await
            .map_err(transient)?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        for table in stale {
            client
                .batch_execute(&format!("TRUNCATE TABLE {}", quote(&table)))
                .await
                .map_err(transient)?;
        }

        Ok(Box::new(commit::PgSession {
            client,
            pipeline: _ctx.pipeline,
            tables: std::collections::BTreeMap::new(),
            options: self.options.clone(),
            single_unit_done: std::collections::BTreeSet::new(),
        }))
    }
}

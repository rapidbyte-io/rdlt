//! # PostgreSQL destination
//!
//! Binary-protocol COPY into unlogged staging tables; publication is one transaction
//! moving stage → target, upserting the state document, and recording the commit
//! receipt (clauses D1–D4). Receives FLATTENED schemas — `structs: false` makes the
//! engine lower nested objects at the seam. Depends on the SPI only.
//!
//! Module layout (feature 008, source-mirroring): [`config`] the handle/
//! builder, [`ddl`] type mapping + table DDL, [`encode`] the binary-COPY
//! wire encoding, [`commit`] the load-session protocol.

mod commit;
mod config;
mod ddl;
mod encode;

use async_trait::async_trait;
use rdlt_connector::{
    ConnectorSpec, DestCapabilities, DestError, Destination, LoadSession, OpenCtx,
    core::naming::IdentRules,
};

pub use config::{
    AbsentPolicy, DedupSort, MergeStrategy, PgDestOptions, PgTableOptions, Postgres, Scd2Options,
    SortOrder,
};

/// Fail-point registry (gate G2.2): every `crash_point!` site in this crate —
/// the ENGINE-OWNED protocol boundaries (stage writes, the publish transaction
/// edges, the D3 redelivery window). Postgres' internal transaction atomicity
/// is the database's own guarantee and is deliberately NOT instrumented
/// (research R20 scope guard).
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &["pg.stage.copy", "pg.publish.begin", "pg.tx.commit"];

/// Review-F6 closure (feature 008, FR-010): every db-originated error
/// carries the server's message + SQLSTATE — tokio-postgres's Display for
/// db errors is just "db error" (the recurring 006 opacity). Non-db errors
/// render their full source chain.
pub(crate) fn describe(e: &tokio_postgres::Error) -> String {
    use std::error::Error as _;
    if let Some(db) = e.as_db_error() {
        // The CONTEXT line names the COPY column for data errors (review F5).
        let context = db.where_().map(|w| format!(" [{w}]")).unwrap_or_default();
        return format!(
            "{e}: {} (SQLSTATE {}){context}",
            db.message(),
            db.code().code()
        );
    }
    let mut rendered = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        rendered.push_str(": ");
        rendered.push_str(&cause.to_string());
        source = cause.source();
    }
    rendered
}

pub(crate) fn transient(e: tokio_postgres::Error) -> DestError {
    DestError::transient(describe(&e))
}

/// Write-path (COPY) error mapping (review F5): data-shaped SQLSTATE classes
/// (22 data exception, 23 integrity, 42 syntax/access) are PERMANENT — a
/// poisoned batch must not burn the engine's retry budget on unwinnable
/// retries. Everything else stays transient (connection-shaped).
pub(crate) fn copy_error(e: tokio_postgres::Error) -> DestError {
    match e.as_db_error() {
        Some(db) if matches!(&db.code().code()[..2], "22" | "23" | "42") => {
            DestError::fatal(describe(&e))
        }
        _ => DestError::transient(describe(&e)),
    }
}

pub(crate) fn fatal(e: impl std::fmt::Display) -> DestError {
    DestError::fatal(e.to_string())
}

pub(crate) fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[async_trait]
impl Destination for Postgres {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("postgres", env!("CARGO_PKG_VERSION"))
    }

    fn capabilities(&self) -> DestCapabilities {
        DestCapabilities {
            merge: true,
            structs: false,      // → engine flattens collision-safely at the seam
            scalar_lists: false, // → scalar lists become child tables at shred planning
            // Feature 008 US1 (dest-types.md): native JSONB + NUMERIC(p,s) —
            // engine lowering passes Json/Decimal128 through untouched. These
            // are CODE-LEVEL declarations; no user configuration exists (R2).
            json_type: true,
            decimal: true,
            ident_rules: IdentRules { max_len: 63 },
        }
    }

    async fn open(&self, _ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestError> {
        let client = self.client().await?;
        let schema = quote(&self.schema);
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema};
                 CREATE TABLE IF NOT EXISTS _rdlt_state (pipeline TEXT PRIMARY KEY, doc TEXT);
                 CREATE TABLE IF NOT EXISTS _rdlt_commits (
                     load_id TEXT, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));"
            ))
            .await
            .map_err(transient)?;

        // Clause D4: staged data from THIS PIPELINE's dead sessions becomes
        // invisible/reclaimable. Scoped by pipeline-hash prefix: other pipelines
        // sharing the schema keep their live staged rows (finding #3).
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
        }))
    }
}

//! The destination connector: the SPI face and `open` — session
//! bookkeeping tables, the search path, and the reclamation obligation.

use async_trait::async_trait;
use rdlt_connector::core::naming::IdentRules;
use rdlt_connector::{
    ConnectorSpec, Destination, DestinationCapabilities, DestinationError, LoadSession, OpenCtx,
};
use rdlt_connector_sqlcore::quote_identifier;

use super::catalog::{Catalog, stage_prefix};
use super::config::{self, Postgres};
use super::errors::{Phase, driver_transient};
use super::load::Load;
use super::unit::Unit;
use crate::session;

#[async_trait]
impl Destination for Postgres {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new("postgres", env!("CARGO_PKG_VERSION"));
        spec.config_schema = Some(config::config_schema());
        spec
    }

    fn capabilities(&self) -> DestinationCapabilities {
        DestinationCapabilities {
            // Merge is native (the sqlcore strategy arms); structs and
            // scalar lists are not — the engine flattens and shreds at the
            // seam. Json and decimal pass through untouched as JSONB and
            // NUMERIC.
            merge: true,
            structs: false,
            scalar_lists: false,
            json_type: true,
            decimal: true,
            ident_rules: IdentRules { max_len: 63 },
        }
    }

    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestinationError> {
        let connection = session::establish(
            &self.connection_string,
            self.tls.as_ref(),
            session::Profile::Plain,
        )
        .await
        .map_err(|e| {
            if e.is_transient() {
                DestinationError::transient(e.to_string())
            } else {
                DestinationError::fatal(e.to_string())
            }
        })?;
        let schema = quote_identifier(&self.schema);
        connection
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema};
                 CREATE TABLE IF NOT EXISTS {state} (pipeline TEXT PRIMARY KEY, doc TEXT);
                 CREATE TABLE IF NOT EXISTS {commits} (
                     load_id TEXT, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));
                 CREATE TABLE IF NOT EXISTS {cleared} (
                     load_id TEXT, table_name TEXT, PRIMARY KEY (load_id, table_name));",
                state = rdlt_connector_sqlcore::names::STATE_TABLE,
                commits = rdlt_connector_sqlcore::names::COMMITS_TABLE,
                cleared = rdlt_connector_sqlcore::names::CLEARED_TABLE
            ))
            .await
            .map_err(|e| driver_transient(Phase::Connect, e))?;

        // Staged data from THIS PIPELINE's dead sessions becomes
        // invisible/reclaimable. Scoped by pipeline-hash prefix: other
        // pipelines sharing the schema keep their live staged rows.
        let prefix_pattern = format!("{}%", stage_prefix(&ctx.pipeline).replace('_', "\\_"));
        let stale: Vec<String> = connection
            .query(
                "SELECT tablename FROM pg_tables
                 WHERE schemaname = $1 AND tablename LIKE $2",
                &[&self.schema, &prefix_pattern],
            )
            .await
            .map_err(|e| driver_transient(Phase::Connect, e))?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        for table in stale {
            connection
                .batch_execute(&format!("TRUNCATE TABLE {}", quote_identifier(&table)))
                .await
                .map_err(|e| driver_transient(Phase::Connect, e))?;
        }

        Ok(Box::new(Load {
            connection,
            pipeline: ctx.pipeline,
            load_id: ctx.load_id,
            catalog: Catalog::default(),
            options: self.options.clone(),
            unit: Unit::default(),
            cleared_targets: std::collections::BTreeSet::new(),
            single_unit_done: std::collections::BTreeSet::new(),
        }))
    }
}

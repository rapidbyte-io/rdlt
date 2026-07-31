//! The destination connector's SPI face: spec, capability declarations, and
//! `open` — session bootstrap plus reclamation of this pipeline's dead stages.

use async_trait::async_trait;
use rdlt_connector::{
    ConnectorSpec, Destination, DestinationCapabilities, DestinationError, LoadSession, OpenCtx,
    core::naming::IdentRules,
};

use super::config::Postgres;
use super::{commit, quote, transient};

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
                     load_id TEXT, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));
                 CREATE TABLE IF NOT EXISTS {cleared} (
                     load_id TEXT, table_name TEXT, PRIMARY KEY (load_id, table_name));",
                state = rdlt_connector_sqlcore::names::STATE_TABLE,
                commits = rdlt_connector_sqlcore::names::COMMITS_TABLE,
                cleared = rdlt_connector_sqlcore::names::CLEARED_TABLE
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
            load_id: _ctx.load_id,
            tables: std::collections::BTreeMap::new(),
            options: self.options.clone(),
            unit: None,
            cleared_targets: std::collections::BTreeSet::new(),
            single_unit_done: std::collections::BTreeSet::new(),
        }))
    }
}

//! # rdlt-source-postgres — bundled PostgreSQL source
//!
//! Declarative (YAML) Postgres source: catalog reflection publishes declared
//! schemas, rows stream as typed Arrow batches decoded straight from the
//! binary COPY wire format (structured path — the shredder is bypassed), and
//! cursor-column incremental has dlt-parity boundary semantics with
//! mid-table checkpointed resume. Depends on the SPI only.
//!
//! Contracts: `specs/005-postgres-source/contracts/{source-config,type-mapping}.md`.
//! Error policy (SPI clause S3): classify Transient/Fatal, never retry here.

pub mod config;
mod errors;
mod reflect;
mod types;

use rdlt_connector::SourceError;
use tokio_postgres::{Client, NoTls};

pub use config::{ConfigError, PostgresConfig};

/// Test-only surface (hidden): lets integration suites drive reflection
/// without going through a full pipeline. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use std::collections::BTreeMap;

    use rdlt_connector::SourceError;

    pub use crate::reflect::{ReflectedColumn, ReflectedTable};

    pub async fn reflect_for_tests(
        config: &crate::PostgresConfig,
    ) -> Result<BTreeMap<String, ReflectedTable>, SourceError> {
        let client = crate::connect(config).await?;
        crate::reflect::reflect_schema(&client, config).await
    }
}

use errors::Phase;

#[derive(Debug)]
pub struct PostgresSource {
    config: PostgresConfig,
}

impl PostgresSource {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        Ok(Self::new(PostgresConfig::from_yaml(yaml)?))
    }

    pub fn new(config: PostgresConfig) -> Self {
        Self { config }
    }
}

/// Open one connection. TLS is not yet wired for the postgres connectors
/// (matching `rdlt-dest-postgres`): a conn string demanding TLS is a Fatal
/// config error, stated plainly. Connection-shaped failures classify
/// Transient — the ENGINE owns the retry loop (clauses S3/E5).
pub(crate) async fn connect(config: &PostgresConfig) -> Result<Client, SourceError> {
    let conn = config.conn.as_str();
    let demands_tls = conn
        .split(&['?', '&'])
        .any(|kv| matches!(kv.trim(), "sslmode=require" | "sslmode=verify-ca" | "sslmode=verify-full"));
    if demands_tls {
        return Err(errors::fatal(
            Phase::Connect,
            None,
            "sslmode=require/verify-* requested, but TLS is not yet wired for the \
             postgres connectors (recorded backlog item); use sslmode=disable/prefer",
        ));
    }
    let (client, connection) = tokio_postgres::connect(conn, NoTls)
        .await
        .map_err(|e| errors::classify(Phase::Connect, None, &e))?;
    tokio::spawn(async move {
        let _ = connection.await; // connection task ends with the client
    });
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_demand_is_fatal_config_error() {
        let config = PostgresConfig::from_yaml(
            "conn: \"postgresql://u:p@localhost/db?sslmode=require\"\n",
        )
        .expect("parses");
        let err = futures::executor::block_on(connect(&config)).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("Fatal"), "{msg}");
        assert!(err.to_string().contains("fatal"), "{err}");
    }
}

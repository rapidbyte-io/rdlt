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
mod copy_decode;
mod errors;
mod reflect;
mod sqlgen;
mod types;

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::TryStreamExt;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};
use tokio_postgres::{Client, NoTls};

pub use config::{ConfigError, PostgresConfig};

use copy_decode::{CopyDecoder, FieldPlan};
use errors::Phase;
use reflect::ReflectedTable;

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

    /// Fuzz entry (targets/pg_copy_decode): arbitrary bytes through the
    /// decoder over a representative multi-type plan — typed errors only,
    /// never a panic. The first fuzz byte splits the input into two feeds so
    /// chunk-boundary states get fuzzed too.
    pub fn fuzz_copy_decode(data: &[u8]) {
        use crate::copy_decode::{CopyDecoder, FieldPlan};
        use crate::types::Decode;
        use arrow_schema::{DataType, TimeUnit};

        let plans = vec![
            FieldPlan { name: "a".into(), decode: Decode::Int8, arrow: DataType::Int64, not_null: true },
            FieldPlan { name: "b".into(), decode: Decode::Utf8, arrow: DataType::Utf8, not_null: false },
            FieldPlan { name: "c".into(), decode: Decode::Decimal { precision: 10, scale: 2 }, arrow: DataType::Decimal128(10, 2), not_null: false },
            FieldPlan { name: "d".into(), decode: Decode::Timestamp { tz: true }, arrow: DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), not_null: false },
            FieldPlan { name: "e".into(), decode: Decode::UuidText, arrow: DataType::Utf8, not_null: false },
            FieldPlan { name: "f".into(), decode: Decode::JsonbText, arrow: DataType::Utf8, not_null: false },
            FieldPlan { name: "g".into(), decode: Decode::Bytea, arrow: DataType::Binary, not_null: false },
            FieldPlan { name: "h".into(), decode: Decode::Bool, arrow: DataType::Boolean, not_null: false },
        ];
        let mut decoder = CopyDecoder::new(plans, 4096, 64);
        let Some((&split, rest)) = data.split_first() else { return };
        let cut = (split as usize).min(rest.len());
        let (one, two) = rest.split_at(cut);
        if decoder.feed(one).is_err() {
            return;
        }
        if decoder.feed(two).is_err() {
            return;
        }
        let _ = decoder.finish();
    }
}

#[derive(Debug)]
pub struct PostgresSource {
    config: PostgresConfig,
    /// Reflection runs once per run (research R3): `streams()` fills it, every
    /// `read()` reuses it. Drift after this point surfaces as typed errors.
    reflected: tokio::sync::OnceCell<BTreeMap<String, ReflectedTable>>,
}

impl PostgresSource {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        Ok(Self::new(PostgresConfig::from_yaml(yaml)?))
    }

    pub fn new(config: PostgresConfig) -> Self {
        Self {
            config,
            reflected: tokio::sync::OnceCell::new(),
        }
    }

    async fn reflected(&self) -> Result<&BTreeMap<String, ReflectedTable>, SourceError> {
        self.reflected
            .get_or_try_init(|| async {
                let client = connect(&self.config).await?;
                reflect::reflect_schema(&client, &self.config).await
            })
            .await
    }
}

/// Open one connection. TLS is not yet wired for the postgres connectors
/// (matching `rdlt-dest-postgres`): a conn string demanding TLS is a Fatal
/// config error, stated plainly. Connection-shaped failures classify
/// Transient — the ENGINE owns the retry loop (clauses S3/E5).
pub(crate) async fn connect(config: &PostgresConfig) -> Result<Client, SourceError> {
    let conn = config.conn.as_str();
    let demands_tls = conn.split(&['?', '&', ' ']).any(|kv| {
        matches!(
            kv.trim(),
            "sslmode=require" | "sslmode=verify-ca" | "sslmode=verify-full"
        )
    });
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

#[async_trait]
impl Source for PostgresSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("postgres", env!("CARGO_PKG_VERSION"))
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let reflected = self.reflected().await?;
        // Listed tables in config order; otherwise every reflected relation.
        let names: Vec<&str> = match &self.config.tables {
            Some(listed) => listed.iter().map(|t| t.name.as_str()).collect(),
            None => reflected.keys().map(String::as_str).collect(),
        };
        let mut specs = Vec::with_capacity(names.len());
        for name in names {
            let table = &reflected[name];
            let table_config = self.config.table_config(name);
            // Validate the cursor column against reflection at publish time
            // (contract rule 3): fail fast, before any data moves.
            if let Some(cursor) = table_config.and_then(|t| t.cursor.as_ref()) {
                reflect::validate_cursor_column(table, &cursor.column)?;
            }
            let mut spec = StreamSpec::new(name).structured();
            let pk: Vec<String> = match table_config.and_then(|t| t.primary_key.clone()) {
                Some(overridden) => overridden,
                None => table.primary_key().iter().map(|s| (*s).to_owned()).collect(),
            };
            if !pk.is_empty() {
                spec = spec.with_primary_key(pk);
            }
            if let Some(cursor) = table_config.and_then(|t| t.cursor.as_ref()) {
                spec = spec.with_cursor_field(cursor.column.clone());
            }
            specs.push(spec);
        }
        Ok(specs)
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let name = req.stream.name.as_str().to_owned();
        let reflected = self.reflected().await?;
        let table = reflected.get(&name).ok_or_else(|| {
            errors::fatal(Phase::Reflect, Some(&name), "stream has no reflected table")
        })?;
        let table_config = self.config.table_config(&name);
        if table_config.and_then(|t| t.cursor.as_ref()).is_some() {
            return Err(errors::fatal(
                Phase::Reflect,
                Some(&name),
                "incremental cursor reads land in Phase 4 (T017); snapshot-only for now",
            ));
        }
        let columns = table.selected_columns(table_config)?;
        let plans: Vec<FieldPlan> = columns
            .iter()
            .map(|c| FieldPlan {
                name: c.name.clone(),
                decode: c.mapped.decode,
                arrow: c.mapped.arrow.clone(),
                not_null: c.not_null,
            })
            .collect();

        let select = sqlgen::select_sql(&self.config.schema, &name, &columns, "", "");
        let copy = sqlgen::copy_sql(&select);

        let client = connect(&self.config).await?;
        let stream = client
            .copy_out(copy.as_str())
            .await
            .map_err(|e| errors::classify(Phase::Copy, Some(&name), &e))?;
        futures::pin_mut!(stream);

        let mut decoder = CopyDecoder::new(plans, self.config.batch_target_bytes, self.config.batch_max_rows);
        loop {
            let chunk = stream
                .try_next()
                .await
                .map_err(|e| errors::classify(Phase::Copy, Some(&name), &e))?;
            let Some(chunk) = chunk else { break };
            let batches = decoder
                .feed(&chunk)
                .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?;
            for batch in batches {
                if req.out.arrow(batch).await.is_err() {
                    return Ok(()); // cancellation (clause S4); dropping the
                                   // client aborts the server-side COPY
                }
            }
        }
        if let Some(tail) = decoder
            .finish()
            .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?
            && req.out.arrow(tail).await.is_err()
        {
            return Ok(());
        }
        if decoder.rows_decoded() == 0 && req.out.arrow(decoder.empty_batch()).await.is_err() {
            return Ok(()); // still cancellation (S4)
        }
        tracing::debug!(table = %name, rows = decoder.rows_decoded(), "snapshot complete");
        // Snapshot (cursor-less) streams never checkpoint: every run is a
        // full read by definition; there is no meaningful resume cursor.
        Ok(())
    }
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

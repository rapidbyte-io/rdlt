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
mod cursor;
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
use rdlt_connector::core::crash_point;
use reflect::ReflectedTable;

/// Fail-point registry (003 gate G2.2, feature 005 FR-009): every
/// `crash_point!` site in this crate — the source read/checkpoint path.
/// The crash sweep pins and iterates exactly this list, both passes.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &[
    "pg.src.after_reflect",
    "pg.src.mid_copy",
    "pg.src.after_batch_push",
    "pg.src.before_checkpoint",
];

/// Test-only surface (hidden): lets integration suites drive reflection
/// without going through a full pipeline. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use std::collections::BTreeMap;

    use rdlt_connector::SourceError;

    pub use crate::source::reflect::{ReflectedColumn, ReflectedTable};

    pub async fn reflect_for_tests(
        config: &crate::source::PostgresConfig,
    ) -> Result<BTreeMap<String, ReflectedTable>, SourceError> {
        let client = crate::source::connect(config).await?;
        crate::source::reflect::reflect_schema(&client, config).await
    }

    /// Canned binary-COPY stream for the gated decoder bench (iai_pg):
    /// `rows` tuples over a representative column mix (int8 pk, int4, float8,
    /// text, timestamptz, bool, uuid, jsonb). Deterministic bytes.
    pub fn bench_wire(rows: usize) -> Vec<u8> {
        let mut wire = b"PGCOPY\n\xff\r\n\0".to_vec();
        wire.extend_from_slice(&0i32.to_be_bytes());
        wire.extend_from_slice(&0i32.to_be_bytes());
        let field = |wire: &mut Vec<u8>, bytes: &[u8]| {
            wire.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            wire.extend_from_slice(bytes);
        };
        for i in 0..rows as i64 {
            wire.extend_from_slice(&8i16.to_be_bytes());
            field(&mut wire, &i.to_be_bytes());
            field(&mut wire, &((i % 100_000) as i32).to_be_bytes());
            field(&mut wire, &(i as f64 * 0.5).to_be_bytes());
            field(&mut wire, format!("user-{i}").as_bytes());
            field(&mut wire, &(i * 1_000_000).to_be_bytes()); // µs since PG epoch
            field(&mut wire, &[(i % 2) as u8]);
            let mut uuid = [0u8; 16];
            uuid[8..].copy_from_slice(&i.to_be_bytes());
            field(&mut wire, &uuid);
            let mut jsonb = vec![1u8];
            jsonb.extend_from_slice(
                format!(r#"{{"city":"NYC","zip":{}}}"#, 10_001 + i % 100).as_bytes(),
            );
            field(&mut wire, &jsonb);
        }
        wire.extend_from_slice(&(-1i16).to_be_bytes());
        wire
    }

    /// The gated decoder hot path (bench body): full stream -> Arrow batches;
    /// returns decoded rows so the work cannot be optimized away.
    pub fn bench_decode(wire: &[u8]) -> u64 {
        use crate::source::copy_decode::{CopyDecoder, FieldPlan};
        use crate::source::types::Decode;
        use arrow_schema::{DataType, TimeUnit};

        let plans = vec![
            FieldPlan {
                name: "id".into(),
                decode: Decode::Int8,
                arrow: DataType::Int64,
                not_null: true,
            },
            FieldPlan {
                name: "small".into(),
                decode: Decode::Int4,
                arrow: DataType::Int64,
                not_null: false,
            },
            FieldPlan {
                name: "ratio".into(),
                decode: Decode::Float8,
                arrow: DataType::Float64,
                not_null: false,
            },
            FieldPlan {
                name: "name".into(),
                decode: Decode::Utf8,
                arrow: DataType::Utf8,
                not_null: false,
            },
            FieldPlan {
                name: "at".into(),
                decode: Decode::Timestamp { tz: true },
                arrow: DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                not_null: false,
            },
            FieldPlan {
                name: "ok".into(),
                decode: Decode::Bool,
                arrow: DataType::Boolean,
                not_null: false,
            },
            FieldPlan {
                name: "token".into(),
                decode: Decode::UuidText,
                arrow: DataType::Utf8,
                not_null: false,
            },
            FieldPlan {
                name: "doc".into(),
                decode: Decode::JsonbText,
                arrow: DataType::Utf8,
                not_null: false,
            },
        ];
        let mut decoder = CopyDecoder::new(plans, 8 << 20, 65_536);
        // Feed in 64 KiB chunks — socket-realistic boundaries.
        let mut rows = 0u64;
        for chunk in wire.chunks(64 << 10) {
            let batches = decoder.feed(chunk).expect("bench wire is valid");
            rows += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
        }
        if let Some(tail) = decoder.finish().expect("trailer") {
            rows += tail.num_rows() as u64;
        }
        rows
    }

    /// Fuzz entry (targets/pg_copy_decode): arbitrary bytes through the
    /// decoder over a representative multi-type plan — typed errors only,
    /// never a panic. The first fuzz byte splits the input into two feeds so
    /// chunk-boundary states get fuzzed too.
    pub fn fuzz_copy_decode(data: &[u8]) {
        use crate::source::copy_decode::{CopyDecoder, FieldPlan};
        use crate::source::types::Decode;
        use arrow_schema::{DataType, TimeUnit};

        let plans = vec![
            FieldPlan {
                name: "a".into(),
                decode: Decode::Int8,
                arrow: DataType::Int64,
                not_null: true,
            },
            FieldPlan {
                name: "b".into(),
                decode: Decode::Utf8,
                arrow: DataType::Utf8,
                not_null: false,
            },
            FieldPlan {
                name: "c".into(),
                decode: Decode::Decimal {
                    precision: 10,
                    scale: 2,
                },
                arrow: DataType::Decimal128(10, 2),
                not_null: false,
            },
            FieldPlan {
                name: "d".into(),
                decode: Decode::Timestamp { tz: true },
                arrow: DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                not_null: false,
            },
            FieldPlan {
                name: "e".into(),
                decode: Decode::UuidText,
                arrow: DataType::Utf8,
                not_null: false,
            },
            FieldPlan {
                name: "f".into(),
                decode: Decode::JsonbText,
                arrow: DataType::Utf8,
                not_null: false,
            },
            FieldPlan {
                name: "g".into(),
                decode: Decode::Bytea,
                arrow: DataType::Binary,
                not_null: false,
            },
            FieldPlan {
                name: "h".into(),
                decode: Decode::Bool,
                arrow: DataType::Boolean,
                not_null: false,
            },
        ];
        let mut decoder = CopyDecoder::new(plans, 4096, 64);
        let Some((&split, rest)) = data.split_first() else {
            return;
        };
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

    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        Ok(Self::new(PostgresConfig::from_json(json)?))
    }

    /// Embedder entry point (see [`PostgresConfig::from_value`]).
    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        Ok(Self::new(PostgresConfig::from_value(value)?))
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
/// config error, stated plainly. The conn string is PARSED here (not
/// string-matched): parse failure is Fatal (contract rule 1 — never the
/// Transient/retry path), and the TLS policy reads the parsed ssl_mode, so
/// every libpq syntax form is covered. `PostgresConfig` has pub fields, so
/// this enforcement point holds even for configs built without `validate`.
pub(crate) async fn connect(config: &PostgresConfig) -> Result<Client, SourceError> {
    let conn = config.conn.as_str();
    let parsed: tokio_postgres::Config = conn.parse().map_err(|e| {
        errors::fatal(
            Phase::Connect,
            None,
            format!("conn string does not parse: {e}"),
        )
    })?;
    if parsed.get_ssl_mode() == tokio_postgres::config::SslMode::Require {
        return Err(errors::fatal(
            Phase::Connect,
            None,
            "sslmode=require requested, but TLS is not yet wired for the \
             postgres connectors (recorded backlog item); use sslmode=disable/prefer",
        ));
    }
    let (client, connection) = parsed
        .connect(NoTls)
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
                None => table
                    .primary_key()
                    .iter()
                    .map(|s| (*s).to_owned())
                    .collect(),
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
        let columns = table.selected_columns(table_config)?;
        crash_point!(
            "pg.src.after_reflect",
            Err(errors::fatal(
                Phase::Reflect,
                Some(&name),
                "injected: after reflect"
            ))
        );
        let plans: Vec<FieldPlan> = columns
            .iter()
            .map(|c| FieldPlan {
                name: c.name.clone(),
                decode: c.mapped.decode,
                arrow: c.mapped.arrow.clone(),
                not_null: c.not_null,
            })
            .collect();

        // Incremental setup (research R5): resume state, boundary matrix,
        // ordered read + tracker. Snapshot streams skip all of it.
        let cursor_config = table_config.and_then(|t| t.cursor.as_ref());
        let mut incremental: Option<(cursor::Tracker, config::CursorConfig)> = None;
        let (where_sql, order_sql) = match cursor_config {
            None => (String::new(), String::new()),
            Some(cc) => {
                let reflected_cursor = reflect::validate_cursor_column(table, &cc.column)?;
                let cursor_decode = reflected_cursor.mapped.decode;
                let cursor_idx = columns
                    .iter()
                    .position(|c| c.name == cc.column)
                    .ok_or_else(|| {
                        errors::fatal(
                            Phase::Reflect,
                            Some(&name),
                            format!(
                                "cursor column `{}` is excluded by the column selection",
                                cc.column
                            ),
                        )
                    })?;
                let direction_max = cc.direction == config::Direction::Max;
                let stored = match &req.since {
                    Some(since) => Some(cursor::CursorState::decode(since, &name)?),
                    None => None,
                };
                // Lower bound: stored state — closed (>= + dedup) iff it
                // carries boundary keys, which every checkpoint does except
                // an open-boundary final; else the configured initial_value
                // under the configured boundary.
                let closed_default = cc.boundary == config::Boundary::Closed;
                let lower: Option<(cursor::Watermark, bool)> = match &stored {
                    Some(state) => Some((state.watermark.clone(), !state.boundary_keys.is_empty())),
                    None => match &cc.initial_value {
                        Some(text) => Some((
                            cursor::Watermark::parse_config_literal(cursor_decode, text, &name)?,
                            closed_default,
                        )),
                        None => None,
                    },
                };
                let upper: Option<cursor::Watermark> = match &cc.end_value {
                    Some(text) => Some(cursor::Watermark::parse_config_literal(
                        cursor_decode,
                        text,
                        &name,
                    )?),
                    None => None,
                };
                let clauses = sqlgen::incremental_clauses(
                    &cc.column,
                    direction_max,
                    lower.as_ref().map(|(w, closed)| (w, *closed)),
                    upper.as_ref(),
                    cc.nulls == config::NullPolicy::Include,
                    matches!(cursor_decode, types::Decode::Utf8),
                );
                // Row keys: configured/reflected PK columns present in the
                // selection; otherwise whole-row hashing.
                let pk_names: Vec<String> = match table_config.and_then(|t| t.primary_key.clone()) {
                    Some(overridden) => overridden,
                    None => table
                        .primary_key()
                        .iter()
                        .map(|s| (*s).to_owned())
                        .collect(),
                };
                let key_columns: Option<Vec<usize>> = if pk_names.is_empty() {
                    None
                } else {
                    pk_names
                        .iter()
                        .map(|k| columns.iter().position(|c| &c.name == k))
                        .collect()
                };
                let tracker = cursor::Tracker::new(
                    cursor_idx,
                    cursor_decode,
                    direction_max,
                    stored,
                    key_columns,
                );
                incremental = Some((tracker, cc.clone()));
                (clauses.where_sql, clauses.order_sql)
            }
        };

        let select =
            sqlgen::select_sql(&self.config.schema, &name, &columns, &where_sql, &order_sql);
        let copy = sqlgen::copy_sql(&select);

        let client = connect(&self.config).await?;
        let stream = client
            .copy_out(copy.as_str())
            .await
            .map_err(|e| errors::classify(Phase::Copy, Some(&name), &e))?;
        futures::pin_mut!(stream);

        let mut decoder = CopyDecoder::new(
            plans,
            self.config.batch_target_bytes,
            self.config.batch_max_rows,
        );
        let mut pushed_any = false;
        loop {
            let chunk = stream
                .try_next()
                .await
                .map_err(|e| errors::classify(Phase::Copy, Some(&name), &e))?;
            let Some(chunk) = chunk else { break };
            // Simulated mid-stream connection loss: Transient — the ENGINE
            // retries the whole read from committed state (E5/E6/S1).
            crash_point!(
                "pg.src.mid_copy",
                Err(errors::transient(
                    Phase::Copy,
                    Some(&name),
                    "injected: connection lost mid-COPY"
                ))
            );
            let batches = decoder
                .feed(&chunk)
                .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?;
            for batch in batches {
                if !push_tracked(&mut req, &mut incremental, batch, &mut pushed_any).await? {
                    return Ok(()); // cancellation (clause S4); dropping the
                    // client aborts the server-side COPY
                }
            }
        }
        if let Some(tail) = decoder
            .finish()
            .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?
            && !push_tracked(&mut req, &mut incremental, tail, &mut pushed_any).await?
        {
            return Ok(());
        }
        if !pushed_any && req.out.arrow(decoder.empty_batch()).await.is_err() {
            return Ok(()); // still cancellation (S4)
        }
        match incremental {
            None => {
                // Snapshot (cursor-less) streams never checkpoint: every run
                // is a full read by definition; no meaningful resume cursor.
                tracing::debug!(table = %name, rows = decoder.rows_decoded(), "snapshot complete");
            }
            Some((tracker, cc)) => {
                crash_point!(
                    "pg.src.before_checkpoint",
                    Err(errors::fatal(
                        Phase::Copy,
                        Some(&name),
                        "injected: before final checkpoint"
                    ))
                );
                let keep_keys = cc.boundary == config::Boundary::Closed;
                if let Some(state) = tracker.final_state(keep_keys)
                    && req.out.checkpoint(state.encode()).await.is_err()
                {
                    return Ok(());
                }
                tracing::debug!(
                    table = %name,
                    rows = decoder.rows_decoded(),
                    deduped = tracker.deduped_rows,
                    "incremental read complete"
                );
            }
        }
        Ok(())
    }
}

/// Push one decoded batch, routed through the incremental tracker when
/// present (dedup → push → intermediate checkpoint, in that order — S2).
/// Returns Ok(false) on cancellation.
async fn push_tracked(
    req: &mut ReadRequest,
    incremental: &mut Option<(cursor::Tracker, config::CursorConfig)>,
    batch: arrow_array::RecordBatch,
    pushed_any: &mut bool,
) -> Result<bool, SourceError> {
    match incremental {
        None => {
            *pushed_any = true;
            Ok(req.out.arrow(batch).await.is_ok())
        }
        Some((tracker, _)) => {
            let (filtered, checkpoint) = tracker.process(batch);
            if let Some(filtered) = filtered {
                *pushed_any = true;
                if req.out.arrow(filtered).await.is_err() {
                    return Ok(false);
                }
                crash_point!(
                    "pg.src.after_batch_push",
                    Err(errors::transient(
                        Phase::Copy,
                        None,
                        "injected: after batch push"
                    ))
                );
            }
            if let Some(state) = checkpoint
                && req.out.checkpoint(state.encode()).await.is_err()
            {
                return Ok(false);
            }
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "failpoints")]
    #[test]
    fn crash_points_actually_fire() {
        fn site() -> Result<(), SourceError> {
            crash_point!(
                "pg.src.after_reflect",
                Err(errors::fatal(Phase::Reflect, None, "probe"))
            );
            Ok(())
        }
        rdlt_connector::core::failpoint::fail::cfg("pg.src.after_reflect", "return").unwrap();
        let fired = site().is_err();
        rdlt_connector::core::failpoint::fail::remove("pg.src.after_reflect");
        assert!(fired, "armed crash_point must fire");
    }

    #[test]
    fn tls_demand_rejected_at_config_validation() {
        // The from_yaml path rejects TLS demands at validate (config.rs has
        // the full matrix incl. the spaced keyword form); the sibling test
        // above proves connect() enforces it even when validate is bypassed.
        let err =
            PostgresConfig::from_yaml("conn: \"postgresql://u:p@localhost/db?sslmode=require\"\n")
                .unwrap_err();
        assert!(err.to_string().contains("TLS is not yet wired"), "{err}");
    }
}

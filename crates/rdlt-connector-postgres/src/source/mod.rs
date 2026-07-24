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

pub(crate) mod cdc;
pub mod config;
pub(crate) mod copy_decode;
mod cursor;
mod errors;
mod reflect;
mod sqlgen;
mod types;

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::TryStreamExt;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};
use tokio_postgres::Client;

pub use config::{ConfigError, PostgresConfig};
pub use types::HintType;

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

/// CDC fail-point registry (feature 009, contract O6) — the CDC sweep pins
/// and iterates exactly this list, both passes.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const CDC_FAIL_POINTS: &[&str] = &[
    "cdc.slot.create",
    "cdc.snapshot.copy",
    "cdc.stream.peek",
    "cdc.ack.advance",
];

/// Test-only surface (hidden): lets integration suites drive reflection
/// without going through a full pipeline. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use std::collections::BTreeMap;

    use rdlt_connector::SourceError;

    pub use crate::source::reflect::{ReflectedColumn, ReflectedTable};

    /// CDC lifecycle surface for the integration suites (feature 009).
    pub use crate::source::cdc::slot as cdc_slot;

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

    /// Fuzz entry (targets/pg_pgoutput_decode): arbitrary bytes through the
    /// logical-replication message parser — typed errors only, never a panic.
    pub fn fuzz_pgoutput_decode(data: &[u8]) {
        let _ = crate::source::cdc::pgoutput::parse(data);
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
    /// CDC run state (feature 009): slot lifecycle, the shared snapshot
    /// transaction, the run's target-LSN pin, and ack accumulation.
    cdc_runtime: cdc::Runtime,
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
            cdc_runtime: cdc::Runtime::new(),
        }
    }

    /// The CDC-covered stream set: every TABLE stream (query streams are
    /// never CDC), in declaration order.
    fn cdc_tables(&self, reflected: &BTreeMap<String, ReflectedTable>) -> Vec<String> {
        match &self.config.tables {
            Some(listed) => listed.iter().map(|t| t.name.clone()).collect(),
            None => reflected
                .keys()
                .filter(|n| self.config.query_config(n).is_none())
                .cloned()
                .collect(),
        }
    }

    async fn reflected(&self) -> Result<&BTreeMap<String, ReflectedTable>, SourceError> {
        self.reflected
            .get_or_try_init(|| async {
                let client = connect(&self.config).await?;
                let mut tables = reflect::reflect_schema(&client, &self.config).await?;
                // Query streams (006): described once per run, same cache.
                for query in &self.config.queries {
                    if tables.contains_key(&query.name) {
                        return Err(errors::fatal(
                            Phase::Reflect,
                            Some(&query.name),
                            "query stream name collides with a reflected table",
                        ));
                    }
                    let described =
                        reflect::describe_query(&client, &query.name, &query.sql).await?;
                    tables.insert(query.name.clone(), described);
                }
                Ok(tables)
            })
            .await
    }

    /// The effective per-stream config: a listed table's, or a query's
    /// (viewed through the table-config shape so hints/cursor machinery
    /// applies unchanged).
    fn stream_config(&self, name: &str) -> Option<config::TableConfig> {
        self.config
            .table_config(name)
            .cloned()
            .or_else(|| self.config.synthesized_table_config(name))
    }
}

/// Open one connection through the shared TLS policy (feature 006, contract
/// tls-policy.md): conn parse failure is Fatal (never the Transient retry
/// path); the effective policy resolves from the parsed sslmode + the
/// config's `tls:` block; verification failures are Fatal (retries don't
/// mint certificates), network-shaped failures Transient (E5). Enforcement
/// lives HERE so it holds even for configs built without `validate`
/// (`PostgresConfig` has pub fields).
pub(crate) async fn connect(config: &PostgresConfig) -> Result<Client, SourceError> {
    let crate::tls::ParsedConn { pg, policy } =
        crate::tls::parse_conn(&config.conn, config.tls.as_ref())
            .map_err(|e| errors::fatal(Phase::Connect, None, e))?;
    match crate::tls::connect(&pg, &policy).await {
        Ok(client) => Ok(client),
        Err(crate::tls::ConnectResult::Config(e)) => Err(errors::fatal(Phase::Connect, None, e)),
        Err(crate::tls::ConnectResult::Connect(e)) if e.transient => {
            Err(errors::transient(Phase::Connect, None, e))
        }
        Err(crate::tls::ConnectResult::Connect(e)) => Err(errors::fatal(Phase::Connect, None, e)),
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new("postgres", env!("CARGO_PKG_VERSION"));
        spec.config_schema = Some(config_schema());
        spec
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let reflected = self.reflected().await?;
        // Listed tables in config order (else every reflected relation),
        // then query streams.
        let mut names: Vec<&str> = match &self.config.tables {
            Some(listed) => listed.iter().map(|t| t.name.as_str()).collect(),
            None => reflected
                .keys()
                .map(String::as_str)
                .filter(|n| self.config.query_config(n).is_none())
                .collect(),
        };
        names.extend(self.config.queries.iter().map(|q| q.name.as_str()));
        let cdc_names = self.cdc_tables(reflected);
        let mut specs = Vec::with_capacity(names.len());
        for name in names {
            let table = &reflected[name];
            let owned_config = self.stream_config(name);
            let table_config = owned_config.as_ref();
            // CDC table streams (feature 009): keyed structured streams, the
            // key from the replica-identity preflight (O1); cursor config is
            // impossible here (C1, validated at parse). Query streams stay on
            // their own machinery.
            if let Some(cdc_config) = &self.config.cdc
                && self.config.query_config(name).is_none()
            {
                let hinted = reflect::hinted_columns(table, table_config)?;
                let identities = self
                    .cdc_runtime
                    .identities(&self.config, cdc_config, reflected, &cdc_names)
                    .await?;
                let identity = &identities[name];
                // The merge key must survive the column selection — delete
                // records are key-only, so an excluded key column would strand
                // them.
                if let Some(missing) = identity
                    .key
                    .iter()
                    .find(|k| !hinted.iter().any(|c| &&c.name == k))
                {
                    return Err(errors::fatal(
                        Phase::Reflect,
                        Some(name),
                        format!(
                            "replica-identity key column `{missing}` is excluded \
                             by the column selection — CDC delete records carry \
                             only key columns"
                        ),
                    ));
                }
                specs.push(
                    StreamSpec::new(name)
                        .structured()
                        .with_primary_key(identity.key.clone()),
                );
                continue;
            }
            // Validate selection + hints + cursor at publish time (contract
            // rule 3 + 006 hints): fail fast, before any data moves. The
            // cursor check runs POST-hint (a hint may change capability).
            let hinted = reflect::hinted_columns(table, table_config)?;
            let pk: Vec<String> = match table_config.and_then(|t| t.primary_key.clone()) {
                Some(overridden) => overridden,
                None => table
                    .primary_key()
                    .iter()
                    .map(|s| (*s).to_owned())
                    .collect(),
            };
            if let Some(cursor) = table_config.and_then(|t| t.cursor.as_ref()) {
                let col = hinted
                    .iter()
                    .find(|c| c.name == cursor.column)
                    .ok_or_else(|| {
                        errors::fatal(
                            Phase::Reflect,
                            Some(name),
                            format!("cursor column `{}` is not a selected column", cursor.column),
                        )
                    })?;
                if !col.mapped.cursor_capable {
                    return Err(errors::fatal(
                        Phase::Reflect,
                        Some(name),
                        format!(
                            "cursor column `{}` is not cursor-capable after type mapping/hints",
                            cursor.column
                        ),
                    ));
                }
                // Lag validation (feature 007, contract L2), all typed and
                // early: closed boundary (also caught at config parse),
                // defined cursor-family subtraction, and a primary key so
                // the keyed-Merge dedup path exists (research R4).
                if let Some(lag) = &cursor.lag {
                    if cursor.boundary == config::Boundary::Open {
                        return Err(errors::fatal(
                            Phase::Reflect,
                            Some(name),
                            format!("cursor `{}`: lag requires a closed boundary", cursor.column),
                        ));
                    }
                    if let Err(detail) = lag.sql_delta(col.mapped.decode) {
                        return Err(errors::fatal(
                            Phase::Reflect,
                            Some(name),
                            format!(
                                "cursor lag on `{}` ({}): {detail}",
                                cursor.column, col.type_name
                            ),
                        ));
                    }
                    if pk.is_empty() {
                        return Err(errors::fatal(
                            Phase::Reflect,
                            Some(name),
                            format!(
                                "cursor `{}`: lag requires a primary key (reflected or \
                                 declared) — window rows re-deliver and dedup by key \
                                 under Merge (contract cursor-lag.md L2)",
                                cursor.column
                            ),
                        ));
                    }
                }
            }
            let mut spec = StreamSpec::new(name).structured();
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
        let owned_config = self.stream_config(&name);
        let table_config = owned_config.as_ref();
        let owned_columns = reflect::hinted_columns(table, table_config)?;
        // US4 lossy visibility (type-mapping contract): each [documented-lossy]
        // column announces itself ONCE per read on a dedicated target, so
        // embedders can subscribe to `rdlt::lossy` without log-scraping.
        for column in &owned_columns {
            if column.mapped.documented_lossy {
                tracing::warn!(
                    target: "rdlt::lossy",
                    stream = %name,
                    column = %column.name,
                    source_type = %column.type_name,
                    "documented-lossy mapping: values arrive via a textual or \
                     normalizing conversion (see contracts/type-hints.md)"
                );
            }
        }
        let columns: Vec<&reflect::ReflectedColumn> = owned_columns.iter().collect();
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

        // CDC dispatch (feature 009): table streams read through the
        // snapshot/change-pass machinery; everything below (cursor columns,
        // COPY-per-read) is the non-CDC path.
        if let Some(cdc_config) = &self.config.cdc
            && self.config.query_config(&name).is_none()
        {
            let cdc_names = self.cdc_tables(reflected);
            let identities = self
                .cdc_runtime
                .identities(&self.config, cdc_config, reflected, &cdc_names)
                .await?;
            let identity = identities.get(&name).ok_or_else(|| {
                errors::fatal(Phase::Reflect, Some(&name), "stream is not a CDC table")
            })?;
            return cdc::read_stream(
                &self.cdc_runtime,
                &self.config,
                cdc_config,
                identity,
                &cdc_names,
                plans,
                &columns,
                req,
            )
            .await;
        }

        // Incremental setup (research R5): resume state, boundary matrix,
        // ordered read + tracker. Snapshot streams skip all of it.
        let cursor_config = table_config.and_then(|t| t.cursor.as_ref());
        let mut incremental: Option<(cursor::Tracker, config::CursorConfig)> = None;
        let (where_sql, order_sql) = match cursor_config {
            None => (String::new(), String::new()),
            Some(cc) => {
                let reflected_cursor = columns
                    .iter()
                    .find(|c| c.name == cc.column)
                    .filter(|c| c.mapped.cursor_capable)
                    .ok_or_else(|| {
                        errors::fatal(
                            Phase::Reflect,
                            Some(&name),
                            format!(
                                "cursor column `{}` missing from selection or not \
                                 cursor-capable after type mapping/hints",
                                cc.column
                            ),
                        )
                    })?;
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
                // Lag (feature 007, contract cursor-lag.md L1): widen the
                // RESUMED window only — run 1 (no watermark) is unaffected,
                // and the saved watermark is never lowered. The window
                // re-read forces closed semantics regardless of how the
                // stored state's boundary flag landed.
                let lag_delta: Option<String> = match (&cc.lag, &stored) {
                    (Some(lag), Some(_)) => Some(lag.sql_delta(cursor_decode).map_err(|d| {
                        errors::fatal(
                            Phase::Reflect,
                            Some(&name),
                            format!("cursor lag on `{}`: {d}", cc.column),
                        )
                    })?),
                    _ => None,
                };
                let lower = match (lower, &lag_delta) {
                    (Some((w, _)), Some(_)) => Some((w, true)),
                    (other, _) => other,
                };
                let clauses = sqlgen::incremental_clauses(
                    &cc.column,
                    direction_max,
                    lower.as_ref().map(|(w, closed)| (w, *closed)),
                    lag_delta.as_deref(),
                    upper
                        .as_ref()
                        .map(|w| (w, cc.end_bound == config::EndBound::Inclusive)),
                    // `error` keeps NULL rows IN the read (like include) so
                    // the tracker can raise on them (N1).
                    cc.nulls != config::NullPolicy::Exclude,
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
                    (cc.nulls == config::NullPolicy::Error)
                        .then(|| (name.clone(), cc.column.clone())),
                    name.clone(),
                    cc.column.clone(),
                );
                incremental = Some((tracker, cc.clone()));
                (clauses.where_sql, clauses.order_sql)
            }
        };

        let select = match self.config.query_config(&name) {
            // Query stream (006): the wrapped user SQL is the FROM clause.
            Some(query) => sqlgen::select_sql_from(
                &format!("( {} ) AS q", query.sql.trim().trim_end_matches(';')),
                &columns,
                &where_sql,
                &order_sql,
            ),
            None => {
                sqlgen::select_sql(&self.config.schema, &name, &columns, &where_sql, &order_sql)
            }
        };
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
            let (filtered, checkpoint) = tracker.process(batch)?;
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

/// JSON Schema GENERATED from the config structs (feature 006, US4) — the
/// declared schema and the parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(config::PostgresConfig)).expect("schema serializes")
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
    fn tls_contradiction_rejected_at_config_validation() {
        // Feature 006: sslmode=require is ACCEPTED (TLS is wired); the
        // config-level rejection is now the contradiction rule.
        assert!(
            PostgresConfig::from_yaml("conn: \"postgresql://u:p@localhost/db?sslmode=require\"\n")
                .is_ok(),
            "require now validates — TLS is wired"
        );
        let err = PostgresConfig::from_yaml(
            "conn: \"postgresql://u:p@localhost/db?sslmode=require\"\ntls:\n  mode: disable\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("contradicts"), "{err}");
    }
}

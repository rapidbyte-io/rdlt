//! Declarative Postgres source configuration: connection, stream selection,
//! per-table cursor + key configuration, batching knobs. One YAML document a
//! platform can render and validate; unknown fields are errors.
//!
//! There is deliberately NO retry configuration — retry policy is engine-owned;
//! this source only classifies errors.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("parsing postgres source config: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("parsing postgres source JSON config: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid postgres source config: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// libpq-style connection string/URL; `sslmode` up to `require` may be
    /// set here (verify-* modes go in the `tls` block).
    pub conn: String,
    /// Reflection scope; bare table names below resolve inside it.
    #[serde(default = "default_schema")]
    pub schema: String,
    /// Include views and materialized views in schema-wide discovery.
    #[serde(default)]
    pub include_views: bool,
    /// Table selection, three-valued. PRESENT ⇒ exactly this list and nothing
    /// else — including the empty list, which selects no tables and leaves
    /// `queries` as the run's only streams. ABSENT ⇒ discover every table in
    /// `schema`, which is why a pipeline that declares queries and omits this
    /// field also receives every table alongside them.
    #[serde(default)]
    pub tables: Option<Vec<TableConfig>>,
    /// Query streams: a stream per SQL statement, schema DESCRIBED by the
    /// database; always executed as `SELECT * FROM (sql) AS q` (read-only
    /// enforced by subquery rules).
    #[serde(default)]
    pub queries: Vec<QueryConfig>,
    /// TLS posture: full sslmode matrix; verify-* modes are expressible only
    /// here (conn-string sslmode covers disable/prefer/require). Contradicting
    /// an explicit conn sslmode is a config error.
    #[serde(default)]
    pub tls: Option<crate::tls::TlsPolicy>,
    /// CDC via logical replication: when present, EVERY configured table is
    /// captured through the
    /// replication slot instead of cursor-column incremental (the two are
    /// mutually exclusive per table). Query streams are unaffected.
    #[serde(default)]
    pub cdc: Option<CdcConfig>,
    /// Decoder cuts a RecordBatch at this many buffered bytes.
    #[serde(default = "default_batch_target_bytes")]
    pub batch_target_bytes: usize,
    /// Secondary cut: maximum rows per batch.
    #[serde(default = "default_batch_max_rows")]
    pub batch_max_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    /// Bare table name; `schema` owns qualification (qualified names rejected).
    pub name: String,
    #[serde(default)]
    pub cursor: Option<CursorConfig>,
    /// Overrides the reflected primary key (dedup/merge key source).
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    /// Mutually exclusive with `excluded_columns`.
    #[serde(default)]
    pub included_columns: Option<Vec<String>>,
    #[serde(default)]
    pub excluded_columns: Option<Vec<String>>,
    /// Per-column type-hint overrides: a CLOSED conversion table; unknown
    /// columns or undefined (source → hint) pairs are typed config errors at
    /// open.
    #[serde(default)]
    pub type_hints: std::collections::BTreeMap<String, crate::source::HintType>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    /// Stream name — unique across tables AND queries.
    pub name: String,
    /// The SELECT/CTE statement (wrapped as a subquery at execution).
    pub sql: String,
    #[serde(default)]
    pub cursor: Option<CursorConfig>,
    /// Declared key (nothing to reflect): dedup keys + merge.
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    #[serde(default)]
    pub type_hints: std::collections::BTreeMap<String, crate::source::HintType>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CursorConfig {
    /// Must exist on the table with a cursor-capable type (validated at open).
    pub column: String,
    /// Typed literal for the first run (absent ⇒ full initial load).
    #[serde(default)]
    pub initial_value: Option<String>,
    #[serde(default = "default_boundary")]
    pub boundary: Bound,
    #[serde(default)]
    pub direction: Direction,
    /// Optional upper bound (typed literal, exclusive under `max` unless
    /// `end_bound: inclusive`).
    #[serde(default)]
    pub end_value: Option<String>,
    /// Upper-bound semantics: `exclusive` (default) or `inclusive` — rows
    /// exactly AT `end_value` load. A read filter only; never resume state.
    #[serde(default = "default_end_bound")]
    pub end_bound: Bound,
    #[serde(default)]
    pub nulls: NullPolicy,
    /// Attribution window: each RESUMED run widens the read window this far
    /// behind the watermark so
    /// late-committed rows are captured. Requires a closed boundary and a
    /// primary key; the saved watermark is never lowered.
    #[serde(default)]
    pub lag: Option<Lag>,
}

/// Lag vocabulary: `"90s"`/`"5m"`/`"2h"`/`"1d"` (time cursors; whole days
/// for `date`) or a plain positive magnitude (`"1000"`, `"0.5"`) for
/// integer/decimal cursors. Config form is the literal string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lag {
    /// Whole seconds (from a duration form).
    Duration { seconds: u64 },
    /// Validated positive numeric literal for numeric cursors.
    Magnitude(String),
}

impl std::str::FromStr for Lag {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let t = s.trim();
        if let Some(unit) = t.chars().last().filter(|c| "smhd".contains(*c)) {
            let count: u64 = t[..t.len() - 1]
                .parse()
                .map_err(|e| format!("lag `{t}`: {e}"))?;
            if count == 0 {
                return Err(format!("lag `{t}` must be positive"));
            }
            let factor = match unit {
                's' => 1,
                'm' => 60,
                'h' => 3600,
                _ => 86400,
            };
            return Ok(Self::Duration {
                seconds: count * factor,
            });
        }
        let numeric = !t.is_empty()
            && t.chars().all(|c| c.is_ascii_digit() || c == '.')
            && t.parse::<f64>().is_ok_and(|v| v > 0.0);
        if numeric {
            Ok(Self::Magnitude(t.to_string()))
        } else {
            Err(format!(
                "lag `{t}` is neither a duration (\"90s\", \"5m\", \"2h\", \"1d\") \
                 nor a positive magnitude"
            ))
        }
    }
}

impl std::fmt::Display for Lag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duration { seconds } => write!(f, "{seconds}s"),
            Self::Magnitude(m) => f.write_str(m),
        }
    }
}

impl serde::Serialize for Lag {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Lag {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let text = String::deserialize(de)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for Lag {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Lag".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        // Mirrors `FromStr` exactly — the string vocabulary IS the config form.
        schemars::json_schema!({
            "type": "string",
            "description": "Attribution window: a duration (\"90s\", \"5m\", \
                            \"2h\", \"1d\") for time cursors, or a positive \
                            magnitude for numeric cursors",
            "pattern": "^([0-9]+[smhd]|[0-9]+(\\.[0-9]+)?)$"
        })
    }
}

impl Lag {
    /// The SQL delta subtracted from (direction max) or added to (min) the
    /// watermark, per cursor family. Err = the pair is undefined — surfaced
    /// as a typed open-time error naming the column.
    pub(crate) fn sql_delta(&self, decode: crate::source::types::Decode) -> Result<String, String> {
        use crate::source::types::Decode;
        match (self, decode) {
            (Self::Duration { seconds }, Decode::Timestamp { .. }) => {
                Ok(format!("INTERVAL '{seconds} seconds'"))
            }
            (Self::Duration { seconds }, Decode::Date) if seconds % 86_400 == 0 => {
                Ok(format!("{}::int4", seconds / 86_400))
            }
            (Self::Duration { .. }, Decode::Date) => {
                Err("date cursors take whole-day lags (e.g. \"2d\")".into())
            }
            (Self::Magnitude(m), Decode::Int2 | Decode::Int4 | Decode::Int8) => {
                if m.contains('.') {
                    Err("integer cursors take integer lags".into())
                } else {
                    Ok(format!("{m}::int8"))
                }
            }
            (Self::Magnitude(m), Decode::Decimal { .. }) => Ok(format!("'{m}'::numeric")),
            (Self::Duration { .. }, Decode::Int2 | Decode::Int4 | Decode::Int8) => {
                Err("integer cursors take a plain magnitude lag, not a duration".into())
            }
            (Self::Magnitude(_), Decode::Timestamp { .. } | Decode::Date) => {
                Err("time cursors take a duration lag (\"90s\", \"5m\", \"1d\")".into())
            }
            _ => Err("lag is not defined for this cursor type".into()),
        }
    }
}

/// Upper-bound semantics for `end_value` (dlt `range_end` parity).
/// Edge semantics for a cursor-window bound — ONE vocabulary for both the
/// resume boundary and the optional end bound. Defaults differ per field
/// (resume: inclusive, so watermark-equal rows re-fetch and dedup; end:
/// exclusive), carried by per-field serde defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Bound {
    /// `>=` / `<=` — rows exactly AT the edge value load. As the resume
    /// boundary this re-fetches watermark-equal rows, deduped via
    /// boundary keys.
    Inclusive,
    /// `>` / `<` — the edge value itself is excluded. As the resume
    /// boundary this skips dedup: safe only for strictly monotonic
    /// cursors.
    Exclusive,
}

fn default_boundary() -> Bound {
    Bound::Inclusive
}

fn default_end_bound() -> Bound {
    Bound::Exclusive
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Ascending cursor, watermark = max seen.
    #[default]
    Max,
    /// Descending cursor, watermark = min seen.
    Min,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum NullPolicy {
    /// NULL-cursor rows are filtered out (`IS NOT NULL`).
    #[default]
    Exclude,
    /// NULL-cursor rows are included on every run (`… OR cursor IS NULL`).
    Include,
    /// A NULL cursor value is a DATA-CONTRACT violation: the run fails with
    /// a typed error naming stream and column — for pipelines that treat NULL
    /// `updated_at` as a bug.
    Error,
}

/// CDC block: slot + publication are USER-OWNED server resources — rdlt
/// creates them only under `create_if_missing` (idempotently) and NEVER drops
/// either. The flag-column collision check needs reflection and runs at open.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CdcConfig {
    /// Replication slot name; single consumer (concurrent use is a typed
    /// error naming the holding pid).
    pub slot: String,
    /// Publication; must cover every CDC table (preflighted at open).
    pub publication: String,
    /// Create slot and publication when absent. rdlt never drops them.
    #[serde(default)]
    pub create_if_missing: bool,
    #[serde(default)]
    pub mode: CdcMode,
    /// Tail-mode quiet wait between chunks (duration forms only: "1s",
    /// "5m", "2h", "1d").
    #[serde(default = "default_idle_wait")]
    pub idle_wait: Wait,
    /// Deletion-flag column emitted on every CDC stream: NULL for
    /// insert/update rows, TRUE for deletes (feature-008 `hard_delete`
    /// turns it into real deletions on postgres destinations).
    #[serde(default = "default_flag_column")]
    pub flag_column: String,
    /// `off` = never advance the slot's acknowledged position (debugging /
    /// fan-in staging) — the server retains WAL indefinitely; documented.
    #[serde(default)]
    pub ack: AckMode,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CdcMode {
    /// Consume the backlog to the run-start WAL position, then finish
    /// (cron-able).
    #[default]
    Catchup,
    /// Chunked catch-up loop until cancelled, `idle_wait` between quiet
    /// chunks (recorded refinement 2).
    Tail,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// Advance the slot once per run, after every stream committed.
    #[default]
    Auto,
    Off,
}

/// A wait interval: the 007 duration vocabulary ("1s", "5m", "2h", "1d")
/// WITHOUT the magnitude forms. Config form is the literal string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Wait {
    pub seconds: u64,
}

impl std::str::FromStr for Wait {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<Lag>() {
            Ok(Lag::Duration { seconds }) => Ok(Self { seconds }),
            _ => Err(format!(
                "wait `{s}` must be a duration (\"1s\", \"5m\", \"2h\", \"1d\")"
            )),
        }
    }
}

impl std::fmt::Display for Wait {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}s", self.seconds)
    }
}

impl serde::Serialize for Wait {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Wait {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let text = String::deserialize(de)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for Wait {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Wait".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A wait interval: \"1s\", \"5m\", \"2h\", \"1d\"",
            "pattern": "^[0-9]+[smhd]$"
        })
    }
}

fn default_idle_wait() -> Wait {
    Wait { seconds: 1 }
}
fn default_flag_column() -> String {
    "_rdlt_deleted".into()
}

fn default_schema() -> String {
    "public".into()
}
fn default_batch_target_bytes() -> usize {
    8 << 20
}
fn default_batch_max_rows() -> usize {
    65_536
}

impl PostgresConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let config: PostgresConfig = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// JSON text form — same document shape and validation as YAML.
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        let config: PostgresConfig = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    /// The embedder entry point: a platform holding connector configs as
    /// JSON documents (validated against the connector's declared config
    /// schema, `ConnectorSpec`) passes the `serde_json::Value` directly —
    /// no string round-trip, same validation as every other entry point.
    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        let config: PostgresConfig = serde_json::from_value(value)?;
        config.validate()?;
        Ok(config)
    }

    /// Local validation (shape rules checkable without the database); rules
    /// that need the live catalog run at open, against reflection. Split by
    /// concern — connection, cursors, CDC, and stream selection — each owning a
    /// coherent slice of the same typed errors.
    fn validate(&self) -> Result<(), ConfigError> {
        self.validate_conn()?;
        self.validate_cursors()?;
        self.validate_cdc()?;
        self.validate_tables()?;
        Ok(())
    }

    /// Top-level connection + batching scalars.
    fn validate_conn(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        if self.conn.trim().is_empty() {
            return invalid("`conn` must not be empty".into());
        }
        // Parse failure = FATAL config error, up front — a malformed conn
        // string must never reach the Transient/retry path. The shared gate
        // also translates libpq's TLS parameter trio and names every rejected
        // parameter — no bare parse errors.
        if let Err(e) = crate::tls::parse_conn(&self.conn, self.tls.as_ref()) {
            return invalid(e.to_string());
        }
        if self.schema.trim().is_empty() {
            return invalid("`schema` must not be empty".into());
        }
        if self.batch_target_bytes == 0 || self.batch_max_rows == 0 {
            return invalid("batch knobs must be positive".into());
        }
        Ok(())
    }

    /// Cursor-shape rules checkable without the catalog.
    fn validate_cursors(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        for cursor in self
            .tables
            .iter()
            .flatten()
            .filter_map(|t| t.cursor.as_ref())
            .chain(self.queries.iter().filter_map(|q| q.cursor.as_ref()))
        {
            if cursor.lag.is_some() && cursor.boundary == Bound::Exclusive {
                return invalid(format!(
                    "cursor `{}`: lag requires an INCLUSIVE boundary (an exclusive boundary \
                     skips the dedup that makes the lag window safe): ",
                    cursor.column
                ));
            }
        }
        Ok(())
    }

    /// The CDC block: required non-empty names + the cursor/cdc exclusivity.
    fn validate_cdc(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        if let Some(cdc) = &self.cdc {
            for (field, value) in [
                ("cdc.slot", &cdc.slot),
                ("cdc.publication", &cdc.publication),
                ("cdc.flag_column", &cdc.flag_column),
            ] {
                if value.trim().is_empty() {
                    return invalid(format!("`{field}` must not be empty"));
                }
            }
            // CDC captures the CONFIGURED tables; selecting none captures
            // nothing, so the slot would never be preflighted or advanced and
            // the block would behave as if it were absent.
            if self.tables.as_ref().is_some_and(Vec::is_empty) {
                return invalid(
                    "`cdc` is configured but `tables` is empty — change data capture reads \
                     the configured tables, so selecting none captures nothing; list the \
                     tables to capture or remove the `cdc` block"
                        .into(),
                );
            }
            // CDC covers every configured table; a cursor block on any
            // of them is a contradiction, not an override.
            if let Some(table) = self.tables.iter().flatten().find(|t| t.cursor.is_some()) {
                return invalid(format!(
                    "table `{}`: `cursor` and `cdc` are mutually exclusive — with a \
                     `cdc:` block every configured table is captured through the \
                     replication slot",
                    table.name
                ));
            }
        }
        Ok(())
    }

    /// Query + table stream selection: unique names, no qualified table names,
    /// exclusive column selections, non-empty overrides.
    fn validate_tables(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        {
            let mut names = BTreeSet::new();
            if let Some(tables) = &self.tables {
                for t in tables {
                    names.insert(t.name.as_str());
                }
            }
            for q in &self.queries {
                if q.name.trim().is_empty() {
                    return invalid("query with empty name".into());
                }
                if q.sql.trim().is_empty() {
                    return invalid(format!("query `{}`: empty sql", q.name));
                }
                if !names.insert(q.name.as_str()) {
                    return invalid(format!(
                        "stream name `{}` used by more than one table/query",
                        q.name
                    ));
                }
                if let Some(pk) = &q.primary_key
                    && pk.is_empty()
                {
                    return invalid(format!("query `{}`: primary_key present but empty", q.name));
                }
            }
        }
        if let Some(tables) = &self.tables {
            // An empty list selects no tables — legitimate alongside queries,
            // and the only way to say "deliver the declared queries and nothing
            // else". With no queries either, the run would select nothing at
            // all and silently move zero rows, so that combination is refused
            // here rather than at read time.
            if tables.is_empty() && self.queries.is_empty() {
                return invalid(
                    "no streams selected: `tables` is empty and no `queries` are declared — \
                     list the tables to read, declare a query, or omit `tables` to discover \
                     every table in the schema"
                        .into(),
                );
            }
            let mut seen = BTreeSet::new();
            for table in tables {
                if table.name.contains('.') {
                    return invalid(format!(
                        "table `{}`: schema-qualified names are rejected; `schema` owns qualification",
                        table.name
                    ));
                }
                if table.name.trim().is_empty() {
                    return invalid("table with empty name".into());
                }
                if !seen.insert(table.name.as_str()) {
                    return invalid(format!("table `{}` listed twice", table.name));
                }
                if table.included_columns.is_some() && table.excluded_columns.is_some() {
                    return invalid(format!(
                        "table `{}`: included_columns and excluded_columns are mutually exclusive",
                        table.name
                    ));
                }
                if let Some(cols) = table
                    .included_columns
                    .as_deref()
                    .or(table.excluded_columns.as_deref())
                    && cols.is_empty()
                {
                    return invalid(format!(
                        "table `{}`: column selection present but empty",
                        table.name
                    ));
                }
                if let Some(pk) = &table.primary_key
                    && pk.is_empty()
                {
                    return invalid(format!(
                        "table `{}`: primary_key present but empty",
                        table.name
                    ));
                }
            }
        }
        Ok(())
    }

    /// The per-table config for a stream name, when the user listed tables.
    pub(crate) fn table_config(&self, name: &str) -> Option<&TableConfig> {
        self.tables.as_ref()?.iter().find(|t| t.name == name)
    }

    pub(crate) fn query_config(&self, name: &str) -> Option<&QueryConfig> {
        self.queries.iter().find(|q| q.name == name)
    }

    /// A query stream's config viewed through the table-config shape, so the
    /// hint/selection/cursor machinery applies unchanged.
    pub(crate) fn synthesized_table_config(&self, name: &str) -> Option<TableConfig> {
        let q = self.query_config(name)?;
        Some(TableConfig {
            name: q.name.clone(),
            cursor: q.cursor.clone(),
            primary_key: q.primary_key.clone(),
            included_columns: None,
            excluded_columns: None,
            type_hints: q.type_hints.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
conn: "postgresql://u:p@localhost:5432/db"
"#;

    #[test]
    fn minimal_defaults() {
        let c = PostgresConfig::from_yaml(MINIMAL).expect("minimal config");
        assert_eq!(c.schema, "public");
        assert!(!c.include_views);
        assert!(c.tables.is_none());
        assert_eq!(c.batch_target_bytes, 8 << 20);
        assert_eq!(c.batch_max_rows, 65_536);
    }

    #[test]
    fn full_document_round_trips() {
        let c = PostgresConfig::from_yaml(
            r#"
conn: "postgresql://u:p@localhost/db"
schema: sales
include_views: true
batch_target_bytes: 1048576
batch_max_rows: 1000
tables:
  - name: orders
    cursor:
      column: updated_at
      initial_value: "2026-01-01T00:00:00Z"
      boundary: exclusive
      direction: min
      end_value: "2027-01-01T00:00:00Z"
      nulls: include
    primary_key: [id]
    excluded_columns: [internal_notes]
  - name: customers
"#,
        )
        .expect("full config");
        let orders = c.table_config("orders").expect("orders");
        let cursor = orders.cursor.as_ref().expect("cursor");
        assert_eq!(cursor.boundary, Bound::Exclusive);
        assert_eq!(cursor.direction, Direction::Min);
        assert_eq!(cursor.nulls, NullPolicy::Include);
        assert!(
            c.table_config("customers")
                .expect("customers")
                .cursor
                .is_none()
        );
    }

    #[test]
    fn unknown_fields_rejected() {
        let err =
            PostgresConfig::from_yaml("conn: host=localhost\nfrobnicate: true\n").unwrap_err();
        assert!(matches!(err, ConfigError::Yaml(_)), "{err}");
    }

    #[test]
    fn qualified_table_name_rejected() {
        let err =
            PostgresConfig::from_yaml("conn: host=localhost\ntables:\n  - name: sales.orders\n")
                .unwrap_err();
        assert!(err.to_string().contains("schema-qualified"), "{err}");
    }

    #[test]
    fn include_exclude_mutually_exclusive() {
        let err = PostgresConfig::from_yaml(
            "conn: host=localhost\ntables:\n  - name: t\n    included_columns: [a]\n    excluded_columns: [b]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
    }

    /// An empty table list means "no tables" — the only way to say "deliver the
    /// declared queries and nothing else". Absent, by contrast, still discovers
    /// every table, so a queries-only pipeline that omits the field receives the
    /// whole schema alongside its queries.
    #[test]
    fn empty_table_list_selects_no_tables_and_keeps_queries() {
        let c = PostgresConfig::from_yaml(
            "conn: host=localhost\ntables: []\nqueries:\n  - name: q\n    sql: SELECT 1\n",
        )
        .expect("empty list alongside a query is a valid selection");
        assert_eq!(c.tables.as_deref(), Some(&[][..]));
        assert_eq!(c.queries.len(), 1);
    }

    /// Selecting nothing at all would move zero rows and report success, so it
    /// is refused where it is knowable — at parse time, from the document alone.
    #[test]
    fn selecting_no_streams_at_all_is_rejected_naming_both_remedies() {
        let err = PostgresConfig::from_yaml("conn: host=localhost\ntables: []\n")
            .expect_err("no tables and no queries selects nothing");
        let msg = err.to_string();
        assert!(msg.contains("no streams selected"), "{msg}");
        assert!(msg.contains("queries") && msg.contains("omit"), "{msg}");
    }

    /// Change data capture reads the configured tables; configuring none would
    /// leave the slot un-preflighted and never advanced, i.e. the block would
    /// silently behave as if it were absent.
    #[test]
    fn cdc_with_an_empty_table_list_is_rejected() {
        let err = PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s1\n  publication: p1\ntables: []\n\
             queries:\n  - name: q\n    sql: SELECT 1\n",
        )
        .expect_err("cdc captures the configured tables; none means nothing");
        assert!(err.to_string().contains("captures nothing"), "{err}");
    }

    #[test]
    fn empty_selections_rejected() {
        for doc in [
            "conn: host=localhost\ntables: []\n",
            "conn: host=localhost\ntables:\n  - name: t\n    included_columns: []\n",
            "conn: host=localhost\ntables:\n  - name: t\n    primary_key: []\n",
            "conn: \"\"\n",
            "conn: host=localhost\nbatch_max_rows: 0\n",
        ] {
            assert!(
                PostgresConfig::from_yaml(doc).is_err(),
                "should reject: {doc}"
            );
        }
    }

    #[test]
    fn cdc_block_parses_with_defaults() {
        let c = PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s1\n  publication: p1\ntables:\n  - name: orders\n",
        )
        .expect("cdc config");
        let cdc = c.cdc.expect("cdc");
        assert_eq!(cdc.slot, "s1");
        assert_eq!(cdc.publication, "p1");
        assert!(!cdc.create_if_missing);
        assert_eq!(cdc.mode, CdcMode::Catchup);
        assert_eq!(cdc.idle_wait, Wait { seconds: 1 });
        assert_eq!(cdc.flag_column, "_rdlt_deleted");
        assert_eq!(cdc.ack, AckMode::Auto);
    }

    #[test]
    fn cdc_validation_matrix() {
        // cursor + cdc on the same table — typed, names the table.
        let err = PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
             tables:\n  - name: orders\n    cursor:\n      column: id\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("`orders`"), "{err}");
        assert!(err.to_string().contains("mutually exclusive"), "{err}");
        // Required + non-empty names.
        for doc in [
            "conn: host=localhost\ncdc:\n  publication: p\n",
            "conn: host=localhost\ncdc:\n  slot: s\n",
            "conn: host=localhost\ncdc:\n  slot: \"\"\n  publication: p\n",
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  flag_column: \"\"\n",
            // unknown fields fail; idle_wait rejects magnitudes.
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  drop_slot: true\n",
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  idle_wait: \"5\"\n",
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n  mode: streaming\n",
        ] {
            assert!(
                PostgresConfig::from_yaml(doc).is_err(),
                "should reject: {doc}"
            );
        }
        // Tail mode + duration idle_wait + ack off parse.
        let c = PostgresConfig::from_yaml(
            "conn: host=localhost\ncdc:\n  slot: s\n  publication: p\n\
             \x20 mode: tail\n  idle_wait: \"5m\"\n  ack: off\n",
        )
        .expect("tail config");
        let cdc = c.cdc.expect("cdc");
        assert_eq!(cdc.mode, CdcMode::Tail);
        assert_eq!(cdc.idle_wait, Wait { seconds: 300 });
        assert_eq!(cdc.ack, AckMode::Off);
    }

    #[test]
    fn json_and_value_entry_points_share_validation() {
        let json =
            r#"{"conn": "host=localhost", "tables": [{"name": "t", "cursor": {"column": "id"}}]}"#;
        let from_json = PostgresConfig::from_json(json).expect("json");
        let from_yaml = PostgresConfig::from_yaml(
            "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: id\n",
        )
        .expect("yaml");
        assert_eq!(from_json, from_yaml, "one document shape, two syntaxes");
        let value: serde_json::Value = serde_json::from_str(json).expect("value");
        assert_eq!(PostgresConfig::from_value(value).expect("value"), from_json);
        // Validation is shared: the parse gate fires on every entry point.
        assert!(PostgresConfig::from_json(r#"{"conn": "not a conn"}"#).is_err());
        assert!(
            PostgresConfig::from_value(serde_json::json!({"conn": "x", "unknown": 1})).is_err(),
            "deny_unknown_fields holds for Value too"
        );
    }

    #[test]
    fn conn_parse_gate_and_tls_policy() {
        // Contract rule 1: parse failure = typed CONFIG error, up front.
        let err = PostgresConfig::from_yaml("conn: not-a-conn-string\n").unwrap_err();
        assert!(err.to_string().contains("does not parse"), "{err}");
        // TLS is wired — every conn-string sslmode level now passes config
        // validation (incl. the spaced keyword form).
        for conn in [
            "postgresql://u:p@h/db?sslmode=require",
            "host=h sslmode=require",
            "host=h sslmode = require",
            "host=h sslmode=prefer",
            "host=h sslmode=disable",
        ] {
            assert!(
                PostgresConfig::from_yaml(&format!("conn: \"{conn}\"\n")).is_ok(),
                "{conn} must validate"
            );
        }
        // Contradiction rule (tls-policy.md): explicit conn sslmode reversed
        // by the block = typed config error; refinement is allowed.
        let err = PostgresConfig::from_yaml(
            "conn: \"host=h sslmode=disable\"\ntls:\n  mode: verify_full\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("contradicts"), "{err}");
        assert!(
            PostgresConfig::from_yaml(
                "conn: \"host=h sslmode=require\"\ntls:\n  mode: verify_full\n"
            )
            .is_ok(),
            "require -> verify_full is refinement, not contradiction"
        );
    }

    #[test]
    fn duplicate_tables_rejected() {
        let err =
            PostgresConfig::from_yaml("conn: host=localhost\ntables:\n  - name: t\n  - name: t\n")
                .unwrap_err();
        assert!(err.to_string().contains("listed twice"), "{err}");
    }

    // ---- lag vocabulary + validation ----

    #[test]
    fn lag_vocabulary_round_trips_and_rejects() {
        use crate::source::types::Decode;
        // Duration forms.
        assert_eq!("90s".parse::<Lag>().unwrap(), Lag::Duration { seconds: 90 });
        assert_eq!("5m".parse::<Lag>().unwrap(), Lag::Duration { seconds: 300 });
        assert_eq!(
            "2h".parse::<Lag>().unwrap(),
            Lag::Duration { seconds: 7200 }
        );
        assert_eq!(
            "1d".parse::<Lag>().unwrap(),
            Lag::Duration { seconds: 86_400 }
        );
        // Magnitudes.
        assert_eq!(
            "1000".parse::<Lag>().unwrap(),
            Lag::Magnitude("1000".into())
        );
        assert_eq!("0.5".parse::<Lag>().unwrap(), Lag::Magnitude("0.5".into()));
        // Rejections: zero, negative, garbage, empty.
        for bad in ["0s", "-5m", "soon", "", "5 m"] {
            assert!(bad.parse::<Lag>().is_err(), "{bad}");
        }
        // Display round-trips through FromStr semantically.
        let lag: Lag = "5m".parse().unwrap();
        assert_eq!(lag.to_string().parse::<Lag>().unwrap(), lag);

        // sql_delta family matrix.
        let five_m = Lag::Duration { seconds: 300 };
        assert_eq!(
            five_m.sql_delta(Decode::Timestamp { tz: true }).unwrap(),
            "INTERVAL '300 seconds'"
        );
        let two_d = Lag::Duration { seconds: 172_800 };
        assert_eq!(two_d.sql_delta(Decode::Date).unwrap(), "2::int4");
        assert!(five_m.sql_delta(Decode::Date).is_err(), "sub-day on date");
        let thousand = Lag::Magnitude("1000".into());
        assert_eq!(thousand.sql_delta(Decode::Int8).unwrap(), "1000::int8");
        let half = Lag::Magnitude("0.5".into());
        assert!(half.sql_delta(Decode::Int8).is_err(), "fractional on int");
        assert_eq!(
            half.sql_delta(Decode::Decimal {
                precision: 10,
                scale: 2
            })
            .unwrap(),
            "'0.5'::numeric"
        );
        // Undefined families and unit mismatches.
        assert!(five_m.sql_delta(Decode::Utf8).is_err(), "text cursor");
        assert!(thousand.sql_delta(Decode::Timestamp { tz: false }).is_err());
        assert!(five_m.sql_delta(Decode::Int8).is_err());
    }

    #[test]
    fn lag_with_open_boundary_dies_at_config_parse() {
        let err = PostgresConfig::from_yaml(
            "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: ts\n      boundary: exclusive\n      lag: \"5m\"\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("INCLUSIVE boundary"), "{err}");
        // Closed (default) parses fine.
        PostgresConfig::from_yaml(
            "conn: host=localhost\ntables:\n  - name: t\n    cursor:\n      column: ts\n      lag: \"5m\"\n",
        )
        .expect("closed + lag parses");
    }
}

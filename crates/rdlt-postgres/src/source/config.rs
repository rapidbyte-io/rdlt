//! Declarative Postgres source configuration (contract:
//! `specs/005-postgres-source/contracts/source-config.md`): connection, stream
//! selection, per-table cursor + key configuration, batching knobs. One YAML
//! document a platform can render and validate; unknown fields are errors.
//!
//! There is deliberately NO retry configuration — retry policy is engine-owned
//! (SPI clauses S3/E5); this source only classifies errors.

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// libpq-style connection string/URL. TLS is not yet wired for the
    /// postgres connectors: `sslmode=require`/`verify-*` is rejected at open.
    pub conn: String,
    /// Reflection scope; bare table names below resolve inside it.
    #[serde(default = "default_schema")]
    pub schema: String,
    /// Include views and materialized views in schema-wide discovery.
    #[serde(default)]
    pub include_views: bool,
    /// Absent ⇒ discover ALL tables in `schema`.
    #[serde(default)]
    pub tables: Option<Vec<TableConfig>>,
    /// TLS posture (feature 006): full sslmode matrix; verify-* modes are
    /// expressible only here (conn-string sslmode covers disable/prefer/
    /// require). Contradicting an explicit conn sslmode is a config error.
    #[serde(default)]
    pub tls: Option<crate::tls::TlsPolicy>,
    /// Decoder cuts a RecordBatch at this many buffered bytes (R4).
    #[serde(default = "default_batch_target_bytes")]
    pub batch_target_bytes: usize,
    /// Secondary cut: maximum rows per batch.
    #[serde(default = "default_batch_max_rows")]
    pub batch_max_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Per-column type-hint overrides (feature 006, contract
    /// type-hints.md): a CLOSED conversion table; unknown columns or
    /// undefined (source → hint) pairs are typed config errors at open.
    #[serde(default)]
    pub type_hints: std::collections::BTreeMap<String, crate::source::HintType>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorConfig {
    /// Must exist on the table with a cursor-capable type (validated at open).
    pub column: String,
    /// Typed literal for the first run (absent ⇒ full initial load).
    #[serde(default)]
    pub initial_value: Option<String>,
    #[serde(default)]
    pub boundary: Boundary,
    #[serde(default)]
    pub direction: Direction,
    /// Optional upper bound (typed literal, exclusive under `max`).
    #[serde(default)]
    pub end_value: Option<String>,
    #[serde(default)]
    pub nulls: NullPolicy,
}

/// Lower-bound semantics on resume (dlt parity).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
    /// `>=` — watermark-equal rows re-fetched and deduped via boundary keys.
    #[default]
    Closed,
    /// `>` — no dedup; safe only for strictly monotonic cursors.
    Open,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Ascending cursor, watermark = max seen.
    #[default]
    Max,
    /// Descending cursor, watermark = min seen.
    Min,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullPolicy {
    /// NULL-cursor rows are filtered out (`IS NOT NULL`).
    #[default]
    Exclude,
    /// NULL-cursor rows are included on every run (`… OR cursor IS NULL`).
    Include,
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

    /// Local validation (contract rules 4–6 and shape rules); rules that need
    /// the live catalog (2–3) run at open, against reflection.
    fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |msg: String| Err(ConfigError::Invalid(msg));
        if self.conn.trim().is_empty() {
            return invalid("`conn` must not be empty".into());
        }
        // Contract rule 1: parse failure = FATAL config error, up front — a
        // malformed conn string must never reach the Transient/retry path
        // (005 review). The same parsed form decides the TLS policy, so the
        // spaced keyword form (`sslmode = require`) is covered too.
        match self.conn.parse::<tokio_postgres::Config>() {
            Err(e) => return invalid(format!("`conn` does not parse: {e}")),
            Ok(parsed) => {
                // Feature 006: TLS is wired — validate mode consistency here
                // (fail fast); root resolution happens at open.
                if let Err(e) = crate::tls::resolve_policy(&parsed, self.tls.as_ref()) {
                    return invalid(e.to_string());
                }
            }
        }
        if self.schema.trim().is_empty() {
            return invalid("`schema` must not be empty".into());
        }
        if self.batch_target_bytes == 0 || self.batch_max_rows == 0 {
            return invalid("batch knobs must be positive".into());
        }
        if let Some(tables) = &self.tables {
            if tables.is_empty() {
                return invalid("`tables` present but empty — omit it to discover all".into());
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
      boundary: open
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
        assert_eq!(cursor.boundary, Boundary::Open);
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
        // Feature 006: TLS is wired — every conn-string sslmode level now
        // passes config validation (incl. the spaced keyword form).
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
}

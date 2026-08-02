//! The source document: connection facts, and streams over tables —
//! parse-then-validate through the sdk Document gate.

use std::collections::BTreeMap;

use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::secret::Secret;

/// The whole source document.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    /// The database host.
    pub host: String,
    /// The listener port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// The service name (a PDB service such as `FREEPDB1`).
    pub service: String,
    pub user: String,
    pub password: Secret,
    /// The streams to read; at least one.
    #[schemars(length(min = 1))]
    pub streams: Vec<Stream>,
}

fn default_port() -> u16 {
    1521
}

/// One stream: a table read incrementally by an optional cursor
/// column.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Stream {
    pub name: String,
    /// The table to read. Bare names fold UPPERCASE (Oracle's own
    /// rule); the connector always emits the quoted form.
    pub table: String,
    /// Watermark column for incremental reads (numeric or timestamp).
    #[serde(default)]
    pub cursor: Option<String>,
    /// Keyed identity for the engine's merge/dedup layers.
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    /// Per-column type hints forwarded to the shredder.
    #[serde(default)]
    pub type_hints: BTreeMap<String, String>,
}

/// Parse and validation failures, typed, with the sdk from-text
/// framings.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid oracle source YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid oracle source JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid oracle source config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |m: String| Err(ConfigError::Invalid(m));
        if self.host.is_empty() {
            return invalid("`host` must not be empty".into());
        }
        if self.service.is_empty() {
            return invalid("`service` must not be empty".into());
        }
        if self.streams.is_empty() {
            return invalid("at least one stream is required".into());
        }
        // Duplicate names are refused at the gate (the 029-031
        // shared-table precedent): the reader resolves streams by
        // name, and a duplicate is silently shadowed on read.
        let mut seen = std::collections::BTreeSet::new();
        for stream in &self.streams {
            if !seen.insert(stream.name.as_str()) {
                return invalid(format!(
                    "duplicate stream name `{}` — stream names must be unique",
                    stream.name
                ));
            }
            if stream.name.is_empty() {
                return invalid("stream names must not be empty".into());
            }
            if stream.table.is_empty() {
                return invalid(format!(
                    "stream `{}`: `table` must not be empty",
                    stream.name
                ));
            }
        }
        Ok(())
    }
}

/// The generated declaration, from the same structs the parser reads.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}

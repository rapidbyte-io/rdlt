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
    /// Exactly one of `service` or `sid` is required.
    #[serde(default)]
    pub service: Option<String>,
    /// The legacy SID, for instances that predate service names —
    /// the shape older estates still hand out.
    #[serde(default)]
    pub sid: Option<String>,
    pub user: String,
    pub password: Secret,
    /// Connection and fetch tuning. Absent means the defaults.
    #[serde(default)]
    pub tuning: Tuning,
    /// The streams to read; at least one.
    #[schemars(length(min = 1))]
    pub streams: Vec<Stream>,
}

/// The knobs an Oracle operator expects to turn.
///
/// The names are rdlt's, but each one is the JDBC parameter an Oracle
/// estate already tunes, so a known-good JDBC string translates
/// directly:
/// `defaultRowPrefetch` → `page_rows`,
/// `oracle.jdbc.defaultLobPrefetchSize` → `lob_chunk_bytes`,
/// `oracle.net.CONNECT_TIMEOUT` → `connect_timeout_ms`,
/// `oracle.jdbc.ReadTimeout` → `read_timeout_ms`,
/// and the session's SDU → `sdu_bytes`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Tuning {
    /// Rows per round trip. The connector DERIVES a safe page from
    /// the described column widths; this only ever LOWERS it, because
    /// a page above the derived size cannot fit one SDU reply.
    #[serde(default)]
    pub page_rows: Option<u32>,
    /// Bytes per LOB read round trip.
    #[serde(default = "default_lob_chunk")]
    pub lob_chunk_bytes: u64,
    /// How long a connect attempt may take.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    /// How long ONE statement may take before it is abandoned. The
    /// connection is dropped when it fires — a statement that timed
    /// out has left the protocol mid-conversation.
    #[serde(default = "default_read_timeout")]
    pub read_timeout_ms: u64,
    /// The session data unit the server negotiates. Raise it here
    /// only if the LISTENER was raised too; the reply must fit one.
    #[serde(default = "default_sdu")]
    pub sdu_bytes: u32,
}

fn default_lob_chunk() -> u64 {
    1 << 20
}
fn default_connect_timeout() -> u64 {
    60_000
}
fn default_read_timeout() -> u64 {
    600_000
}
fn default_sdu() -> u32 {
    8192
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            page_rows: None,
            lob_chunk_bytes: default_lob_chunk(),
            connect_timeout_ms: default_connect_timeout(),
            read_timeout_ms: default_read_timeout(),
            sdu_bytes: default_sdu(),
        }
    }
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
        match (&self.service, &self.sid) {
            (Some(s), None) if !s.is_empty() => {}
            (None, Some(s)) if !s.is_empty() => {}
            (Some(_), Some(_)) => {
                return invalid(
                    "`service` and `sid` are two ways to name one instance — set one".into(),
                );
            }
            _ => {
                return invalid("one of `service` (modern) or `sid` (legacy) is required".into());
            }
        }
        if self.tuning.page_rows == Some(0) {
            return invalid("`tuning.page_rows` is 0 — a page must hold at least one row".into());
        }
        if self.tuning.lob_chunk_bytes == 0 {
            return invalid("`tuning.lob_chunk_bytes` is 0 — a LOB read must make progress".into());
        }
        // The page size is derived from THIS number, but the server
        // negotiates the real SDU and the driver reads one packet per
        // reply — and it exposes no accessor for what was agreed. So a
        // value above the universally-accepted default could size
        // pages for packets the server will never send, truncating
        // replies while reporting success. Until the negotiated value
        // is readable, the safe ceiling is the default.
        if self.tuning.sdu_bytes < 512 || self.tuning.sdu_bytes > 8192 {
            return invalid(format!(
                "`tuning.sdu_bytes` is {} — this build reads one packet per reply and \
                 cannot confirm what the server negotiated, so the supported range is \
                 512..=8192",
                self.tuning.sdu_bytes
            ));
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

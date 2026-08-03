//! The source document: connection facts, and streams over tables —
//! parse-then-validate through the sdk Document gate.

use std::collections::BTreeMap;

use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::secret::Secret;

/// The whole source document.
/// NOT `Serialize` — deliberately. `password` is a `Secret`, whose
/// serde impl is `transparent` over the String, so a derived
/// `Serialize` would print the credential in full to anything that
/// dumped a config. `Debug` is redacted; serialization has no such
/// guard, so the capability is simply absent.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, schemars::JsonSchema)]
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
/// and `oracle.jdbc.implicitStatementCacheSize` → `statement_cache`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Tuning {
    /// Rows the CLIENT prefetches per round trip.
    ///
    /// This is a throughput knob, not a correctness one: the read
    /// streams a single cursor whatever the value, so raising it
    /// trades client memory for fewer round trips. Absent means the
    /// driver's own default.
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
    /// Rows per Arrow batch pushed downstream.
    ///
    /// The batch is the unit of backpressure AND of the checkpoint
    /// that follows it, so this trades memory against how much work a
    /// crash costs. It is not a protocol limit — the old page size
    /// was, because a reply had to fit one network packet.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: u32,
    /// Statements the driver may keep open for reuse.
    #[serde(default = "default_statement_cache")]
    pub statement_cache: u32,
    /// The largest single LOB value a read will materialize.
    ///
    /// A LOB is read whole into memory before it becomes a JSON
    /// string, and a page holds many rows, so without a ceiling peak
    /// memory is a property of the DATA rather than of anything
    /// configured. Exceeding it fails the stream by name; raise it
    /// deliberately if the estate really holds values that large.
    #[serde(default = "default_max_lob")]
    pub max_lob_bytes: u64,
    /// TCP keepalive idle time, or `0` to switch keepalive off.
    ///
    /// On by default, unlike the driver: a firewall or NAT that reaps
    /// an idle connection does so SILENTLY, and the read then waits
    /// out its whole `read_timeout_ms` against a socket nothing will
    /// answer. This is `oracle.net.keepAlive`.
    #[serde(default = "default_keepalive")]
    pub keepalive_secs: u64,
}

/// 256 MiB: generous for documents and images, far below the point
/// where one value decides the process's fate.
fn default_max_lob() -> u64 {
    256 << 20
}

/// Well below the 300 s idle timeout common to firewalls and NAT.
fn default_keepalive() -> u64 {
    30
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
fn default_batch_rows() -> u32 {
    8_192
}
fn default_statement_cache() -> u32 {
    20
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            page_rows: None,
            lob_chunk_bytes: default_lob_chunk(),
            connect_timeout_ms: default_connect_timeout(),
            read_timeout_ms: default_read_timeout(),
            batch_rows: default_batch_rows(),
            statement_cache: default_statement_cache(),
            max_lob_bytes: default_max_lob(),
            keepalive_secs: default_keepalive(),
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
    /// Per-column type OVERRIDES.
    ///
    /// The connector already declares every column from the server's
    /// own describe, so this is only needed where that derivation is
    /// deliberately conservative — chiefly BARE `NUMBER` (no declared
    /// scale), which lands `Utf8` because Oracle will accept any
    /// magnitude in it. An operator who knows the real domain says so
    /// here.
    #[serde(default)]
    pub type_hints: BTreeMap<String, HintType>,
}

/// The type vocabulary an operator may override a column with.
///
/// A CLOSED enum, not free text: serde refuses an unknown spelling at
/// parse, so a typo fails the document instead of being accepted and
/// then silently ignored.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum HintType {
    Bool,
    Int64,
    Float64,
    Utf8,
    Binary,
    Json,
    TimestampTz,
    TimestampNaive,
    Date,
    /// Exact decimal; both bounds cap at Oracle's own 38.
    Decimal { precision: u8, scale: u8 },
}

impl From<HintType> for rdlt_connector_sdk::spi::core::LogicalType {
    fn from(hint: HintType) -> Self {
        use rdlt_connector_sdk::spi::core::LogicalType as L;
        match hint {
            HintType::Bool => L::Bool,
            HintType::Int64 => L::Int64,
            HintType::Float64 => L::Float64,
            HintType::Utf8 => L::Utf8,
            HintType::Binary => L::Binary,
            HintType::Json => L::Json,
            HintType::TimestampTz => L::TimestampTz,
            HintType::TimestampNaive => L::TimestampNaive,
            HintType::Date => L::Date,
            HintType::Decimal { precision, scale } => L::Decimal { precision, scale },
        }
    }
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

impl Config {
    /// The Easy Connect descriptor the driver takes.
    ///
    /// A SID is spelled with a colon rather than a slash — the legacy
    /// form older estates still hand out.
    pub(crate) fn connect_string(&self) -> String {
        match (&self.service, &self.sid) {
            (Some(service), _) => format!("//{}:{}/{}", self.host, self.port, service),
            (_, Some(sid)) => format!("{}:{}:{}", self.host, self.port, sid),
            _ => format!("//{}:{}", self.host, self.port),
        }
    }
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
        if self.user.is_empty() {
            return invalid("`user` must not be empty".into());
        }
        if self.password.reveal().is_empty() {
            return invalid("`password` must not be empty".into());
        }
        if self.tuning.page_rows == Some(0) {
            return invalid("`tuning.page_rows` is 0 — a page must hold at least one row".into());
        }
        if self.tuning.lob_chunk_bytes == 0 {
            return invalid("`tuning.lob_chunk_bytes` is 0 — a LOB read must make progress".into());
        }
        if self.tuning.batch_rows == 0 {
            return invalid("`tuning.batch_rows` is 0 — a batch must hold at least one row".into());
        }
        if self.tuning.max_lob_bytes == 0 {
            return invalid("`tuning.max_lob_bytes` is 0 — no LOB could ever be read".into());
        }
        // socket2 clamps an out-of-range idle time to c_int::MAX,
        // which the kernel then treats as "effectively never" — the
        // exact OPPOSITE of what the knob says it does. Refuse it
        // instead of silently meaning "off".
        if self.tuning.keepalive_secs > 86_400 {
            return invalid(format!(
                "`tuning.keepalive_secs` is {} — the supported range is 0 (off) to 86400",
                self.tuning.keepalive_secs
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
            if stream.cursor.as_deref() == Some("") {
                return invalid(format!(
                    "stream `{}`: `cursor` is empty — omit it to read the stream in full",
                    stream.name
                ));
            }
            // `primary_key: []` is not "no key": the engine matches
            // on a NON-EMPTY key list and otherwise falls back to
            // hashing the whole row, so two versions of one business
            // row become two identities and dedup keeps both — with
            // no diagnostic at any layer. Omitting the field means
            // that on purpose; an empty list means it by accident.
            if stream.primary_key.as_deref() == Some(&[]) {
                return invalid(format!(
                    "stream `{}`: `primary_key` is an empty list — omit the field to key rows \
                     by content hash, or name the columns",
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

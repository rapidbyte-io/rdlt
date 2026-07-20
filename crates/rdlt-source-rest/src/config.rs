//! Declarative REST source configuration (spec FR-018): base URL, auth, pagination
//! strategy, incremental cursor, per-column type hints — everything in one YAML/JSON
//! document a platform can render and validate.

use std::collections::BTreeMap;

use rdlt_connector::core::LogicalType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RestConfig {
    /// e.g. `https://api.example.com`
    pub base_url: String,
    #[serde(default)]
    pub auth: Auth,
    pub streams: Vec<RestStream>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Auth {
    #[default]
    None,
    /// `Authorization: Bearer <token>`
    Bearer {
        token: String,
    },
    /// Arbitrary header.
    Header {
        name: String,
        value: String,
    },
    Basic {
        username: String,
        password: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RestStream {
    /// Stream name (becomes the root table name).
    pub name: String,
    /// Path joined onto `base_url`, e.g. `/issues`.
    pub path: String,
    /// Extra query parameters sent with every request.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// Dot-separated path to the records array in the response body; omitted = the
    /// body IS the array (that case streams bytes straight to the shredder).
    #[serde(default)]
    pub records_path: Option<String>,
    #[serde(default)]
    pub pagination: Pagination,
    /// Field carrying the incremental cursor; its max observed value checkpoints the
    /// stream and resumes it via `cursor_param`.
    #[serde(default)]
    pub cursor_field: Option<String>,
    /// Query parameter the API accepts for "only records after this cursor".
    #[serde(default)]
    pub cursor_param: Option<String>,
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    /// Per-column logical types overriding inference, e.g. `created_at: timestamp_tz`.
    #[serde(default)]
    pub type_hints: BTreeMap<String, HintType>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Pagination {
    /// Single request.
    #[default]
    None,
    /// `?<page_param>=N`, starting at `start`, until a page returns no records.
    Page {
        #[serde(default = "default_page_param")]
        page_param: String,
        #[serde(default = "default_page_start")]
        start: u64,
    },
    /// `?<offset_param>=N&<limit_param>=page_size` until a short page.
    Offset {
        #[serde(default = "default_offset_param")]
        offset_param: String,
        #[serde(default = "default_limit_param")]
        limit_param: String,
        page_size: u64,
    },
}

fn default_page_param() -> String {
    "page".into()
}
fn default_page_start() -> u64 {
    1
}
fn default_offset_param() -> String {
    "offset".into()
}
fn default_limit_param() -> String {
    "limit".into()
}

/// Human-friendly hint names in YAML, mapped onto the engine's logical types.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HintType {
    Bool,
    Int64,
    Float64,
    Utf8,
    TimestampTz,
    Date,
    Time,
    Uuid,
    Json,
}

impl From<HintType> for LogicalType {
    fn from(hint: HintType) -> Self {
        match hint {
            HintType::Bool => LogicalType::Bool,
            HintType::Int64 => LogicalType::Int64,
            HintType::Float64 => LogicalType::Float64,
            HintType::Utf8 => LogicalType::Utf8,
            HintType::TimestampTz => LogicalType::TimestampTz,
            HintType::Date => LogicalType::Date,
            HintType::Time => LogicalType::Time,
            HintType::Uuid => LogicalType::Uuid,
            HintType::Json => LogicalType::Json,
        }
    }
}

impl RestConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let config: RestConfig = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// JSON text form — same document shape and validation as YAML.
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        let config: RestConfig = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    /// The embedder entry point: a platform holding connector configs as
    /// JSON documents (validated against the connector's declared config
    /// schema, `ConnectorSpec`) passes the `serde_json::Value` directly —
    /// no string round-trip, same validation as every other entry point.
    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        let config: RestConfig = serde_json::from_value(value)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.streams.is_empty() {
            return Err(ConfigError::Invalid(
                "at least one stream is required".into(),
            ));
        }
        for stream in &self.streams {
            if stream.cursor_field.is_some() != stream.cursor_param.is_some() {
                return Err(ConfigError::Invalid(format!(
                    "stream `{}`: cursor_field and cursor_param must be set together",
                    stream.name
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid REST source YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid REST source JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid REST source config: {0}")]
    Invalid(String),
}

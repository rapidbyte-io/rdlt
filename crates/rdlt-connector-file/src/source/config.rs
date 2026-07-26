//! File source configuration: the `streams` list and each stream's format, location,
//! path/glob, and per-format options, all deserialized from the pipeline's YAML.

use std::collections::BTreeMap;

use rdlt_connector::core::LogicalType;
use serde::{Deserialize, Serialize};

pub use crate::formats::CsvOptions;
// `Format` is re-exported once, at the crate root (`rdlt_connector_file::Format`);
// here it is only brought into scope for the config field types.
use crate::formats::Format;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct FileConfig {
    /// At least one stream (mirrors `validate()`).
    #[schemars(length(min = 1))]
    pub streams: Vec<FileStream>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct FileStream {
    pub name: String,
    /// Explicit — the format is never inferred from the file extension.
    pub format: Format,
    /// Where the files live: absent = local filesystem; `{s3: {...}}` =
    /// S3-compatible object storage.
    #[serde(default)]
    pub location: Option<crate::location::LocationOptions>,
    /// CSV reader options (only with `format: csv`; typed otherwise).
    #[serde(default)]
    pub csv: Option<CsvOptions>,
    /// Explicit file path or glob pattern. An explicitly named missing file is an
    /// error; an empty glob is an empty stream.
    pub path: String,
    /// Record-stream identity columns. Honoured for jsonl and csv; declaring it
    /// on a parquet stream is REFUSED at configuration time (parquet streams are
    /// structured and carry no per-row identity), so it is never silently
    /// dropped.
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    /// Per-column type pins. Applied to the record formats — csv reads them
    /// directly, jsonl carries them into the shredder. They have no effect on a
    /// parquet stream, whose types come from the file's own schema, and that
    /// case is currently ACCEPTED AND IGNORED rather than refused.
    #[serde(default)]
    pub type_hints: BTreeMap<String, HintType>,
    /// JSONL only: validate each line before pushing so malformed input fails naming
    /// the file + byte offset (default true; disable for maximum throughput — errors
    /// then surface from the engine with stream-level context only).
    ///
    /// On csv and parquet streams this field is ACCEPTED AND IGNORED — it is not
    /// refused, so setting it there does nothing and says nothing.
    #[serde(default = "default_validate")]
    pub validate: bool,
}

fn default_validate() -> bool {
    true
}

/// Same human-friendly hint names as the REST source.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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

impl FileConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let config: FileConfig = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// JSON text form — same document shape and validation as YAML.
    pub fn from_json(json: &str) -> Result<Self, ConfigError> {
        let config: FileConfig = serde_json::from_str(json)?;
        config.validate()?;
        Ok(config)
    }

    /// The embedder entry point: a platform holding connector configs as
    /// JSON documents (validated against the connector's declared config
    /// schema, `ConnectorSpec`) passes the `serde_json::Value` directly —
    /// no string round-trip, same validation as every other entry point.
    pub fn from_value(value: serde_json::Value) -> Result<Self, ConfigError> {
        let config: FileConfig = serde_json::from_value(value)?;
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
            if let Some(location) = &stream.location {
                location
                    .validate(&format!("stream `{}`", stream.name))
                    .map_err(ConfigError::Invalid)?;
            }
            if let Some(csv) = &stream.csv {
                if stream.format != Format::Csv {
                    return Err(ConfigError::Invalid(format!(
                        "stream `{}`: a `csv` options block requires `format: csv`",
                        stream.name
                    )));
                }
                csv.validate(&format!("stream `{}`", stream.name))
                    .map_err(ConfigError::Invalid)?;
            }
            if stream.format == Format::Parquet && stream.primary_key.is_some() {
                return Err(ConfigError::Invalid(format!(
                    "stream `{}`: parquet streams are structured and cannot declare \
                     primary_key (no per-row identity)",
                    stream.name
                )));
            }
            if stream.format == Format::Parquet
                && !crate::formats::codec_of(&stream.path).is_plain()
            {
                return Err(ConfigError::Invalid(format!(
                    "stream `{}`: parquet carries its own internal codecs — a \
                     compression extension on a parquet path is not supported",
                    stream.name
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid file source YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid file source JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid file source config: {0}")]
    Invalid(String),
}

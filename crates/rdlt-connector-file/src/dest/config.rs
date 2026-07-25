//! File destination configuration: output format, location (local | S3-compatible),
//! optional partition column. The plain `ParquetDir::open(path)` form is equivalent to
//! local + parquet + no partitioning (frozen spelling, kept for compatibility).

use rdlt_connector::ParquetOptions;
use serde::{Deserialize, Serialize};

use crate::location::LocationOptions;

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DestFormat {
    #[default]
    Parquet,
    Jsonl,
}

impl DestFormat {
    pub(crate) fn extension(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Jsonl => "jsonl",
        }
    }

    /// The spelling used in error messages — the same word the user wrote in
    /// their configuration. Kept separate from [`Self::extension`], which
    /// happens to coincide today but answers a different question.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Jsonl => "jsonl",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct FileDestConfig {
    /// Output directory (local) or key prefix (object store).
    pub path: String,
    /// Absent = local filesystem.
    #[serde(default)]
    pub location: Option<LocationOptions>,
    #[serde(default)]
    pub format: DestFormat,
    /// Optional partition column: one prefix per value
    /// (`<table>/<column>=<value>/...`; NULLs land under `__null__`). The
    /// column must exist in the stream's schema at write time (typed).
    #[serde(default)]
    pub partition_by: Option<String>,
    /// How to write parquet. Absent uses the defaults, which compress —
    /// see [`ParquetOptions`]. Meaningless under `format: jsonl`, and
    /// rejected there rather than ignored.
    #[serde(default)]
    pub parquet: Option<ParquetOptions>,
}

impl FileDestConfig {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            location: None,
            format: DestFormat::Parquet,
            partition_by: None,
            parquet: None,
        }
    }

    pub fn with_location(mut self, location: LocationOptions) -> Self {
        self.location = Some(location);
        self
    }

    pub fn with_format(mut self, format: DestFormat) -> Self {
        self.format = format;
        self
    }

    pub fn with_partition_by(mut self, column: impl Into<String>) -> Self {
        self.partition_by = Some(column.into());
        self
    }

    pub fn with_parquet(mut self, parquet: ParquetOptions) -> Self {
        self.parquet = Some(parquet);
        self
    }

    /// The parquet settings this destination will actually write with —
    /// the configured block, or the defaults. Callers use this rather than
    /// reading `parquet` directly, so "absent" cannot be mistaken for
    /// "uncompressed".
    pub fn parquet_options(&self) -> ParquetOptions {
        self.parquet.clone().unwrap_or_default()
    }

    /// Eager, typed validation following the one config convention: every
    /// message is prefixed by `context` (the subject), and nested blocks
    /// receive the SAME context, so source and destination configs read
    /// identically. The SPI mapping (String → `DestinationError`) stays at the caller.
    pub fn validate(&self, context: &str) -> Result<(), String> {
        if self.path.is_empty() {
            return Err(format!("{context}: `path` must not be empty"));
        }
        if let Some(location) = &self.location {
            location.validate(context)?;
        }
        if let Some(column) = &self.partition_by
            && column.is_empty()
        {
            return Err(format!("{context}: `partition_by` must name a column"));
        }
        if let Some(parquet) = &self.parquet {
            // This rule lives HERE rather than on `ParquetOptions::validate`
            // because it needs the sibling `format` field, which the options
            // themselves cannot see. A `parquet:` block under `jsonl` is a
            // mistake worth refusing: honouring it is impossible and ignoring
            // it would leave the user believing they had configured something.
            if self.format != DestFormat::Parquet {
                return Err(format!(
                    "{context}: a `parquet` block is set but `format` is `{}` — \
                     these settings only apply to parquet output; remove the block, \
                     or set `format: parquet`",
                    self.format.as_str()
                ));
            }
            parquet
                .validate()
                .map_err(|message| format!("{context}: {message}"))?;
        }
        Ok(())
    }
}

/// JSON Schema for the destination config document (round-trip tested).
pub fn dest_config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(FileDestConfig)).expect("schema serializes")
}

//! File destination configuration: output format, location (local | S3-compatible),
//! optional partition column. The plain `ParquetDir::open(path)` form is equivalent to
//! local + parquet + no partitioning (frozen spelling, kept for compatibility).

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
}

impl FileDestConfig {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            location: None,
            format: DestFormat::Parquet,
            partition_by: None,
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

    pub fn validate(&self) -> Result<(), String> {
        if self.path.is_empty() {
            return Err("file destination: `path` must not be empty".into());
        }
        if let Some(location) = &self.location {
            location.validate("file destination")?;
        }
        if let Some(column) = &self.partition_by
            && column.is_empty()
        {
            return Err("file destination: `partition_by` must name a column".into());
        }
        Ok(())
    }
}

/// JSON Schema for the destination config document (round-trip tested).
pub fn dest_config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(FileDestConfig)).expect("schema serializes")
}

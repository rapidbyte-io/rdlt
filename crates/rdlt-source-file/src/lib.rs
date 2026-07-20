//! # rdlt-source-file — bundled file source
//!
//! JSONL and Parquet files by explicit path or glob, with per-file incremental
//! cursors (completed files skipped, in-progress files resumed at their offset,
//! shrunk/rewritten files rejected loudly). Parquet streams are STRUCTURED (contract
//! clause S7): batches push through the Arrow passthrough path with run-level
//! provenance only. Depends on the SPI only.

pub mod config;
pub mod cursor;
mod jsonl;
mod parquet;

use async_trait::async_trait;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

pub use config::{FileConfig, FileStream, Format};
use cursor::{FileCursor, FileMeta};

#[derive(Debug)]
pub struct FileSource {
    config: FileConfig,
}

impl FileSource {
    pub fn from_yaml(yaml: &str) -> Result<Self, config::ConfigError> {
        Ok(Self::new(FileConfig::from_yaml(yaml)?))
    }

    pub fn from_json(json: &str) -> Result<Self, config::ConfigError> {
        Ok(Self::new(FileConfig::from_json(json)?))
    }

    /// Embedder entry point (see [`FileConfig::from_value`]).
    pub fn from_value(value: serde_json::Value) -> Result<Self, config::ConfigError> {
        Ok(Self::new(FileConfig::from_value(value)?))
    }

    pub fn new(config: FileConfig) -> Self {
        Self { config }
    }

    fn stream_config(&self, name: &str) -> Option<&FileStream> {
        self.config.streams.iter().find(|s| s.name == name)
    }
}

/// Resolve a path-or-glob into a lexicographically sorted file snapshot.
/// Empty glob ⇒ empty list (success); explicitly named missing file ⇒ error.
/// A path naming an EXISTING file is always taken literally, even when it contains
/// glob metacharacters (`events[prod].jsonl` is a file, not a character class).
/// Glob-expansion errors (e.g. an unreadable directory) are fatal — a partial match
/// list must never pass as a complete one.
fn resolve_files(pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
    let mut matched: Vec<String> = if std::path::Path::new(pattern).is_file() {
        vec![pattern.to_owned()]
    } else if pattern.contains(['*', '?', '[']) {
        let mut paths = Vec::new();
        for entry in glob::glob(pattern)
            .map_err(|e| SourceError::fatal(format!("invalid glob `{pattern}`: {e}")))?
        {
            let path = entry.map_err(|e| {
                SourceError::fatal(format!(
                    "expanding glob `{pattern}`: {e}; refusing to load a partial file list"
                ))
            })?;
            if path.is_file() {
                paths.push(path.to_string_lossy().into_owned());
            }
        }
        paths
    } else {
        return Err(SourceError::fatal(format!(
            "file `{pattern}` does not exist"
        )));
    };
    matched.sort();
    matched
        .into_iter()
        .map(|path| {
            let meta = std::fs::metadata(&path)
                .map_err(|e| SourceError::fatal(format!("stat `{path}`: {e}")))?;
            let mtime_ms = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64);
            Ok(FileMeta {
                path,
                size: meta.len(),
                mtime_ms,
            })
        })
        .collect()
}

#[async_trait]
impl Source for FileSource {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new("file", env!("CARGO_PKG_VERSION"));
        spec.config_schema = Some(config_schema());
        spec
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(self
            .config
            .streams
            .iter()
            .map(|stream| {
                let mut spec = StreamSpec::new(stream.name.as_str());
                match stream.format {
                    Format::Jsonl => {
                        if let Some(key) = &stream.primary_key {
                            spec = spec.with_primary_key(key.iter().cloned());
                        }
                        for (column, hint) in &stream.type_hints {
                            spec = spec.with_type_hint(column.clone(), (*hint).into());
                        }
                    }
                    // Parquet streams are structured: Arrow batches, run-level
                    // provenance only (clause S7).
                    Format::Parquet => spec = spec.structured(),
                }
                spec
            })
            .collect())
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let stream = self
            .stream_config(req.stream.name.as_str())
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", req.stream.name)))?;

        let mut cursor = FileCursor::decode(req.since.as_ref())?;
        // Snapshot the file list once per run (stable list; new files next run).
        let matched = match stream.format {
            Format::Jsonl => resolve_files(&stream.path)?,
            Format::Parquet => parquet::resolve_with_row_groups(&stream.path)?,
        };
        let tasks = cursor.plan(&matched)?;

        for task in &tasks {
            let proceeded = match stream.format {
                Format::Jsonl => {
                    jsonl::read_task(task, stream.validate, &mut cursor, &mut req.out).await?
                }
                Format::Parquet => parquet::read_task(task, &mut cursor, &mut req.out).await?,
            };
            if !proceeded {
                return Ok(()); // cancellation (clause S4)
            }
        }
        Ok(())
    }
}

/// JSON Schema GENERATED from the config structs (feature 006, US4) — the
/// declared schema and the parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(config::FileConfig)).expect("schema serializes")
}

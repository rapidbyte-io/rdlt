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
use cursor::FileCursor;

#[derive(Debug)]
pub struct FileSource {
    config: FileConfig,
}

impl FileSource {
    pub fn from_yaml(yaml: &str) -> Result<Self, config::ConfigError> {
        Ok(Self::new(FileConfig::from_yaml(yaml)?))
    }

    pub fn new(config: FileConfig) -> Self {
        Self { config }
    }

    fn stream_config(&self, name: &str) -> Option<&FileStream> {
        self.config.streams.iter().find(|s| s.name == name)
    }
}

/// Resolve a path-or-glob into a lexicographically sorted `(path, size)` snapshot.
/// Empty glob ⇒ empty list (success); explicitly named missing file ⇒ error.
fn resolve_files(pattern: &str) -> Result<Vec<(String, u64)>, SourceError> {
    let is_glob = pattern.contains(['*', '?', '[']);
    let mut matched: Vec<String> = if is_glob {
        glob::glob(pattern)
            .map_err(|e| SourceError::fatal(format!("invalid glob `{pattern}`: {e}")))?
            .filter_map(Result::ok)
            .filter(|p| p.is_file())
            .map(|p| p.to_string_lossy().into_owned())
            .collect()
    } else {
        if !std::path::Path::new(pattern).is_file() {
            return Err(SourceError::fatal(format!(
                "file `{pattern}` does not exist"
            )));
        }
        vec![pattern.to_owned()]
    };
    matched.sort();
    matched
        .into_iter()
        .map(|path| {
            let size = std::fs::metadata(&path)
                .map_err(|e| SourceError::fatal(format!("stat `{path}`: {e}")))?
                .len();
            Ok((path, size))
        })
        .collect()
}

#[async_trait]
impl Source for FileSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("file", env!("CARGO_PKG_VERSION"))
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

//! The file SOURCE side: config, per-file cursors, and the read loop.
//!
//! JSONL and Parquet files by explicit path or glob, with per-file incremental
//! cursors (completed files skipped, in-progress files resumed at their offset,
//! shrunk/rewritten files rejected loudly). Parquet streams are STRUCTURED (contract
//! clause S7): batches push through the Arrow passthrough path with run-level
//! provenance only. Depends on the SPI only.

pub mod config;
pub mod cursor;

use async_trait::async_trait;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

use crate::formats::{csv, jsonl, parquet};

/// Fail-point registry (gate G2.2): the SOURCE-side crash points (015 R9;
/// the dest points live in `crate::dest::FAIL_POINTS`).
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &["file.list", "file.read"];
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
pub(crate) fn resolve_files(pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
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
                etag: None,
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
                    // Record streams: jsonl and csv alike (R4).
                    Format::Jsonl | Format::Csv => {
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
        let location = crate::location::Location::from_options(stream.location.as_ref())?;
        // Snapshot the file list once per run (stable list; new files next run).
        // Local parquet keeps its row-group listing; object-store parquet is
        // fetched to temp files first (correctness-first, research R10) with
        // the cursor still keyed by the object.
        let mut fetched_dir: Option<std::path::PathBuf> = None;
        let is_s3 = matches!(location, crate::location::Location::S3(_));
        let (matched, read_paths) = match (&location, stream.format) {
            (crate::location::Location::Local, Format::Jsonl | Format::Csv) => {
                (resolve_files(&stream.path)?, None)
            }
            (crate::location::Location::Local, Format::Parquet) => {
                (parquet::resolve_with_row_groups(&stream.path)?, None)
            }
            (crate::location::Location::S3(_), Format::Jsonl | Format::Csv) => {
                (location.list(&stream.path).await?, None)
            }
            (crate::location::Location::S3(_), Format::Parquet) => {
                let listed = location.list(&stream.path).await?;
                let dir = temp_fetch_dir(&stream.name)?;
                let mut metas = Vec::with_capacity(listed.len());
                let mut paths = std::collections::BTreeMap::new();
                for (i, meta) in listed.into_iter().enumerate() {
                    let local = fetch_to_temp(&location, &meta.path, &dir, i).await?;
                    let counted = parquet::resolve_with_row_groups(&local.to_string_lossy())?;
                    let groups = counted.first().map(|m| m.size).unwrap_or(0);
                    paths.insert(meta.path.clone(), local.to_string_lossy().into_owned());
                    metas.push(FileMeta {
                        size: groups,
                        ..meta
                    });
                }
                fetched_dir = Some(dir);
                (metas, Some(paths))
            }
        };
        rdlt_connector::core::crash_point!(
            "file.list",
            Err(SourceError::fatal("injected crash at file.list"))
        );
        // Plan per the format's incremental unit: parquet = row groups
        // (tail), csv and COMPRESSED jsonl = whole-file (R5), plain jsonl =
        // byte tails. A jsonl glob may match both plain and compressed
        // files — each follows its own rule.
        let mut tasks = match stream.format {
            Format::Parquet => cursor.plan(&matched)?,
            Format::Csv => cursor.plan_whole(&matched)?,
            Format::Jsonl => {
                let (tail, whole): (Vec<_>, Vec<_>) = matched
                    .into_iter()
                    .partition(|m| crate::formats::codec_of(&m.path).is_plain());
                let mut tasks = cursor.plan(&tail)?;
                tasks.extend(cursor.plan_whole(&whole)?);
                tasks.sort_by(|a, b| a.path.cmp(&b.path));
                tasks
            }
        };
        if let Some(paths) = &read_paths {
            for task in &mut tasks {
                task.read_path = paths.get(&task.path).cloned();
            }
        }
        // Object-store csv/compressed-jsonl read through a LOCAL decode:
        // fetch the (post-plan, non-skipped) tasks to temp files.
        if is_s3 && stream.format != Format::Parquet {
            for (i, task) in tasks.iter_mut().enumerate() {
                let needs_local = stream.format == Format::Csv
                    || !crate::formats::codec_of(&task.path).is_plain();
                if needs_local && task.read_path.is_none() {
                    let dir = match &fetched_dir {
                        Some(dir) => dir.clone(),
                        None => {
                            let dir = temp_fetch_dir(&stream.name)?;
                            fetched_dir = Some(dir.clone());
                            dir
                        }
                    };
                    let local = fetch_to_temp(&location, &task.path, &dir, i).await?;
                    task.read_path = Some(local.to_string_lossy().into_owned());
                }
            }
        }

        let csv_options = stream.csv.clone().unwrap_or_default();
        let mut outcome = Ok(());
        for task in &tasks {
            rdlt_connector::core::crash_point!(
                "file.read",
                Err(SourceError::fatal("injected crash at file.read"))
            );
            let proceeded = match stream.format {
                Format::Jsonl if crate::formats::codec_of(&task.path).is_plain() => {
                    jsonl::read_task(&location, task, stream.validate, &mut cursor, &mut req.out)
                        .await
                }
                Format::Jsonl => {
                    jsonl::read_task_whole(task, stream.validate, &mut cursor, &mut req.out).await
                }
                Format::Csv => {
                    csv::read_task(
                        task,
                        &csv_options,
                        &stream.type_hints,
                        &mut cursor,
                        &mut req.out,
                    )
                    .await
                }
                Format::Parquet => parquet::read_task(task, &mut cursor, &mut req.out).await,
            };
            match proceeded {
                Ok(true) => {}
                Ok(false) => break, // cancellation (clause S4)
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
        if let Some(dir) = fetched_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
        outcome
    }
}

/// JSON Schema GENERATED from the config structs (feature 006, US4) — the
/// declared schema and the parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(config::FileConfig)).expect("schema serializes")
}

/// Per-stream temp dir for object fetches.
fn temp_fetch_dir(stream: &str) -> Result<std::path::PathBuf, SourceError> {
    let dir = std::env::temp_dir().join(format!("rdlt-file-{}-{stream}", std::process::id()));
    std::fs::create_dir_all(&dir)
        .map_err(|e| SourceError::fatal(format!("temp dir for object fetch: {e}")))?;
    Ok(dir)
}

/// Drain one object into a temp file (bounded buffer).
async fn fetch_to_temp(
    location: &crate::location::Location,
    key: &str,
    dir: &std::path::Path,
    i: usize,
) -> Result<std::path::PathBuf, SourceError> {
    use std::io::Write;
    let mut reader = location.open_from(key, 0).await?;
    let path = dir.join(format!("obj-{i}"));
    let mut file = std::fs::File::create(&path)
        .map_err(|e| SourceError::fatal(format!("temp file for `{key}`: {e}")))?;
    let mut buf = vec![0u8; 8 << 20];
    loop {
        let n = reader
            .read_full(&mut buf)
            .await
            .map_err(|e| SourceError::fatal(format!("fetching `{key}`: {e}")))?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])
            .map_err(|e| SourceError::fatal(format!("writing temp for `{key}`: {e}")))?;
    }
    Ok(path)
}

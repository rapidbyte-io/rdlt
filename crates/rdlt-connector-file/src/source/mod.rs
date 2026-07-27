//! The file SOURCE side: config, per-file cursors, and the read loop.
//!
//! JSONL, CSV, and Parquet files by explicit path or glob, on the local filesystem or
//! S3-compatible object storage, with per-file incremental cursors (completed files
//! skipped, in-progress files resumed at their offset, shrunk/rewritten files rejected
//! loudly). Parquet streams are STRUCTURED: batches push through the Arrow passthrough
//! path with run-level provenance only. Depends on the SPI only.

pub mod config;
pub mod cursor;

use async_trait::async_trait;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

use crate::formats::{Format, csv, jsonl, parquet};

/// Fail-point registry: the SOURCE-side crash points (the dest points live in
/// `crate::dest::FAIL_POINTS`).
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &["file.list", "file.read"];
pub use config::{FileConfig, FileStream};
use cursor::{FileCursor, FileMeta, FileTask};

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
    } else if std::path::Path::new(pattern).is_dir() {
        // A directory is not "missing" — saying so sends the operator looking
        // for a path that is right there. Naming the pattern they probably
        // meant is the difference between a dead end and a fix. Expanding it
        // implicitly is deliberately NOT done: which files a stream reads is
        // the operator's declaration, not a guess.
        return Err(SourceError::fatal(format!(
            "`{pattern}` is a directory, not a file — name a file, or use a \
             pattern such as `{pattern}/*.jsonl`"
        )));
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
                size_units: meta.len(),
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
                    // provenance only.
                    Format::Parquet => spec = spec.with_structured(),
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

        let ResolvedInputs {
            matched,
            read_paths,
            mut fetched_dir,
        } = resolve_inputs(&location, stream, &cursor).await?;
        rdlt_connector::core::crash_point!(
            "file.list",
            Err(SourceError::fatal("injected crash at file.list"))
        );
        let mut tasks = plan_tasks(&cursor, stream, matched)?;
        stage_s3_fetches(&location, stream, &mut tasks, &read_paths, &mut fetched_dir).await?;

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
                Ok(false) => break, // closed channel = cancellation
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
        // `fetched_dir` releases its directory when it drops — on every exit
        // from this function, including the error and cancellation paths above.
        outcome
    }
}

/// The run's resolved file inputs: the snapshot listing, and — for object-store
/// parquet, fetched up front — the per-object local temp paths and the temp dir.
struct ResolvedInputs {
    matched: Vec<FileMeta>,
    /// object key → local temp path (object-store parquet, fetched before planning).
    read_paths: Option<std::collections::BTreeMap<String, String>>,
    /// temp dir holding object fetches, removed after the run.
    fetched_dir: Option<FetchDir>,
}

/// Snapshot the file list once per run (stable list; new files next run). Local
/// parquet keeps its row-group listing; object-store parquet is fetched to temp
/// files first (correctness over streaming) with the cursor still keyed by the
/// object.
async fn resolve_inputs(
    location: &crate::location::Location,
    stream: &FileStream,
    cursor: &FileCursor,
) -> Result<ResolvedInputs, SourceError> {
    use crate::location::Location;
    let plain = |matched| ResolvedInputs {
        matched,
        read_paths: None,
        fetched_dir: None,
    };
    match (location, stream.format) {
        (Location::Local { .. }, Format::Jsonl | Format::Csv) => {
            Ok(plain(resolve_files(&stream.path)?))
        }
        (Location::Local { .. }, Format::Parquet) => {
            Ok(plain(parquet::resolve_with_row_groups(&stream.path)?))
        }
        (Location::S3(_), Format::Jsonl | Format::Csv) => {
            Ok(plain(location.list(&stream.path).await?))
        }
        (Location::S3(_), Format::Parquet) => {
            let listed = location.list(&stream.path).await?;
            let dir = temp_fetch_dir(&stream.name)?;
            let mut metas = Vec::with_capacity(listed.len());
            let mut paths = std::collections::BTreeMap::new();
            for (i, meta) in listed.into_iter().enumerate() {
                // An object the last run finished, whose etag still matches, has
                // nothing left to read — and downloading it only to count row
                // groups and then plan zero tasks is the whole cost for none of
                // the benefit. Its unit count is recovered from the recorded
                // progress instead, which is what a fetch would have produced.
                //
                // Both etags must be PRESENT and equal. A missing etag on either
                // side proves nothing, and the point of this skip is to act only
                // where the object is provably unchanged; anything less falls
                // through to the fetch and meets the ordinary rewrite tripwires
                // in `plan` untouched.
                if let Some(size_units) = recorded_completion(cursor, &meta) {
                    SKIPPED_FETCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metas.push(FileMeta { size_units, ..meta });
                    continue;
                }
                let local = fetch_to_temp(location, &meta.path, dir.path(), i).await?;
                let counted = parquet::resolve_with_row_groups(&local.to_string_lossy())?;
                let groups = counted.first().map(|m| m.size_units).unwrap_or(0);
                paths.insert(meta.path.clone(), local.to_string_lossy().into_owned());
                metas.push(FileMeta {
                    size_units: groups,
                    ..meta
                });
            }
            Ok(ResolvedInputs {
                matched: metas,
                read_paths: Some(paths),
                fetched_dir: Some(dir),
            })
        }
    }
}



/// Fetches skipped because the recorded progress already covered the object.
///
/// An optimisation that silently stops engaging is indistinguishable from one
/// that was never written: the behaviour is identical either way, so a test can
/// only prove the skip HAPPENED by counting it. Cheap enough to leave in — one
/// relaxed increment per listed object on a path that then performs a network
/// round trip, or doesn't.
static SKIPPED_FETCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Test-only view of [`SKIPPED_FETCHES`], for asserting that a second run over
/// unchanged objects downloaded nothing.
#[doc(hidden)]
pub fn skipped_fetches() -> u64 {
    SKIPPED_FETCHES.load(std::sync::atomic::Ordering::Relaxed)
}

/// The unit count to use for an object that needs no fetch, or `None` if it
/// must be downloaded.
///
/// Downloading an object the last run finished, only to count its row groups
/// and then plan zero tasks, is the entire cost of the fetch for none of its
/// benefit. When the object is provably unchanged the count is already recorded,
/// and it is exactly what the fetch would have produced.
///
/// **Provably** is the load-bearing word, and it is why this is conservative in
/// three separate ways:
///
/// - Both etags must be PRESENT and equal. A missing etag on either side proves
///   nothing about the content, so it is not treated as agreement.
/// - The recorded progress must be COMPLETE. A partially-read object still has
///   row groups to deliver, and skipping it would silently drop them.
/// - Anything that fails these falls through to the fetch and meets the
///   ordinary shrink and rewrite-in-place tripwires in `FileCursor::plan`
///   untouched. This function can cause an object to be fetched needlessly; it
///   can never cause one to be trusted that `plan` would have rejected.
fn recorded_completion(cursor: &FileCursor, meta: &FileMeta) -> Option<u64> {
    let progress = cursor.files.get(&meta.path)?;
    if progress.done_units != progress.size_units {
        return None;
    }
    let (recorded, listed) = (progress.etag.as_deref()?, meta.etag.as_deref()?);
    (recorded == listed).then_some(progress.size_units)
}

/// Plan per the format's incremental unit: parquet = row groups (tail), csv and
/// COMPRESSED jsonl = whole-file (R5), plain jsonl = byte tails. A jsonl glob may
/// match both plain and compressed files — each follows its own rule.
fn plan_tasks(
    cursor: &FileCursor,
    stream: &FileStream,
    matched: Vec<FileMeta>,
) -> Result<Vec<FileTask>, SourceError> {
    match stream.format {
        Format::Parquet => cursor.plan(&matched),
        Format::Csv => cursor.plan_whole(&matched),
        Format::Jsonl => {
            let (tail, whole): (Vec<_>, Vec<_>) = matched
                .into_iter()
                .partition(|m| crate::formats::codec_of(&m.path).is_plain());
            let mut tasks = cursor.plan(&tail)?;
            tasks.extend(cursor.plan_whole(&whole)?);
            tasks.sort_by(|a, b| a.path.cmp(&b.path));
            Ok(tasks)
        }
    }
}

/// Wire up local read paths for the planned tasks: object-store parquet uses the
/// up-front fetches; object-store csv/compressed-jsonl fetch the (post-plan,
/// non-skipped) tasks to temp files here so they read through a LOCAL decode.
async fn stage_s3_fetches(
    location: &crate::location::Location,
    stream: &FileStream,
    tasks: &mut [FileTask],
    read_paths: &Option<std::collections::BTreeMap<String, String>>,
    fetched_dir: &mut Option<FetchDir>,
) -> Result<(), SourceError> {
    if let Some(paths) = read_paths {
        for task in tasks.iter_mut() {
            task.read_path = paths.get(&task.path).cloned();
        }
    }
    let is_s3 = matches!(location, crate::location::Location::S3(_));
    if is_s3 && stream.format != Format::Parquet {
        for (i, task) in tasks.iter_mut().enumerate() {
            let needs_local =
                stream.format == Format::Csv || !crate::formats::codec_of(&task.path).is_plain();
            if needs_local && task.read_path.is_none() {
                // Created on first need and owned by the caller, so one
                // directory serves every fetch and is released exactly once.
                if fetched_dir.is_none() {
                    *fetched_dir = Some(temp_fetch_dir(&stream.name)?);
                }
                let dir = fetched_dir.as_ref().expect("created above");
                let local = fetch_to_temp(location, &task.path, dir.path(), i).await?;
                task.read_path = Some(local.to_string_lossy().into_owned());
            }
        }
    }
    Ok(())
}

/// JSON Schema GENERATED from the config structs — the declared schema and the
/// parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(config::FileConfig)).expect("schema serializes")
}

/// Per-READ temp dir for object fetches: unique per call (pid + a
/// process-wide counter), so concurrent in-process pipelines with
/// same-named streams can never share or clobber fetch files.
fn temp_fetch_dir(stream: &str) -> Result<FetchDir, SourceError> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("rdlt-file-{}-{seq}-{stream}", std::process::id()));
    std::fs::create_dir_all(&path)
        .map_err(|e| SourceError::fatal(format!("temp dir for object fetch: {e}")))?;
    Ok(FetchDir { path })
}

/// A fetch directory that removes itself.
///
/// Ownership, not a cleanup call: the read loop has several failure exits and a
/// cancellation exit, and a directory released only on the success path leaks
/// fetched objects — potentially a whole listing's worth — on every one of them.
#[derive(Debug)]
pub(crate) struct FetchDir {
    path: std::path::PathBuf,
}

impl FetchDir {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for FetchDir {
    fn drop(&mut self) {
        // Best effort by construction: a failure here cannot be reported (Drop
        // has no channel for it) and must not mask the outcome that unwound us.
        let _ = std::fs::remove_dir_all(&self.path);
    }
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
    let mut buf = vec![0u8; crate::formats::SLAB_BYTES];
    loop {
        let n = reader
            .read_full(&mut buf)
            .await
            .map_err(|e| crate::location::classify_read_error(&format!("fetching `{key}`"), e))?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])
            .map_err(|e| SourceError::fatal(format!("writing temp for `{key}`: {e}")))?;
    }
    Ok(path)
}

#[cfg(test)]
mod temp_dir_tests {
    #[test]
    fn temp_fetch_dirs_are_unique_per_call() {
        let a = super::temp_fetch_dir("events").unwrap();
        let b = super::temp_fetch_dir("events").unwrap();
        assert_ne!(
            a.path(),
            b.path(),
            "concurrent pipelines must never share fetch dirs"
        );
    }

    /// The directory releases itself, so every exit from the read loop — success,
    /// typed failure, cancellation — leaves no fetched objects behind.
    #[test]
    fn a_fetch_dir_removes_itself_when_dropped() {
        let path = {
            let dir = super::temp_fetch_dir("events").unwrap();
            let path = dir.path().to_path_buf();
            assert!(path.exists());
            path
        };
        assert!(!path.exists(), "the directory is released on drop");
    }
}

#[cfg(test)]
mod skip_fetch_tests {
    //! When an already-downloaded object may be left un-downloaded.
    //!
    //! Container-free by design: the risk in the skip is the DECISION, not the
    //! transfer, and a decision is testable without a bucket. The live S3 legs
    //! cover the end-to-end path.
    use super::*;
    use crate::location::types::FileProgress;

    fn meta(path: &str, etag: Option<&str>) -> FileMeta {
        FileMeta {
            path: path.to_owned(),
            size_units: 4,
            mtime_ms: None,
            etag: etag.map(str::to_owned),
        }
    }

    fn cursor_with(path: &str, done: u64, size: u64, etag: Option<&str>) -> FileCursor {
        let mut cursor = FileCursor::default();
        cursor.files.insert(
            path.to_owned(),
            FileProgress {
                done_units: done,
                size_units: size,
                ended_at_record_boundary: true,
                mtime_ms: None,
                etag: etag.map(str::to_owned),
                tail_hash: None,
                row_groups_hash: None,
            },
        );
        cursor
    }

    #[test]
    fn a_finished_object_with_a_matching_etag_needs_no_fetch() {
        let cursor = cursor_with("pq/a.parquet", 4, 4, Some("abc"));
        assert_eq!(
            recorded_completion(&cursor, &meta("pq/a.parquet", Some("abc"))),
            Some(4),
            "the recorded unit count is what the fetch would have produced"
        );
    }

    #[test]
    fn a_partially_read_object_is_still_fetched() {
        // The remaining row groups are real data; skipping here would drop them.
        let cursor = cursor_with("pq/a.parquet", 2, 4, Some("abc"));
        assert_eq!(
            recorded_completion(&cursor, &meta("pq/a.parquet", Some("abc"))),
            None
        );
    }

    #[test]
    fn a_changed_etag_is_fetched_so_the_rewrite_tripwire_still_fires() {
        // The safety-critical case: the skip must never be the reason a
        // rewritten object escapes `plan`'s rewrite detection.
        let cursor = cursor_with("pq/a.parquet", 4, 4, Some("abc"));
        assert_eq!(
            recorded_completion(&cursor, &meta("pq/a.parquet", Some("zzz"))),
            None
        );
    }

    #[test]
    fn an_absent_etag_on_either_side_proves_nothing_and_is_fetched() {
        let recorded_only = cursor_with("pq/a.parquet", 4, 4, Some("abc"));
        assert_eq!(
            recorded_completion(&recorded_only, &meta("pq/a.parquet", None)),
            None,
            "the listing offered no etag to compare against"
        );
        let listed_only = cursor_with("pq/a.parquet", 4, 4, None);
        assert_eq!(
            recorded_completion(&listed_only, &meta("pq/a.parquet", Some("abc"))),
            None,
            "the recorded progress carries no etag to compare"
        );
    }

    #[test]
    fn an_object_with_no_recorded_progress_is_fetched() {
        let cursor = cursor_with("pq/other.parquet", 4, 4, Some("abc"));
        assert_eq!(
            recorded_completion(&cursor, &meta("pq/a.parquet", Some("abc"))),
            None
        );
    }
}

//! The framework face: [`File`] implements the sdk's `SourceConnector`,
//! and `read_stream` runs the whole choreography — snapshot listing,
//! planning against the cursor, object staging, and the per-format
//! readers.

use std::collections::BTreeMap;

use async_trait::async_trait;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::core::{Cursor, StreamName, crash_point};
use rdlt_connector_sdk::spi::{SourceError, StreamSpec};

use super::config::{self, Config, Stream};
use super::cursor::{FileCursor, FileMeta, FileTask};
use super::{list, read};
use crate::format::{Format, codec_of};
use crate::location::Location;

/// The file source: one validated document, streams over files.
#[derive(Debug, Clone)]
pub struct File {
    config: Config,
}

impl File {
    fn stream_config(&self, name: &StreamName) -> Option<&Stream> {
        self.config.streams.iter().find(|s| s.name == name.as_str())
    }
}

#[async_trait]
impl SourceConnector for File {
    const NAME: &'static str = "file";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;

    fn assemble(config: Config) -> Result<Self, config::ConfigError> {
        Ok(Self { config })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::config_schema())
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(self
            .config
            .streams
            .iter()
            .map(|stream| {
                let mut spec = StreamSpec::new(stream.name.as_str());
                match stream.format {
                    // Record streams — jsonl and csv alike.
                    Format::Jsonl | Format::Csv => {
                        if let Some(key) = &stream.primary_key {
                            spec = spec.with_primary_key(key.iter().cloned());
                        }
                        for (column, hint) in &stream.type_hints {
                            spec = spec.with_type_hint(column.clone(), (*hint).into());
                        }
                    }
                    // Parquet is structured: Arrow batches, run-level
                    // provenance only.
                    Format::Parquet => spec = spec.with_structured(),
                }
                spec
            })
            .collect())
    }

    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        let stream = self
            .stream_config(&stream.name)
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", stream.name)))?;

        let mut cursor = FileCursor::decode(since.as_ref())?;
        let location = Location::from_options(stream.location.as_ref())?;

        let ResolvedInputs {
            matched,
            read_paths,
            mut fetched_dir,
        } = resolve_inputs(&location, stream, &cursor).await?;
        crash_point!(
            "file.list",
            Err(SourceError::fatal("injected crash at file.list"))
        );
        let mut tasks = plan_tasks(&cursor, stream, matched)?;
        stage_fetches(&location, stream, &mut tasks, &read_paths, &mut fetched_dir).await?;

        let csv_options = stream.csv.clone().unwrap_or_default();
        for task in &tasks {
            crash_point!(
                "file.read",
                Err(SourceError::fatal("injected crash at file.read"))
            );
            let proceeded = match stream.format {
                Format::Jsonl if codec_of(&task.path).is_plain() => {
                    read::jsonl::read_task(&location, task, stream.validate, &mut cursor, feed)
                        .await
                }
                Format::Jsonl => {
                    read::jsonl::read_task_whole(task, stream.validate, &mut cursor, feed).await
                }
                Format::Csv => {
                    read::csv::read_task(task, &csv_options, &stream.type_hints, &mut cursor, feed)
                        .await
                }
                Format::Parquet => read::parquet::read_task(task, &mut cursor, feed).await,
            };
            match proceeded {
                Ok(true) => {}
                Ok(false) => break, // the host hung up — return promptly
                Err(e) => return Err(e),
            }
        }
        // `fetched_dir` drops here — its directory is released on EVERY
        // exit, including the error and cancellation paths.
        Ok(())
    }
}

/// The run's resolved inputs: the snapshot listing, and — for
/// object-store parquet, fetched up front — per-object temp paths.
struct ResolvedInputs {
    matched: Vec<FileMeta>,
    read_paths: Option<BTreeMap<String, String>>,
    fetched_dir: Option<FetchDir>,
}

/// Snapshot once per run. Local parquet lists row groups; object-store
/// parquet fetches to temp files FIRST (correctness over streaming),
/// the cursor still keyed by the object — unless the object is
/// PROVABLY unchanged and complete, where the recorded count stands in
/// for the fetch it would have reproduced.
async fn resolve_inputs(
    location: &Location,
    stream: &Stream,
    cursor: &FileCursor,
) -> Result<ResolvedInputs, SourceError> {
    let plain = |matched| ResolvedInputs {
        matched,
        read_paths: None,
        fetched_dir: None,
    };
    match (location, stream.format) {
        (Location::Local, Format::Jsonl | Format::Csv) => {
            Ok(plain(list::local_listing(&stream.path)?))
        }
        (Location::Local, Format::Parquet) => Ok(plain(
            read::parquet::local_listing_with_row_groups(&stream.path)?,
        )),
        (Location::S3(_), Format::Jsonl | Format::Csv) => {
            Ok(plain(location.list(&stream.path).await?))
        }
        (Location::S3(_), Format::Parquet) => {
            let listed = location.list(&stream.path).await?;
            let dir = temp_fetch_dir(&stream.name)?;
            let mut metas = Vec::with_capacity(listed.len());
            let mut paths = BTreeMap::new();
            for (i, meta) in listed.into_iter().enumerate() {
                if let Some(size_units) = recorded_completion(cursor, &meta) {
                    SKIPPED_FETCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metas.push(FileMeta { size_units, ..meta });
                    continue;
                }
                let local = fetch_to_temp(location, &meta.path, dir.path(), i).await?;
                let counted =
                    read::parquet::local_listing_with_row_groups(&local.to_string_lossy())?;
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

/// Fetches skipped because recorded progress already covered the
/// object. An optimisation that silently stops engaging is
/// indistinguishable from one never written — a test can only prove
/// the skip HAPPENED by counting it.
static SKIPPED_FETCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Test-only view of the skip counter.
#[doc(hidden)]
pub fn skipped_fetches() -> u64 {
    SKIPPED_FETCHES.load(std::sync::atomic::Ordering::Relaxed)
}

/// The unit count for an object needing no fetch, or None. PROVABLY is
/// the load-bearing word — conservative three ways: both etags must be
/// present and equal; the recorded progress must be COMPLETE; and
/// anything else falls through to the fetch and the ordinary
/// tripwires. This can fetch needlessly; it can never trust what
/// `plan` would have rejected.
fn recorded_completion(cursor: &FileCursor, meta: &FileMeta) -> Option<u64> {
    let progress = cursor.files.get(&meta.path)?;
    if progress.done_units != progress.size_units {
        return None;
    }
    let (recorded, listed) = (progress.etag.as_deref()?, meta.etag.as_deref()?);
    (recorded == listed).then_some(progress.size_units)
}

/// Plan per the format's incremental unit. A jsonl glob may match both
/// plain and compressed files — each follows its own rule, and the
/// tasks re-sort by path.
fn plan_tasks(
    cursor: &FileCursor,
    stream: &Stream,
    matched: Vec<FileMeta>,
) -> Result<Vec<FileTask>, SourceError> {
    match stream.format {
        Format::Parquet => cursor.plan(&matched),
        Format::Csv => cursor.plan_whole(&matched),
        Format::Jsonl => {
            let (tail, whole): (Vec<_>, Vec<_>) = matched
                .into_iter()
                .partition(|m| codec_of(&m.path).is_plain());
            let mut tasks = cursor.plan(&tail)?;
            tasks.extend(cursor.plan_whole(&whole)?);
            tasks.sort_by(|a, b| a.path.cmp(&b.path));
            Ok(tasks)
        }
    }
}

/// Wire local read paths: object-store parquet uses the up-front
/// fetches; object-store csv and compressed jsonl fetch their planned
/// (non-skipped) tasks here, so they read through a LOCAL decode.
async fn stage_fetches(
    location: &Location,
    stream: &Stream,
    tasks: &mut [FileTask],
    read_paths: &Option<BTreeMap<String, String>>,
    fetched_dir: &mut Option<FetchDir>,
) -> Result<(), SourceError> {
    if let Some(paths) = read_paths {
        for task in tasks.iter_mut() {
            task.read_path = paths.get(&task.path).cloned();
        }
    }
    if matches!(location, Location::S3(_)) && stream.format != Format::Parquet {
        for (i, task) in tasks.iter_mut().enumerate() {
            let needs_local = stream.format == Format::Csv || !codec_of(&task.path).is_plain();
            if needs_local && task.read_path.is_none() {
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

/// A per-read temp dir, unique per call (pid + process counter) so
/// concurrent in-process pipelines never share fetch files.
fn temp_fetch_dir(stream: &str) -> Result<FetchDir, SourceError> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("rdlt-file-{}-{seq}-{stream}", std::process::id()));
    std::fs::create_dir_all(&path)
        .map_err(|e| SourceError::fatal(format!("temp dir for object fetch: {e}")))?;
    Ok(FetchDir { path })
}

/// A fetch directory that removes itself — ownership, not a cleanup
/// call: the read loop has failure and cancellation exits, and a
/// directory released only on success leaks a listing's worth of
/// fetches on every other one.
#[derive(Debug)]
struct FetchDir {
    path: std::path::PathBuf,
}

impl FetchDir {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for FetchDir {
    fn drop(&mut self) {
        // Best effort by construction: Drop has no error channel, and
        // a failure here must not mask what unwound us.
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Drain one object into a temp file through a bounded buffer.
async fn fetch_to_temp(
    location: &Location,
    key: &str,
    dir: &std::path::Path,
    i: usize,
) -> Result<std::path::PathBuf, SourceError> {
    use std::io::Write as _;
    let mut reader = location.open_from(key, 0).await?;
    let path = dir.join(format!("obj-{i}"));
    let mut file = std::fs::File::create(&path)
        .map_err(|e| SourceError::fatal(format!("temp file for `{key}`: {e}")))?;
    let mut buf = vec![0u8; crate::format::SLAB_BYTES];
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
mod tests {
    use super::*;
    use crate::source::cursor::FileProgress;

    fn meta(path: &str, etag: Option<&str>) -> FileMeta {
        FileMeta {
            path: path.into(),
            size_units: 3,
            mtime_ms: None,
            etag: etag.map(str::to_owned),
        }
    }

    fn cursor_with(path: &str, done: u64, size: u64, etag: Option<&str>) -> FileCursor {
        let mut cursor = FileCursor::default();
        cursor.record(
            path,
            FileProgress {
                done_units: done,
                size_units: size,
                ended_at_record_boundary: true,
                mtime_ms: None,
                etag: etag.map(str::to_owned),
                tail_hash: None,
                row_groups_hash: Some("g".into()),
            },
        );
        cursor
    }

    /// The skip is conservative three ways: complete + both etags
    /// present + equal, or it fetches.
    #[test]
    fn the_skip_fetch_rule_is_conservative_three_ways() {
        let m = meta("k", Some("e1"));
        assert_eq!(
            recorded_completion(&cursor_with("k", 3, 3, Some("e1")), &m),
            Some(3),
            "provably unchanged and complete"
        );
        assert!(recorded_completion(&cursor_with("k", 2, 3, Some("e1")), &m).is_none());
        assert!(recorded_completion(&cursor_with("k", 3, 3, None), &m).is_none());
        assert!(recorded_completion(&cursor_with("k", 3, 3, Some("e2")), &m).is_none());
        assert!(
            recorded_completion(&cursor_with("k", 3, 3, Some("e1")), &meta("k", None)).is_none()
        );
        assert!(recorded_completion(&FileCursor::default(), &m).is_none());
    }

    /// Temp fetch dirs are unique per call and remove themselves.
    #[test]
    fn fetch_dirs_are_unique_and_self_removing() {
        let a = temp_fetch_dir("events").expect("a");
        let b = temp_fetch_dir("events").expect("b");
        assert_ne!(a.path(), b.path());
        let path = a.path().to_path_buf();
        assert!(path.exists());
        drop(a);
        assert!(!path.exists(), "released on drop");
        drop(b);
    }
}

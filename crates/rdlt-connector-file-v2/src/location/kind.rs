//! The unified Location: one enum over the local filesystem and S3,
//! serving the source's list/open primitives and the destination's
//! staging/publish/document protocol.

use std::path::{Path, PathBuf};

use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::{DestinationError, SourceError};
use serde::Serialize;

use super::options::LocationOptions;
use super::s3::S3Location;
use crate::destination::STAGING_DIR;
use crate::source::cursor::FileMeta;
use crate::source::list::local_listing;

fn fatal(e: impl std::fmt::Display) -> DestinationError {
    DestinationError::fatal(e.to_string())
}

/// A connected location. `root` is the output directory (write half)
/// or empty (read half, which names whole paths).
#[derive(Debug, Clone)]
pub(crate) enum Location {
    Local { root: PathBuf },
    S3(S3Location),
}

impl Location {
    /// The read-half constructor: full paths/keys, no root.
    pub(crate) fn from_options(options: Option<&LocationOptions>) -> Result<Self, SourceError> {
        match options.and_then(|o| o.s3.as_ref()) {
            None => Ok(Self::Local {
                root: PathBuf::new(),
            }),
            Some(s3) => Ok(Self::S3(S3Location::connect(s3)?)),
        }
    }

    /// The write-half constructor: `path` is the output directory
    /// (local, created) or the key prefix (S3).
    pub(crate) fn for_dest(
        path: &str,
        options: Option<&LocationOptions>,
    ) -> Result<Self, DestinationError> {
        match options.and_then(|o| o.s3.as_ref()) {
            None => Self::local_dir(PathBuf::from(path)),
            Some(s3) => Ok(Self::S3(S3Location::connect_for_dest(
                s3,
                path.trim_matches('/').to_owned(),
            )?)),
        }
    }

    /// A LOCAL output directory, created if needed — the plain-path
    /// form; the PathBuf is used AS-IS (non-UTF-8 paths keep their
    /// bytes).
    pub(crate) fn local_dir(root: PathBuf) -> Result<Self, DestinationError> {
        std::fs::create_dir_all(&root).map_err(fatal)?;
        Ok(Self::Local { root })
    }

    /// Is this the local filesystem? The local-only durability steps
    /// (and their crash points) key off this.
    pub(crate) fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// The output root, for the local-only synchronous row counter.
    pub(crate) fn local_root(&self) -> Option<&Path> {
        match self {
            Self::Local { root } => Some(root),
            Self::S3(_) => None,
        }
    }

    /// The complete-or-fail listing, either kind (read half).
    pub(crate) async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        match self {
            Self::Local { .. } => local_listing(pattern),
            Self::S3(s3) => s3.list(pattern).await,
        }
    }

    /// A sequential byte reader positioned at `start` (read half).
    pub(crate) async fn open_from(
        &self,
        name: &str,
        start: u64,
    ) -> Result<ByteReader, SourceError> {
        match self {
            Self::Local { .. } => {
                use std::io::{Seek as _, SeekFrom};
                let mut file = std::fs::File::open(name)
                    .map_err(|e| SourceError::fatal(format!("opening `{name}`: {e}")))?;
                if start > 0 {
                    file.seek(SeekFrom::Start(start))
                        .map_err(|e| SourceError::fatal(format!("seek `{name}`: {e}")))?;
                }
                Ok(ByteReader::Local(file))
            }
            Self::S3(s3) => s3.open_from(name, start).await,
        }
    }

    /// Reclaim THIS scope's dead-session staging (a crashed session's
    /// staged-but-uncommitted data must never survive into a fresh
    /// load), then ready the fresh load's area. Scoped — a sibling
    /// pipeline sharing the output keeps its live staged data.
    pub(crate) async fn prepare_staging(
        &self,
        scope: &str,
        load: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => {
                let scope_root = root.join(STAGING_DIR).join(scope);
                if scope_root.exists() {
                    std::fs::remove_dir_all(&scope_root).map_err(fatal)?;
                }
                std::fs::create_dir_all(scope_root.join(load)).map_err(fatal)?;
                Ok(())
            }
            Self::S3(s3) => {
                for key in s3.list_keys(&format!("{STAGING_DIR}/{scope}")).await? {
                    s3.delete_key(&key).await?;
                }
                Ok(())
            }
        }
    }

    /// Write one staged part (the S3 staging crash point fires here).
    pub(crate) async fn stage_put(
        &self,
        staging_tail: &str,
        bytes: Vec<u8>,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => std::fs::write(root.join(staging_tail), &bytes).map_err(fatal),
            Self::S3(s3) => {
                crash_point!(
                    "file.stage.put",
                    Err(DestinationError::fatal("injected crash at file.stage.put"))
                );
                s3.put(staging_tail, bytes).await
            }
        }
    }

    /// Best-effort staged-part removal (a replayed commit discards its
    /// staging before returning the prior receipt).
    pub(crate) async fn stage_remove(&self, staging_tail: &str) {
        match self {
            Self::Local { root } => {
                let _ = std::fs::remove_file(root.join(staging_tail));
            }
            Self::S3(s3) => s3.delete_best_effort(staging_tail).await,
        }
    }

    /// Publish one staged part to its deterministic final name — the
    /// atomic visibility step. Local: fsync then rename (per-file
    /// atomic). S3: COPY then idempotent DELETE (a replayed finalize
    /// tolerates a missing staged object). The crash points fire
    /// between the sub-steps of their own arms.
    pub(crate) async fn publish_part(
        &self,
        staging_tail: &str,
        final_tail: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => {
                let from = root.join(staging_tail);
                let to = root.join(final_tail);
                if let Some(parent) = to.parent() {
                    std::fs::create_dir_all(parent).map_err(fatal)?;
                }
                crash_point!(
                    "pq.staged.sync",
                    Err(DestinationError::fatal("injected crash at pq.staged.sync"))
                );
                let file = std::fs::File::open(&from).map_err(fatal)?;
                file.sync_all().map_err(fatal)?;
                crash_point!(
                    "pq.part.rename",
                    Err(DestinationError::fatal("injected crash at pq.part.rename"))
                );
                std::fs::rename(&from, &to).map_err(fatal)
            }
            Self::S3(s3) => {
                crash_point!(
                    "file.finalize.copy",
                    Err(DestinationError::fatal(
                        "injected crash at file.finalize.copy"
                    ))
                );
                s3.copy(staging_tail, final_tail).await?;
                crash_point!(
                    "file.finalize.delete",
                    Err(DestinationError::fatal(
                        "injected crash at file.finalize.delete"
                    ))
                );
                s3.delete_idempotent(staging_tail).await
            }
        }
    }

    /// Fsync one directory so a rename inside it survives power loss.
    /// No-op on object stores.
    pub(crate) fn sync_dir(&self, dir_tail: &str) -> Result<(), DestinationError> {
        if let Self::Local { root } = self {
            fsync_dir(&root.join(dir_tail))?;
        }
        Ok(())
    }

    /// Read one durable document's verbatim bytes; None when absent.
    pub(crate) async fn read_doc(&self, name: &str) -> Result<Option<Vec<u8>>, DestinationError> {
        match self {
            Self::Local { root } => read_raw(&root.join(name)),
            Self::S3(s3) => s3.read_doc(name).await,
        }
    }

    /// Write one durable document. Local: atomic temp file, fsync,
    /// rename, parent-dir fsync — metadata must not be LESS durable
    /// than the parts it describes. S3: a single PUT.
    pub(crate) async fn write_doc<T: Serialize>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => write_json_atomic(&root.join(name), value),
            Self::S3(s3) => {
                let bytes = serde_json::to_vec_pretty(value).map_err(fatal)?;
                s3.put(name, bytes).await
            }
        }
    }

    /// THE ownership listing: every file under `{table}/`, as tails
    /// relative to the table root. Row counting and Replace truncation
    /// both consume it, so the scope rule lives exactly once.
    pub(crate) async fn keys_of_table(&self, table: &str) -> Result<Vec<String>, DestinationError> {
        match self {
            Self::Local { root } => walk_local_files(&root.join(table)),
            Self::S3(s3) => {
                let root = format!("{}/", s3.key_of_table_root(table));
                let mut tails = Vec::new();
                for key in s3.list_keys(table).await? {
                    let name = key.to_string();
                    if let Some(tail) = tail_of_key(&name, &root)? {
                        tails.push(tail.to_owned());
                    }
                }
                Ok(tails)
            }
        }
    }

    /// Read one owned file by its tail (row counting).
    pub(crate) async fn read_table_file(
        &self,
        table: &str,
        tail: &str,
    ) -> Result<Vec<u8>, DestinationError> {
        match self {
            Self::Local { root } => std::fs::read(root.join(table).join(tail)).map_err(fatal),
            Self::S3(s3) => s3.get_key(&s3.key_of_table(table, tail)).await,
        }
    }

    /// Delete one owned file by its tail (Replace truncation).
    pub(crate) async fn delete_table_file(
        &self,
        table: &str,
        tail: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => {
                std::fs::remove_file(root.join(table).join(tail)).map_err(fatal)
            }
            Self::S3(s3) => s3.delete_key(&s3.key_of_table(table, tail)).await,
        }
    }
}

/// Recursively list every FILE under `base` as slash-joined tails. A
/// missing base is an empty listing (no data yet). Symlinks are
/// SKIPPED — never staged by us, and descending one would let
/// truncation unlink outside the root. A non-UTF-8 name is a TYPED
/// error: naming it empty would report children at the wrong depth
/// and hand truncation a path it never listed.
fn walk_local_files(base: &Path) -> Result<Vec<String>, DestinationError> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) -> Result<(), DestinationError> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(fatal(e)),
        };
        for entry in entries {
            let entry = entry.map_err(fatal)?;
            let path = entry.path();
            let kind = entry.file_type().map_err(fatal)?;
            if kind.is_symlink() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                return Err(fatal(format!(
                    "output directory contains a non-UTF-8 name under `{}`; rename it or move \
                     it outside the destination",
                    dir.display()
                )));
            };
            let tail = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            if kind.is_dir() {
                walk(&path, &tail, out)?;
            } else {
                out.push(tail);
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(base, "", &mut out)?;
    Ok(out)
}

/// The part of a listed key below the table root. The root is
/// STRIPPED, never searched — a partition value can spell the table's
/// own name. A key outside the prefix is a TYPED listing violation
/// (skipping would under-report ownership and Replace would leave data
/// behind); the ONE non-violation is the zero-byte directory MARKER
/// whose key IS the table root — carried by consoles and `put-object`
/// on folder paths, holding no data, skipped.
fn tail_of_key<'k>(key: &'k str, root: &str) -> Result<Option<&'k str>, DestinationError> {
    if key == root.trim_end_matches('/') {
        return Ok(None);
    }
    match key.strip_prefix(root) {
        Some(tail) => Ok(Some(tail)),
        None => Err(fatal(format!(
            "listing returned key `{key}`, which is not under the prefix `{root}` it was \
             listed by"
        ))),
    }
}

fn fsync_dir(path: &Path) -> Result<(), DestinationError> {
    std::fs::File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(fatal)
}

/// Atomic durable JSON rewrite: temp + fsync + rename + parent fsync.
fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), DestinationError> {
    use std::io::Write as _;
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(value).map_err(fatal)?;
    let mut file = std::fs::File::create(&tmp).map_err(fatal)?;
    file.write_all(&bytes).map_err(fatal)?;
    file.sync_all().map_err(fatal)?;
    drop(file);
    std::fs::rename(&tmp, path).map_err(fatal)?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }
    Ok(())
}

fn read_raw(path: &Path) -> Result<Option<Vec<u8>>, DestinationError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(fatal(e)),
    }
}

/// A sequential reader over either kind.
pub(crate) enum ByteReader {
    Local(std::fs::File),
    S3(super::s3::S3Reader),
}

impl std::fmt::Debug for ByteReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(_) => f.write_str("ByteReader::Local"),
            Self::S3(reader) => reader.fmt(f),
        }
    }
}

impl ByteReader {
    /// Fill `buf` as far as possible — short only at end-of-stream.
    pub(crate) async fn read_full(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Local(file) => {
                use std::io::Read as _;
                let mut filled = 0;
                while filled < buf.len() {
                    match file.read(&mut buf[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }
                Ok(filled)
            }
            Self::S3(reader) => reader.read_full(buf).await,
        }
    }
}

/// Classify a `read_full` failure: the retryable kind rides the engine
/// budget; everything else is fatal, subject named.
pub(crate) fn classify_read_error(context: &str, e: std::io::Error) -> SourceError {
    if e.kind() == std::io::ErrorKind::ConnectionReset {
        SourceError::transient(format!("{context}: {e}"))
    } else {
        SourceError::fatal(format!("{context}: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The walk: files at every depth as tails, symlinks skipped, a
    /// missing base empty.
    #[test]
    fn the_walk_lists_files_skips_symlinks_and_tolerates_absence() {
        let dir = tempfile::tempdir().expect("dir");
        std::fs::create_dir_all(dir.path().join("t/us")).expect("dirs");
        std::fs::write(dir.path().join("t/a.parquet"), b"x").expect("write");
        std::fs::write(dir.path().join("t/us/b.parquet"), b"x").expect("write");
        #[cfg(unix)]
        std::os::unix::fs::symlink("/etc", dir.path().join("t/link")).expect("symlink");
        let mut tails = walk_local_files(&dir.path().join("t")).expect("walk");
        tails.sort();
        assert_eq!(tails, vec!["a.parquet", "us/b.parquet"]);
        assert!(
            walk_local_files(&dir.path().join("missing"))
                .expect("empty")
                .is_empty()
        );
    }

    /// tail_of_key: strip-not-search, the marker skip, and the typed
    /// violation.
    #[test]
    fn tails_are_stripped_never_searched() {
        assert_eq!(
            tail_of_key("out/t/t/part.parquet", "out/t/").expect("ok"),
            Some("t/part.parquet"),
            "a partition value may spell the table's own name"
        );
        assert_eq!(tail_of_key("out/t", "out/t/").expect("marker"), None);
        let err = tail_of_key("elsewhere/k", "out/t/").expect_err("violation");
        assert!(
            format!("{err}").contains(
                "listing returned key `elsewhere/k`, which is not under the prefix `out/t/`"
            ),
            "{err}"
        );
    }

    /// The atomic doc write round-trips and leaves no temp file.
    #[test]
    fn doc_writes_are_atomic_and_clean() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("doc.json");
        write_json_atomic(&path, &serde_json::json!({"v": 1})).expect("writes");
        let read: serde_json::Value =
            serde_json::from_slice(&read_raw(&path).expect("read").expect("present"))
                .expect("parses");
        assert_eq!(read, serde_json::json!({"v": 1}));
        assert!(!path.with_extension("json.tmp").exists(), "no temp residue");
        assert!(
            read_raw(&dir.path().join("absent.json"))
                .expect("ok")
                .is_none()
        );
    }
}

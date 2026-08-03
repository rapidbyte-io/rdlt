//! Storage dispatch. A [`Location`] is the single value through which
//! every disk-versus-bucket decision in the crate flows: the source's
//! listing and sequential reads on one side, the destination's
//! staging, atomic publish, durable documents, and table-ownership
//! sweeps on the other. Each method matches once and hands the local
//! arm to the disk helpers at the bottom of this file; the S3 arm
//! delegates to [`S3Location`].

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::{DestinationError, SourceError};
use serde::Serialize;

use super::options::LocationOptions;
use super::s3::S3Location;
use crate::destination::STAGING_DIR;
use crate::source::cursor::FileMeta;
use crate::source::list::local_listing;

/// Render any displayable cause as a fatal destination error.
fn to_fatal<E: std::fmt::Display>(cause: E) -> DestinationError {
    DestinationError::fatal(cause.to_string())
}

/// One connected storage target. The destination form carries `root`,
/// the directory everything hangs under; the source form is rootless
/// and addresses whole paths (or keys) directly.
#[derive(Debug, Clone)]
pub(crate) enum Location {
    Local { root: PathBuf },
    S3(S3Location),
}

impl Location {
    /// Source-side constructor — rootless, since streams name their
    /// files in full.
    pub(crate) fn from_options(options: Option<&LocationOptions>) -> Result<Self, SourceError> {
        let Some(s3) = options.and_then(|o| o.s3.as_ref()) else {
            return Ok(Self::Local {
                root: PathBuf::new(),
            });
        };
        Ok(Self::S3(S3Location::connect(s3)?))
    }

    /// Destination-side constructor. On disk `path` is the output
    /// directory (created here); on S3 it becomes the key prefix.
    pub(crate) fn for_dest(
        path: &str,
        options: Option<&LocationOptions>,
    ) -> Result<Self, DestinationError> {
        if let Some(s3) = options.and_then(|o| o.s3.as_ref()) {
            let prefix = path.trim_matches('/').to_owned();
            return Ok(Self::S3(S3Location::connect_for_dest(s3, prefix)?));
        }
        Self::local_dir(PathBuf::from(path))
    }

    /// A local output directory, created if absent. The `PathBuf` is
    /// taken verbatim — a non-UTF-8 path keeps its exact bytes.
    pub(crate) fn local_dir(root: PathBuf) -> Result<Self, DestinationError> {
        std::fs::create_dir_all(&root).map_err(to_fatal)?;
        Ok(Self::Local { root })
    }

    /// True on disk. The durability steps that only exist locally —
    /// and the crash points guarding them — branch on this.
    pub(crate) fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// The output directory, when there is one; the synchronous local
    /// row counter needs the concrete path.
    pub(crate) fn local_root(&self) -> Option<&Path> {
        if let Self::Local { root } = self {
            Some(root)
        } else {
            None
        }
    }

    /// List everything a pattern names — complete, or a typed error.
    pub(crate) async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        match self {
            Self::Local { .. } => local_listing(pattern),
            Self::S3(s3) => s3.list(pattern).await,
        }
    }

    /// A byte reader positioned `start` bytes in.
    pub(crate) async fn open_from(
        &self,
        name: &str,
        start: u64,
    ) -> Result<ByteReader, SourceError> {
        match self {
            Self::S3(s3) => s3.open_from(name, start).await,
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
        }
    }

    /// Clear whatever a dead session left staged under this scope —
    /// staged-but-never-committed bytes must not leak into a new load —
    /// then make room for the fresh one. The scope key keeps a sibling
    /// pipeline's live staging untouched.
    pub(crate) async fn prepare_staging(
        &self,
        scope: &str,
        load: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::S3(s3) => {
                let scope_prefix = format!("{STAGING_DIR}/{scope}");
                for stale in s3.list_keys(&scope_prefix).await? {
                    s3.delete_key(&stale).await?;
                }
                Ok(())
            }
            Self::Local { root } => {
                let scope_dir = root.join(STAGING_DIR).join(scope);
                if scope_dir.exists() {
                    std::fs::remove_dir_all(&scope_dir).map_err(to_fatal)?;
                }
                std::fs::create_dir_all(scope_dir.join(load)).map_err(to_fatal)
            }
        }
    }

    /// Write one part into staging. The S3 staging crash point sits
    /// immediately ahead of the PUT.
    pub(crate) async fn stage_put(
        &self,
        staging_tail: &str,
        bytes: Vec<u8>,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => {
                let path = root.join(staging_tail);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(to_fatal)?;
                }
                std::fs::write(path, &bytes).map_err(to_fatal)
            }
            Self::S3(s3) => {
                crash_point!(
                    "file.stage.put",
                    Err(DestinationError::fatal("injected crash at file.stage.put"))
                );
                s3.put(staging_tail, bytes).await
            }
        }
    }

    /// Drop one staged part without caring whether it succeeds — a
    /// replayed commit clears its staging before handing back the
    /// receipt it already earned.
    pub(crate) async fn stage_remove(&self, staging_tail: &str) {
        match self {
            Self::S3(s3) => s3.delete_best_effort(staging_tail).await,
            Self::Local { root } => {
                let _ = std::fs::remove_file(root.join(staging_tail));
            }
        }
    }

    /// Make one staged part visible under its deterministic final
    /// name. Disk gets fsync-then-rename (atomic per file); S3 gets
    /// COPY followed by an idempotent DELETE, so a replayed finalize
    /// that finds the staged object already gone still succeeds. Each
    /// arm's crash points separate its own sub-steps.
    pub(crate) async fn publish_part(
        &self,
        staging_tail: &str,
        final_tail: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => promote_on_disk(root, staging_tail, final_tail),
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

    /// Fsync a directory so a rename it contains survives power loss.
    /// Object stores have no such step — nothing to do there.
    pub(crate) fn sync_dir(&self, dir_tail: &str) -> Result<(), DestinationError> {
        match self {
            Self::S3(_) => Ok(()),
            Self::Local { root } => sync_directory(&root.join(dir_tail)),
        }
    }

    /// One durable document's exact bytes, or `None` if it does not
    /// exist.
    pub(crate) async fn read_doc(&self, name: &str) -> Result<Option<Vec<u8>>, DestinationError> {
        match self {
            Self::Local { root } => read_optional(&root.join(name)),
            Self::S3(s3) => s3.read_doc(name).await,
        }
    }

    /// Persist one document. Metadata may never be flimsier than the
    /// data parts it describes, so the disk arm goes through the full
    /// atomic sequence (temp file, fsync, rename, parent-dir fsync);
    /// on S3 a lone PUT already is the atomic step.
    pub(crate) async fn write_doc<T: Serialize>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => write_doc_durably(&root.join(name), value),
            Self::S3(s3) => {
                let body = serde_json::to_vec_pretty(value).map_err(to_fatal)?;
                s3.put(name, body).await
            }
        }
    }

    /// The ownership sweep: every file under `{table}/`, expressed as
    /// tails relative to the table root. Both consumers — row counting
    /// and Replace truncation — read this one method, so what "owned"
    /// means is decided in exactly one place.
    pub(crate) async fn keys_of_table(&self, table: &str) -> Result<Vec<String>, DestinationError> {
        match self {
            Self::Local { root } => files_under(&root.join(table)),
            Self::S3(s3) => {
                let table_root = format!("{}/", s3.key_of_table_root(table));
                let mut owned = Vec::new();
                for key in s3.list_keys(table).await? {
                    let full = key.to_string();
                    if let Some(rest) = tail_under_root(&full, &table_root)? {
                        owned.push(rest.to_owned());
                    }
                }
                Ok(owned)
            }
        }
    }

    /// Remove one published final by tail from the output root,
    /// TOLERATING absence: the manifest sweep deletes what a crashed
    /// predecessor MAY have published, and intent always covers more
    /// than what landed before the crash.
    pub(crate) async fn remove_final_if_present(&self, tail: &str) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => match std::fs::remove_file(root.join(tail)) {
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                other => other.map_err(to_fatal),
            },
            Self::S3(s3) => s3.delete_tail_if_present(tail).await,
        }
    }

    /// Fetch one owned file, addressed by tail (row counting).
    pub(crate) async fn read_table_file(
        &self,
        table: &str,
        tail: &str,
    ) -> Result<Vec<u8>, DestinationError> {
        match self {
            Self::Local { root } => std::fs::read(root.join(table).join(tail)).map_err(to_fatal),
            Self::S3(s3) => s3.get_key(&s3.key_of_table(table, tail)).await,
        }
    }

    /// Remove one owned file, addressed by tail (Replace truncation).
    pub(crate) async fn delete_table_file(
        &self,
        table: &str,
        tail: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => {
                std::fs::remove_file(root.join(table).join(tail)).map_err(to_fatal)
            }
            Self::S3(s3) => s3.delete_key(&s3.key_of_table(table, tail)).await,
        }
    }
}

/// A byte stream over either storage kind.
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
    /// Read until `buf` is full; a shorter count means the stream
    /// ended.
    pub(crate) async fn read_full(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::S3(reader) => reader.read_full(buf).await,
            Self::Local(file) => {
                use std::io::Read as _;
                let mut done = 0;
                while done < buf.len() {
                    let n = match file.read(&mut buf[done..]) {
                        Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                        Ok(n) => n,
                    };
                    if n == 0 {
                        break;
                    }
                    done += n;
                }
                Ok(done)
            }
        }
    }
}

/// Turn a `read_full` failure into a source error: the one retryable
/// io kind becomes transient (the engine budget absorbs it); anything
/// else is fatal with the subject named.
pub(crate) fn classify_read_error(context: &str, e: std::io::Error) -> SourceError {
    let rendered = format!("{context}: {e}");
    match e.kind() {
        ErrorKind::ConnectionReset => SourceError::transient(rendered),
        _ => SourceError::fatal(rendered),
    }
}

// ---- disk helpers ----------------------------------------------------------

/// Local promotion of one staged part: ensure the target's directory,
/// force the staged bytes to stable storage, then rename them into
/// place. The two crash points bracket the fsync and the rename so
/// the sweep can interrupt either side.
fn promote_on_disk(
    root: &Path,
    staging_tail: &str,
    final_tail: &str,
) -> Result<(), DestinationError> {
    let staged = root.join(staging_tail);
    let target = root.join(final_tail);
    if let Some(dir) = target.parent() {
        std::fs::create_dir_all(dir).map_err(to_fatal)?;
    }
    crash_point!(
        "pq.staged.sync",
        Err(DestinationError::fatal("injected crash at pq.staged.sync"))
    );
    std::fs::File::open(&staged)
        .and_then(|f| f.sync_all())
        .map_err(to_fatal)?;
    crash_point!(
        "pq.part.rename",
        Err(DestinationError::fatal("injected crash at pq.part.rename"))
    );
    std::fs::rename(&staged, &target).map_err(to_fatal)
}

/// Fsync one directory through an open handle.
fn sync_directory(dir: &Path) -> Result<(), DestinationError> {
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(to_fatal)
}

/// Durable JSON replacement: serialize, write a `.json.tmp` sibling,
/// fsync it, rename over the destination, fsync the parent directory.
fn write_doc_durably<T: Serialize>(path: &Path, value: &T) -> Result<(), DestinationError> {
    use std::io::Write as _;
    let body = serde_json::to_vec_pretty(value).map_err(to_fatal)?;
    let scratch = path.with_extension("json.tmp");
    {
        let mut out = std::fs::File::create(&scratch).map_err(to_fatal)?;
        out.write_all(&body).map_err(to_fatal)?;
        out.sync_all().map_err(to_fatal)?;
    }
    std::fs::rename(&scratch, path).map_err(to_fatal)?;
    match path.parent() {
        Some(dir) => sync_directory(dir),
        None => Ok(()),
    }
}

/// A file's bytes, with absence mapped to `None` rather than an error.
fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, DestinationError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(to_fatal(e)),
    }
}

/// Every regular file below `base`, depth-first, as slash-joined
/// tails. An absent `base` simply means nothing has been written yet.
/// Symlinks are never followed: this crate never stages one, and
/// walking through one would let truncation delete files the root
/// does not own. A name that is not valid UTF-8 is refused with a
/// typed error — inventing a tail for it would misplace its children
/// and give truncation a path the listing never produced.
fn files_under(base: &Path) -> Result<Vec<String>, DestinationError> {
    let mut found = Vec::new();
    descend(base, None, &mut found)?;
    Ok(found)
}

fn descend(dir: &Path, rel: Option<&str>, found: &mut Vec<String>) -> Result<(), DestinationError> {
    let listing = match std::fs::read_dir(dir) {
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        other => other.map_err(to_fatal)?,
    };
    for item in listing {
        let item = item.map_err(to_fatal)?;
        let file_type = item.file_type().map_err(to_fatal)?;
        if file_type.is_symlink() {
            continue;
        }
        let path = item.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return Err(to_fatal(format!(
                "output directory contains a non-UTF-8 name under `{}`; rename it or move \
                 it outside the destination",
                dir.display()
            )));
        };
        let tail = match rel {
            Some(above) => format!("{above}/{name}"),
            None => name.to_owned(),
        };
        if file_type.is_dir() {
            descend(&path, Some(&tail), found)?;
        } else {
            found.push(tail);
        }
    }
    Ok(())
}

/// What remains of a listed key once the table root is stripped off
/// the FRONT — stripping, never searching: nothing stops a partition
/// directory from being named like the table, and a substring search
/// would cut at the wrong occurrence. A key the prefix does not
/// cover is a typed listing violation: silently dropping it would
/// shrink ownership and let Replace strand data. The single tolerated
/// exception is the zero-byte directory marker whose key equals the
/// table root itself (consoles and folder-path `put-object` create
/// these); it carries no data and yields no tail.
fn tail_under_root<'k>(key: &'k str, root: &str) -> Result<Option<&'k str>, DestinationError> {
    if let Some(tail) = key.strip_prefix(root) {
        return Ok(Some(tail));
    }
    if key == root.trim_end_matches('/') {
        return Ok(None);
    }
    Err(to_fatal(format!(
        "listing returned key `{key}`, which is not under the prefix `{root}` it was \
         listed by"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nested files come back as tails, symlinks are invisible, and a
    /// base that does not exist lists as empty.
    #[test]
    fn ownership_walk_covers_depth_skips_symlinks_and_tolerates_absence() {
        let dir = tempfile::tempdir().expect("dir");
        let table = dir.path().join("t");
        std::fs::create_dir_all(table.join("us")).expect("dirs");
        std::fs::write(table.join("a.parquet"), b"x").expect("write");
        std::fs::write(table.join("us/b.parquet"), b"x").expect("write");
        #[cfg(unix)]
        std::os::unix::fs::symlink("/etc", table.join("link")).expect("symlink");

        let mut tails = files_under(&table).expect("walk");
        tails.sort();
        assert_eq!(tails, vec!["a.parquet", "us/b.parquet"]);

        let nothing = files_under(&dir.path().join("missing")).expect("empty");
        assert!(nothing.is_empty());
    }

    /// The root is stripped rather than searched for, the directory
    /// marker yields no tail, and an out-of-prefix key is refused.
    #[test]
    fn tails_are_stripped_never_searched() {
        assert_eq!(
            tail_under_root("out/t/t/part.parquet", "out/t/").expect("ok"),
            Some("t/part.parquet"),
            "a same-named partition directory must not confuse the strip"
        );
        assert_eq!(tail_under_root("out/t", "out/t/").expect("marker"), None);

        let err = tail_under_root("elsewhere/k", "out/t/").expect_err("violation");
        assert!(
            format!("{err}").contains(
                "listing returned key `elsewhere/k`, which is not under the prefix `out/t/`"
            ),
            "{err}"
        );
    }

    /// A written document reads back byte-faithfully, the temp sibling
    /// is gone, and an absent document is `None`.
    #[test]
    fn doc_writes_are_atomic_and_clean() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("doc.json");
        write_doc_durably(&path, &serde_json::json!({"v": 1})).expect("writes");

        let bytes = read_optional(&path).expect("read").expect("present");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("parses");
        assert_eq!(value, serde_json::json!({"v": 1}));

        assert!(!path.with_extension("json.tmp").exists(), "no temp residue");
        assert!(
            read_optional(&dir.path().join("absent.json"))
                .expect("ok")
                .is_none()
        );
    }
}

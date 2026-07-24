//! The location layer: WHERE files live — the local filesystem or an S3-compatible
//! object store. ONE abstraction serves both sides: the source's read half (list,
//! open, metadata) and the destination's write half (stage, publish, delete,
//! list-by-table, durable document IO). Listings are deterministic and
//! COMPLETE-OR-FAIL; errors classify by ONE rulebook and always name their subject
//! (endpoint/bucket/key).
//!
//! The value types (`FileMeta`/`FileTask`/`FileProgress`) live in `types` here, not
//! under `source/`, so this lower layer never imports upward.

pub mod s3;
pub mod types;

use std::path::{Path, PathBuf};

use rdlt_connector::core::crash_point;
use rdlt_connector::{DestinationError, SourceError};
use serde::{Deserialize, Serialize};

pub use types::{FileMeta, FileProgress, FileTask};

/// Where staged (pre-publish) parts and per-pipeline scoping live, relative to
/// the output root/prefix. Shared by the source-invisible destination protocol.
pub(crate) const STAGING_DIR: &str = ".rdlt-staging";

fn fatal(e: impl std::fmt::Display) -> DestinationError {
    DestinationError::fatal(e.to_string())
}

/// The one store-error rulebook on the WRITE path, mirroring the source's
/// `classify`: transport-level failures ride the engine's retry budget;
/// missing objects and auth/permission failures are configuration problems
/// (fatal, the operator's to fix). Both halves consult `s3::is_recoverable`.
pub(crate) fn store_err(e: object_store::Error) -> DestinationError {
    if s3::is_recoverable(&e) {
        DestinationError::transient(e.to_string())
    } else {
        DestinationError::fatal(e.to_string())
    }
}

/// Config form: a struct, not an enum, so YAML stays the natural
/// `location: {s3: {...}}` and future kinds (gcs:, azure:) are ADDITIVE fields
/// with exactly-one-set validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct LocationOptions {
    #[serde(default)]
    pub s3: Option<s3::S3Options>,
}

impl LocationOptions {
    /// Programmatic construction seam for an S3-compatible location.
    pub fn s3(options: s3::S3Options) -> Self {
        Self { s3: Some(options) }
    }

    /// Eager, typed: a `location:` block must name exactly one kind.
    pub fn validate(&self, context: &str) -> Result<(), String> {
        let Some(s3) = &self.s3 else {
            return Err(format!(
                "{context}: `location` block declares no storage kind (expected `s3`)"
            ));
        };
        s3.validate(context)
    }
}

/// A runtime location handle. The read half (source) lists globs and opens
/// sequential readers; the write half (destination) stages, publishes, and
/// truncates under a root directory (local) or key prefix (S3).
#[derive(Debug, Clone)]
pub enum Location {
    Local { root: PathBuf },
    S3(s3::S3Location),
}

// ---- Read half (source side) ----

impl Location {
    /// Source constructor: the read half names whole objects / absolute paths,
    /// so the local root is empty and the S3 prefix is empty.
    pub fn from_options(options: Option<&LocationOptions>) -> Result<Self, SourceError> {
        match options.and_then(|o| o.s3.as_ref()) {
            None => Ok(Self::Local {
                root: PathBuf::new(),
            }),
            Some(options) => Ok(Self::S3(s3::S3Location::connect(options)?)),
        }
    }

    /// Resolve a path/glob (local) or prefix+glob key pattern (S3) into the
    /// deterministic, COMPLETE, lexicographically sorted file snapshot.
    pub async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        match self {
            Self::Local { .. } => crate::source::resolve_files(pattern),
            Self::S3(s3) => s3.list(pattern).await,
        }
    }

    /// Open a sequential byte reader positioned at `start`.
    pub async fn open_from(&self, name: &str, start: u64) -> Result<ByteReader, SourceError> {
        match self {
            Self::Local { .. } => {
                use std::io::{Seek, SeekFrom};
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
}

// ---- Write half (destination side) ----

impl Location {
    /// Destination constructor: `path` is the output directory (local, created)
    /// or the key prefix (S3). Absent `location` → local.
    pub fn for_dest(
        path: &str,
        options: Option<&LocationOptions>,
    ) -> Result<Self, DestinationError> {
        match options.and_then(|o| o.s3.as_ref()) {
            None => {
                let root = PathBuf::from(path);
                std::fs::create_dir_all(&root).map_err(fatal)?;
                Ok(Self::Local { root })
            }
            Some(options) => Ok(Self::S3(s3::S3Location::connect_for_dest(
                options,
                path.trim_matches('/').to_owned(),
            )?)),
        }
    }

    /// A LOCAL output directory, created if needed (the plain-path form). The
    /// `PathBuf` is used AS-IS — non-UTF-8 paths keep their bytes.
    pub fn local_dir(root: PathBuf) -> Result<Self, DestinationError> {
        std::fs::create_dir_all(&root).map_err(fatal)?;
        Ok(Self::Local { root })
    }

    /// Is this the local filesystem? The local-only durability protocol steps
    /// (dir fsync, and their crash points) key off this.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// Reclaim THIS pipeline scope's dead-session staging — staged-but-
    /// uncommitted data from a crashed session must never survive into a
    /// fresh load — then make
    /// the fresh load's staging area ready. Scoped — a sibling pipeline sharing
    /// the output keeps its live staged data.
    pub async fn prepare_staging(&self, scope: &str, load: &str) -> Result<(), DestinationError> {
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

    /// Write one staged part. Local writes the file directly; S3 puts it (the
    /// object-store staging crash point fires HERE, S3-only).
    pub async fn stage_put(
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

    /// Best-effort remove of a staged part (a replayed commit discards its
    /// staged data before returning the prior receipt).
    pub async fn stage_remove(&self, staging_tail: &str) {
        match self {
            Self::Local { root } => {
                let _ = std::fs::remove_file(root.join(staging_tail));
            }
            Self::S3(s3) => s3.delete_best_effort(staging_tail).await,
        }
    }

    /// Publish one staged part to its deterministic final name — the atomic
    /// visibility step. Local: fsync the staged file then rename (per-file
    /// atomic). S3: COPY staged→final then DELETE staged (per-key atomic; a
    /// replayed finalize tolerates a missing staged object). The publish crash
    /// points fire between the sub-steps in their respective (local/S3) arms.
    pub async fn publish_part(
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

    /// Fsync one directory so a rename inside it survives power loss (D2).
    /// No-op on object stores (no directory metadata to sync).
    pub fn sync_dir(&self, dir_tail: &str) -> Result<(), DestinationError> {
        if let Self::Local { root } = self {
            fsync_dir(&root.join(dir_tail))?;
        }
        Ok(())
    }

    /// Read one durable document (state / commit log) as raw bytes; `None` when
    /// absent. Both backends return the bytes verbatim — no parse/reserialize.
    pub async fn read_doc(&self, name: &str) -> Result<Option<Vec<u8>>, DestinationError> {
        match self {
            Self::Local { root } => read_raw(&root.join(name)),
            Self::S3(s3) => s3.read_doc(name).await,
        }
    }

    /// Write one durable document. Local: atomic temp+fsync+rename+dir-fsync so
    /// metadata is no LESS durable than the parts it describes. S3: a single put.
    pub async fn write_doc<T: Serialize>(
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

    /// The ONE ownership listing: every file this destination owns under
    /// `{table}/`, returned as tails relative to the table root. Both row
    /// counting and Replace truncation consume it, so the `"{table}/"` scope
    /// rule lives in exactly one place — a sibling table `events2` can never
    /// leak into table `events`'s listing.
    pub async fn keys_of_table(&self, table: &str) -> Result<Vec<String>, DestinationError> {
        match self {
            Self::Local { root } => walk_local_files(&root.join(table)),
            Self::S3(s3) => {
                let scope = format!("{table}/");
                let mut tails = Vec::new();
                for key in s3.list_keys(table).await? {
                    let name = key.to_string();
                    if let Some(at) = name.rfind(&scope) {
                        tails.push(name[at + scope.len()..].to_owned());
                    }
                }
                Ok(tails)
            }
        }
    }

    /// Read one file under `{table}/` by its tail (row counting).
    pub async fn read_table_file(
        &self,
        table: &str,
        tail: &str,
    ) -> Result<Vec<u8>, DestinationError> {
        match self {
            Self::Local { root } => std::fs::read(root.join(table).join(tail)).map_err(fatal),
            Self::S3(s3) => s3.get_key(&s3.key_of_table(table, tail)).await,
        }
    }

    /// Delete one file under `{table}/` by its tail (Replace truncation).
    pub async fn delete_table_file(&self, table: &str, tail: &str) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => {
                std::fs::remove_file(root.join(table).join(tail)).map_err(fatal)
            }
            Self::S3(s3) => s3.delete_key(&s3.key_of_table(table, tail)).await,
        }
    }

    /// The output root, for the local-only synchronous row counter.
    pub(crate) fn local_root(&self) -> Option<&Path> {
        match self {
            Self::Local { root } => Some(root),
            Self::S3(_) => None,
        }
    }
}

/// Recursively list every FILE under `base`, as slash-joined tails relative to
/// `base`. A missing `base` is an empty listing (the table has no data yet).
fn walk_local_files(base: &Path) -> Result<Vec<String>, DestinationError> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) -> Result<(), DestinationError> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(fatal(e)),
        };
        for entry in entries {
            let path = entry.map_err(fatal)?.path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            let tail = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            if path.is_dir() {
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

/// Fsync a directory so a preceding rename inside it survives power loss.
fn fsync_dir(path: &Path) -> Result<(), DestinationError> {
    std::fs::File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(fatal)
}

/// Atomic durable JSON rewrite: write-temp + fsync + rename + parent-dir fsync.
/// The data-file path fsyncs before rename too — metadata must not be LESS
/// durable than the parquet parts it describes.
fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), DestinationError> {
    use std::io::Write;
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

/// Read a file's raw bytes, or `None` if it does not exist.
fn read_raw(path: &Path) -> Result<Option<Vec<u8>>, DestinationError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(fatal(e)),
    }
}

/// A sequential reader over either location kind. Local reads stay plain
/// `std::fs` (the fast path); S3 reads drain a streaming GET.
pub enum ByteReader {
    Local(std::fs::File),
    S3(s3::S3Reader),
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
    /// Fill `buf` as far as possible (short only at end-of-stream).
    pub async fn read_full(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Local(file) => {
                use std::io::Read;
                let mut filled = 0;
                let mut rest: &mut [u8] = buf;
                while !rest.is_empty() {
                    match file.read(rest) {
                        Ok(0) => break,
                        Ok(n) => {
                            filled += n;
                            rest = &mut rest[n..];
                        }
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

/// Classify a `read_full` failure into the source taxonomy: a retryable
/// kind (a mid-object transport reset carried through the io seam) rides
/// the engine budget; everything else is fatal with the subject named.
pub(crate) fn classify_read_error(context: &str, e: std::io::Error) -> SourceError {
    if e.kind() == std::io::ErrorKind::ConnectionReset {
        SourceError::transient(format!("{context}: {e}"))
    } else {
        SourceError::fatal(format!("{context}: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use rdlt_connector::Secret;

    use super::*;

    /// One recoverability rulebook for store errors, both halves: a
    /// mid-object transport failure must ride the engine's retry budget
    /// (transient), never abort the run; missing objects and auth
    /// failures stay fatal.
    #[test]
    fn store_error_recoverability_is_shared_and_honest() {
        let transport = object_store::Error::Generic {
            store: "S3",
            source: "connection reset by peer".into(),
        };
        assert!(s3::is_recoverable(&transport));
        assert!(matches!(
            store_err(transport),
            DestinationError::Transient { .. }
        ));
        let missing = object_store::Error::NotFound {
            path: "x".into(),
            source: "gone".into(),
        };
        assert!(!s3::is_recoverable(&missing));
        assert!(matches!(store_err(missing), DestinationError::Fatal { .. }));

        // The io seam carries the classification through read_full.
        let reset = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "mid-stream");
        assert!(matches!(
            classify_read_error("reading `k`", reset),
            SourceError::Transient { .. }
        ));
        let other = std::io::Error::other("parse");
        assert!(matches!(
            classify_read_error("reading `k`", other),
            SourceError::Fatal { .. }
        ));
    }

    fn s3_options(endpoint: &str, bucket: &str) -> LocationOptions {
        LocationOptions {
            s3: Some(s3::S3Options {
                endpoint: endpoint.into(),
                bucket: bucket.into(),
                region: None,
                access_key: Secret::new("k"),
                secret_key: Secret::new("s"),
                path_style: s3::default_path_style(),
            }),
        }
    }

    #[test]
    fn location_block_requires_a_kind() {
        let empty = LocationOptions { s3: None };
        let err = empty.validate("stream `a`").unwrap_err();
        assert!(err.contains("no storage kind"), "{err}");
    }

    #[test]
    fn s3_options_validate_eagerly() {
        let err = s3_options("not a url", "b")
            .validate("stream `a`")
            .unwrap_err();
        assert!(err.contains("endpoint"), "{err}");
        let err = s3_options("http://x:9000", "")
            .validate("stream `a`")
            .unwrap_err();
        assert!(err.contains("bucket"), "{err}");
        s3_options("http://x:9000", "raw")
            .validate("stream `a`")
            .unwrap();
    }

    /// Credentials never render in Debug output; serde serialization intentionally
    /// round-trips the value so config can still be parsed.
    #[test]
    fn secrets_never_render_in_location_config() {
        let options = LocationOptions {
            s3: Some(s3::S3Options {
                endpoint: "http://x:9000".into(),
                bucket: "raw".into(),
                region: None,
                access_key: Secret::new("hunter2-access"),
                secret_key: Secret::new("hunter2-secret"),
                path_style: true,
            }),
        };
        let rendered = format!("{options:?}");
        assert!(!rendered.contains("hunter2-access"), "{rendered}");
        assert!(!rendered.contains("hunter2-secret"), "{rendered}");
        assert!(rendered.contains("***"), "{rendered}");
    }
}

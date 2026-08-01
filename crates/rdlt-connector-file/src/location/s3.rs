//! S3-compatible object storage via `object_store` (the `aws` feature only). Listings
//! drain every continuation page or fail — a partial listing must never pass as a
//! complete one. Errors classify by kind (transient vs fatal) and name
//! endpoint/bucket/key.

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use rdlt_connector::{DestinationError, SourceError};
use serde::{Deserialize, Serialize};

use rdlt_connector::Secret;

use super::store_err;
use crate::location::types::FileMeta;

/// Configuration for an S3-compatible location: `location: {s3: {...}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct S3Options {
    /// e.g. `http://127.0.0.1:9000` or `https://s3.amazonaws.com`
    pub endpoint: String,
    pub bucket: String,
    /// Default `us-east-1` (S3-compatible servers commonly ignore it).
    #[serde(default)]
    pub region: Option<String>,
    pub access_key: Secret,
    pub secret_key: Secret,
    /// Path-style addressing (default true — the test-server form; AWS
    /// itself accepts it for most operations).
    #[serde(default = "default_path_style")]
    pub path_style: bool,
    /// Send `UNSIGNED-PAYLOAD` instead of the body's SHA-256 in the SigV4
    /// canonical request. **Off by default, and deliberately never inferred.**
    ///
    /// Hashing the body is 6.72% of the parquet-to-S3 cell, so this is real
    /// throughput — but it is a security setting, not a tuning knob, and the
    /// trade must be made by the person who knows the deployment:
    ///
    /// Signing the payload means the request signature covers the BODY. A
    /// proxy, a compromised intermediary, or a corrupted transfer cannot alter
    /// the bytes without invalidating the signature. With this on, the
    /// signature covers only the headers and the request line: the transport
    /// (TLS) becomes the only thing standing between the body and
    /// modification in flight.
    ///
    /// So: reasonable over HTTPS to an endpoint you trust, on a network you
    /// control. Not reasonable over plain HTTP, where it removes the last
    /// integrity check the request had.
    #[serde(default)]
    pub unsigned_payload: bool,
}

pub(crate) fn default_path_style() -> bool {
    true
}

impl S3Options {
    /// Programmatic construction seam (config documents are the primary
    /// path; fixtures and embedders build options directly).
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<Secret>,
        secret_key: impl Into<Secret>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            region: None,
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            path_style: true,
            unsigned_payload: false,
        }
    }

    /// Opt out of payload signing. Read [`Self::unsigned_payload`] before
    /// calling this — it trades request-body integrity for throughput.
    #[must_use]
    pub fn with_unsigned_payload(mut self, unsigned: bool) -> Self {
        self.unsigned_payload = unsigned;
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = path_style;
        self
    }

    pub fn validate(&self, context: &str) -> Result<(), String> {
        let rest = self
            .endpoint
            .strip_prefix("http://")
            .or_else(|| self.endpoint.strip_prefix("https://"));
        match rest {
            Some(host) if !host.is_empty() => {}
            _ => {
                return Err(format!(
                    "{context}: location.s3.endpoint `{}` must be an http(s) URL",
                    self.endpoint
                ));
            }
        }
        if self.bucket.is_empty() {
            return Err(format!("{context}: location.s3.bucket must not be empty"));
        }
        Ok(())
    }
}

/// A connected S3-compatible location — the S3 half of the unified
/// [`super::Location`], serving BOTH the source read path (list/get) and the
/// destination write path (put/copy/delete/list-by-table). `prefix` is the key
/// prefix all write-side tails hang under (the destination's `path`); it is
/// EMPTY on the read side, where callers pass full keys.
#[derive(Debug, Clone)]
pub struct S3Location {
    store: AmazonS3,
    endpoint: String,
    bucket: String,
    prefix: String,
}

/// Build the raw client (shared by the source Location and the dest
/// store). Secrets are revealed HERE only.
pub(crate) fn build_store(options: &S3Options) -> Result<AmazonS3, String> {
    AmazonS3Builder::new()
        .with_endpoint(&options.endpoint)
        .with_bucket_name(&options.bucket)
        .with_region(options.region.as_deref().unwrap_or("us-east-1"))
        .with_access_key_id(options.access_key.reveal())
        .with_secret_access_key(options.secret_key.reveal())
        .with_virtual_hosted_style_request(!options.path_style)
        // Only ever what the user asked for; `false` is object_store's own
        // default, so passing it through changes nothing.
        .with_unsigned_payload(options.unsigned_payload)
        .with_allow_http(true)
        .build()
        .map_err(|e| {
            format!(
                "s3 location `{}` bucket `{}`: {e}",
                options.endpoint, options.bucket
            )
        })
}

impl S3Location {
    /// Read-side connect: full keys, no prefix (the source names whole objects).
    pub fn connect(options: &S3Options) -> Result<Self, SourceError> {
        Self::build(options, String::new()).map_err(SourceError::fatal)
    }

    /// Write-side connect: `prefix` is the destination's key prefix; every
    /// staged, published, state, and receipt key hangs under it.
    pub(crate) fn connect_for_dest(
        options: &S3Options,
        prefix: String,
    ) -> Result<Self, DestinationError> {
        Self::build(options, prefix).map_err(DestinationError::fatal)
    }

    fn build(options: &S3Options, prefix: String) -> Result<Self, String> {
        Ok(Self {
            store: build_store(options)?,
            endpoint: options.endpoint.clone(),
            bucket: options.bucket.clone(),
            prefix,
        })
    }

    /// Resolve a tail into a full object key under this location's prefix.
    fn key(&self, tail: &str) -> object_store::path::Path {
        let joined = if self.prefix.is_empty() {
            tail.to_owned()
        } else {
            format!("{}/{tail}", self.prefix.trim_end_matches('/'))
        };
        object_store::path::Path::from(joined)
    }

    /// Read one document's bytes (state / commit log); `None` when absent.
    pub(crate) async fn read_doc(&self, name: &str) -> Result<Option<Vec<u8>>, DestinationError> {
        match self.store.get(&self.key(name)).await {
            Ok(result) => Ok(Some(result.bytes().await.map_err(store_err)?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(store_err(e)),
        }
    }

    /// Put raw bytes at `tail` (atomic per key: no partial object is ever
    /// visible under the final name).
    pub(crate) async fn put(&self, tail: &str, bytes: Vec<u8>) -> Result<(), DestinationError> {
        self.store
            .put(&self.key(tail), bytes::Bytes::from(bytes).into())
            .await
            .map_err(store_err)?;
        Ok(())
    }

    /// COPY `from_tail` → `to_tail` (per-key atomic publish).
    pub(crate) async fn copy(
        &self,
        from_tail: &str,
        to_tail: &str,
    ) -> Result<(), DestinationError> {
        self.store
            .copy(&self.key(from_tail), &self.key(to_tail))
            .await
            .map_err(store_err)
    }

    /// Delete `tail`; a replayed finalize may find it already gone, which is
    /// success (idempotent).
    pub(crate) async fn delete_idempotent(&self, tail: &str) -> Result<(), DestinationError> {
        match self.store.delete(&self.key(tail)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(store_err(e)),
        }
    }

    /// Best-effort delete (staging cleanup on a replayed commit); errors are
    /// swallowed — the reclaim converges on the next open.
    pub(crate) async fn delete_best_effort(&self, tail: &str) {
        let _ = self.store.delete(&self.key(tail)).await;
    }

    /// List full keys under `tail` (server-side segment-scoped: listing
    /// `foo/a` never returns `foo/ab/*` — pinned by tests/prefix_semantics.rs).
    pub(crate) async fn list_keys(
        &self,
        tail: &str,
    ) -> Result<Vec<object_store::path::Path>, DestinationError> {
        let mut listing = self.store.list(Some(&self.key(tail)));
        let mut keys = Vec::new();
        while let Some(entry) = listing.next().await {
            keys.push(entry.map_err(store_err)?.location);
        }
        Ok(keys)
    }

    /// Delete a full object key (already resolved by a prior listing).
    pub(crate) async fn delete_key(
        &self,
        key: &object_store::path::Path,
    ) -> Result<(), DestinationError> {
        self.store.delete(key).await.map_err(store_err)
    }

    /// The full object key for one file under `{table}/` addressed by its tail.
    pub(crate) fn key_of_table(&self, table: &str, tail: &str) -> object_store::path::Path {
        self.key(&format!("{table}/{tail}"))
    }

    /// The key prefix every one of a table's objects sits under. Ownership
    /// listing strips this exactly rather than searching for it: a partition
    /// value can spell the table's own name, and a search would then split the
    /// key at the wrong place.
    pub(crate) fn key_of_table_root(&self, table: &str) -> object_store::path::Path {
        self.key(table)
    }

    /// Read a full object key's bytes (already resolved by a prior listing).
    pub(crate) async fn get_key(
        &self,
        key: &object_store::path::Path,
    ) -> Result<Vec<u8>, DestinationError> {
        Ok(self
            .store
            .get(key)
            .await
            .map_err(store_err)?
            .bytes()
            .await
            .map_err(store_err)?
            .to_vec())
    }

    fn classify(&self, action: &str, subject: &str, error: object_store::Error) -> SourceError {
        let name = format!(
            "{action} `{subject}` (s3 `{}` bucket `{}`)",
            self.endpoint, self.bucket
        );
        // Severity comes from the ONE rulebook; this match only chooses the
        // wording, so a message can never disagree with a classification.
        if rdlt_connector::store::is_recoverable(&error) {
            return SourceError::transient(format!("{name}: {error}"));
        }
        match &error {
            object_store::Error::NotFound { .. } => {
                SourceError::fatal(format!("{name}: not found"))
            }
            // Auth/permission failures are configuration problems: fatal,
            // named — never a silent empty load.
            object_store::Error::Unauthenticated { .. }
            | object_store::Error::PermissionDenied { .. } => {
                SourceError::fatal(format!("{name}: unauthorized — check credentials/bucket"))
            }
            _ => SourceError::fatal(format!("{name}: {error}")),
        }
    }
}

impl S3Location {
    /// Split a `path` pattern into the fixed prefix and an optional glob
    /// remainder (`landed/2026/*.jsonl` → prefix `landed/2026/`, glob over
    /// the full key).
    fn prefix_of(pattern: &str) -> &str {
        match pattern.find(['*', '?', '[']) {
            Some(at) => &pattern[..pattern[..at].rfind('/').map(|s| s + 1).unwrap_or(0)],
            None => pattern,
        }
    }

    /// COMPLETE listing (continuation pages fully drained or typed
    /// failure), deterministic order, glob-filtered. Semantics mirror the
    /// local rules: a glob with zero matches is success; a pattern WITHOUT
    /// glob metacharacters names one object — missing is a typed error.
    pub async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        let has_glob = pattern.contains(['*', '?', '[']);
        // The LOCAL rule holds here too: a pattern naming an EXISTING object
        // is taken literally, even when it contains glob metacharacters
        // (`events[v1].jsonl` is a key, not a character class). One HEAD
        // decides; only then does glob interpretation apply.
        let literal = object_store::path::Path::from(pattern);
        match self.store.head(&literal).await {
            Ok(head) => {
                return Ok(vec![FileMeta {
                    path: pattern.to_owned(),
                    size_units: head.size,
                    mtime_ms: None,
                    etag: head.e_tag,
                }]);
            }
            Err(object_store::Error::NotFound { .. }) if has_glob => {}
            Err(object_store::Error::NotFound { .. }) => {
                return Err(SourceError::fatal(format!(
                    "object `{pattern}` (s3 `{}` bucket `{}`): not found",
                    self.endpoint, self.bucket
                )));
            }
            Err(e) => return Err(self.classify("object", pattern, e)),
        }
        let matcher = glob::Pattern::new(pattern)
            .map_err(|e| SourceError::fatal(format!("invalid glob `{pattern}`: {e}")))?;
        // `*`/`?` must NOT cross `/` — the local glob walks per component,
        // and staged keys under .rdlt-staging/ must never match a data glob.
        let match_options = glob::MatchOptions {
            require_literal_separator: true,
            ..Default::default()
        };
        let prefix = Self::prefix_of(pattern);
        let prefix_path = (!prefix.is_empty()).then(|| object_store::path::Path::from(prefix));
        let mut listing = self.store.list(prefix_path.as_ref());
        let mut matched = Vec::new();
        while let Some(entry) = listing.next().await {
            let entry = entry.map_err(|e| self.classify("listing", pattern, e))?;
            let key = entry.location.to_string();
            if matcher.matches_with(&key, match_options) {
                matched.push(FileMeta {
                    path: key,
                    size_units: entry.size,
                    mtime_ms: None,
                    etag: entry.e_tag,
                });
            }
        }
        matched.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(matched)
    }

    /// Streaming GET from `start` (range read for resumed tails).
    pub async fn open_from(
        &self,
        name: &str,
        start: u64,
    ) -> Result<super::ByteReader, SourceError> {
        let path = object_store::path::Path::from(name);
        let options = object_store::GetOptions {
            range: (start > 0).then_some(object_store::GetRange::Offset(start)),
            ..Default::default()
        };
        let result = self
            .store
            .get_opts(&path, options)
            .await
            .map_err(|e| self.classify("reading", name, e))?;
        Ok(super::ByteReader::S3(S3Reader {
            stream: result.into_stream().boxed(),
            pending: bytes::Bytes::new(),
            subject: format!("{name} (s3 `{}` bucket `{}`)", self.endpoint, self.bucket),
        }))
    }
}

/// Sequential reader draining a streaming GET.
pub struct S3Reader {
    stream: futures::stream::BoxStream<'static, object_store::Result<bytes::Bytes>>,
    pending: bytes::Bytes,
    subject: String,
}

impl std::fmt::Debug for S3Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Reader")
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}

impl S3Reader {
    pub async fn read_full(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            if self.pending.is_empty() {
                match self.stream.next().await {
                    None => break,
                    Some(Err(e)) => {
                        // Carry recoverability through the io::Error seam:
                        // a mid-stream transport failure keeps a retryable
                        // kind so consumers classify it transient instead
                        // of failing the whole run.
                        let kind = if rdlt_connector::store::is_recoverable(&e) {
                            std::io::ErrorKind::ConnectionReset
                        } else {
                            std::io::ErrorKind::Other
                        };
                        return Err(std::io::Error::new(
                            kind,
                            format!("reading {}: {e}", self.subject),
                        ));
                    }
                    Some(Ok(chunk)) => self.pending = chunk,
                }
            }
            let take = self.pending.len().min(buf.len() - filled);
            buf[filled..filled + take].copy_from_slice(&self.pending[..take]);
            bytes::Buf::advance(&mut self.pending, take);
            filled += take;
        }
        Ok(filled)
    }
}

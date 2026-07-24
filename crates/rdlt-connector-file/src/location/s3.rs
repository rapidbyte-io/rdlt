//! S3-compatible object storage via `object_store` (research R1: the one
//! surveyed new dependency, `aws` feature only). Listings drain every
//! continuation page or fail — a partial listing must never pass as a
//! complete one (FF2). Errors classify per the S3 posture (FF6) and name
//! endpoint/bucket/key.

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use rdlt_connector::SourceError;
use serde::{Deserialize, Serialize};

use super::Secret;
use crate::source::cursor::FileMeta;

/// `location: {s3: {…}}` (data-model §1).
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
        }
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

/// A connected S3-compatible location.
#[derive(Debug)]
pub struct S3Location {
    store: AmazonS3,
    endpoint: String,
    bucket: String,
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
    pub fn connect(options: &S3Options) -> Result<Self, SourceError> {
        let store = build_store(options).map_err(SourceError::fatal)?;
        Ok(Self {
            store,
            endpoint: options.endpoint.clone(),
            bucket: options.bucket.clone(),
        })
    }

    fn classify(&self, action: &str, subject: &str, error: object_store::Error) -> SourceError {
        let name = format!(
            "{action} `{subject}` (s3 `{}` bucket `{}`)",
            self.endpoint, self.bucket
        );
        match &error {
            object_store::Error::NotFound { .. } => {
                SourceError::fatal(format!("{name}: not found"))
            }
            // Auth/permission failures are configuration problems: fatal,
            // named — never a silent empty load (FF2).
            object_store::Error::Unauthenticated { .. }
            | object_store::Error::PermissionDenied { .. } => {
                SourceError::fatal(format!("{name}: unauthorized — check credentials/bucket"))
            }
            // Everything else at the transport/store level is the engine
            // budget's business.
            _ => SourceError::transient(format!("{name}: {error}")),
        }
    }
}

/// The one store-error recoverability rule, shared by the read and write
/// paths: missing objects and auth/permission failures are configuration
/// problems (fatal, the operator's to fix); everything else at the
/// transport/store level heals on retry and rides the engine budget.
pub(crate) fn is_recoverable(error: &object_store::Error) -> bool {
    !matches!(
        error,
        object_store::Error::NotFound { .. }
            | object_store::Error::Unauthenticated { .. }
            | object_store::Error::PermissionDenied { .. }
    )
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
                    size: head.size,
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
                    size: entry.size,
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
                        let kind = if is_recoverable(&e) {
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

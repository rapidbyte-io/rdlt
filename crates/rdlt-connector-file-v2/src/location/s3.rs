//! The S3 half: store construction (the one Secret boundary), the
//! complete-or-fail listing, and the streaming range reader.

use futures::StreamExt as _;
use object_store::ObjectStore as _;
use rdlt_connector_sdk::spi::SourceError;

use super::options::S3Options;
use crate::source::cursor::FileMeta;

/// Build the raw S3 client. Every `Secret::reveal` in the crate
/// happens here.
pub(crate) fn build_store(options: &S3Options) -> Result<object_store::aws::AmazonS3, String> {
    object_store::aws::AmazonS3Builder::new()
        .with_endpoint(&options.endpoint)
        .with_bucket_name(&options.bucket)
        .with_region(options.region.as_deref().unwrap_or("us-east-1"))
        .with_access_key_id(options.access_key.reveal())
        .with_secret_access_key(options.secret_key.reveal())
        .with_virtual_hosted_style_request(!options.path_style)
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

/// A connected S3 location — the read half: full keys, no prefix.
#[derive(Debug, Clone)]
pub(crate) struct S3Location {
    store: object_store::aws::AmazonS3,
    endpoint: String,
    bucket: String,
}

impl S3Location {
    pub(crate) fn connect(options: &S3Options) -> Result<Self, SourceError> {
        Ok(Self {
            store: build_store(options).map_err(SourceError::fatal)?,
            endpoint: options.endpoint.clone(),
            bucket: options.bucket.clone(),
        })
    }

    /// Severity comes from the ONE shared recoverability rulebook; the
    /// match below only chooses wording, so a message can never
    /// disagree with a classification.
    fn classify(&self, action: &str, subject: &str, error: object_store::Error) -> SourceError {
        let name = format!(
            "{action} `{subject}` (s3 `{}` bucket `{}`)",
            self.endpoint, self.bucket
        );
        if rdlt_connector_sdk::spi::store::is_recoverable(&error) {
            return SourceError::transient(format!("{name}: {error}"));
        }
        match &error {
            object_store::Error::NotFound { .. } => {
                SourceError::fatal(format!("{name}: not found"))
            }
            object_store::Error::Unauthenticated { .. }
            | object_store::Error::PermissionDenied { .. } => {
                SourceError::fatal(format!("{name}: unauthorized — check credentials/bucket"))
            }
            _ => SourceError::fatal(format!("{name}: {error}")),
        }
    }

    /// The fixed key prefix ahead of the first glob metacharacter —
    /// what the server-side listing scopes to.
    fn prefix_of(pattern: &str) -> &str {
        match pattern.find(['*', '?', '[']) {
            Some(at) => &pattern[..pattern[..at].rfind('/').map(|s| s + 1).unwrap_or(0)],
            None => pattern,
        }
    }

    /// COMPLETE listing: continuation pages fully drained or a typed
    /// failure. The local ambiguity rule holds — one HEAD decides
    /// whether a metacharacter-bearing pattern names an existing
    /// object; only then does glob interpretation apply, and `*`/`?`
    /// never cross `/` (staged keys must never match a data glob).
    pub(crate) async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        let has_glob = pattern.contains(['*', '?', '[']);
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

    /// Streaming GET from `start` — the range read a resumed tail uses.
    pub(crate) async fn open_from(
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

/// Sequential reader draining a streaming GET. A mid-stream transport
/// failure keeps a RETRYABLE io kind, so consumers classify it
/// transient instead of failing the run — recoverability carried
/// through the io seam.
pub(crate) struct S3Reader {
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
    pub(crate) async fn read_full(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            if self.pending.is_empty() {
                match self.stream.next().await {
                    None => break,
                    Some(Err(e)) => {
                        let kind = if rdlt_connector_sdk::spi::store::is_recoverable(&e) {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The listing scope: everything before the first metacharacter's
    /// last separator; a glob-free pattern scopes to itself.
    #[test]
    fn the_listing_prefix_stops_at_the_first_metacharacter() {
        assert_eq!(S3Location::prefix_of("landed/2026/*.jsonl"), "landed/2026/");
        assert_eq!(S3Location::prefix_of("landed/y=*/f.jsonl"), "landed/");
        assert_eq!(S3Location::prefix_of("*.jsonl"), "");
        assert_eq!(S3Location::prefix_of("plain/key.jsonl"), "plain/key.jsonl");
    }

    /// The store builds from valid options — the one boundary works.
    #[test]
    fn a_valid_store_builds() {
        build_store(&S3Options::new("http://127.0.0.1:9000", "b", "ak", "sk")).expect("builds");
    }
}

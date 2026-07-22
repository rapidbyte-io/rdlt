//! The location layer (contract FF2/FF3): WHERE files live — the local
//! filesystem (exactly the pre-015 semantics) or an S3-compatible object
//! store. Shared by the source and destination sides. Listings are
//! deterministic and COMPLETE-OR-FAIL; errors classify per the S3 posture
//! and always name their subject (endpoint/bucket/key).

pub mod s3;
pub mod secret;

use rdlt_connector::SourceError;
use serde::{Deserialize, Serialize};

pub use secret::Secret;

use crate::source::cursor::FileMeta;

/// Config form (data-model §1): a struct, not an enum, so YAML stays the
/// natural `location: {s3: {…}}` and future kinds (gcs:, azure:) are
/// ADDITIVE fields with exactly-one-set validation.
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

    /// Eager, typed (FF2 tail): a `location:` block must name exactly one
    /// kind.
    pub fn validate(&self, context: &str) -> Result<(), String> {
        if self.s3.is_none() {
            return Err(format!(
                "{context}: `location` block declares no storage kind (expected `s3`)"
            ));
        }
        if let Some(s3) = &self.s3 {
            s3.validate(context)?;
        }
        Ok(())
    }
}

/// A runtime location handle: listing and reading for one stream/dest.
#[derive(Debug)]
pub enum Location {
    Local,
    S3(s3::S3Location),
}

impl Location {
    pub fn from_options(options: Option<&LocationOptions>) -> Result<Self, SourceError> {
        match options.and_then(|o| o.s3.as_ref()) {
            None => Ok(Self::Local),
            Some(options) => Ok(Self::S3(s3::S3Location::connect(options)?)),
        }
    }

    /// Resolve a path/glob (local) or prefix+glob key pattern (S3) into the
    /// deterministic, COMPLETE, lexicographically sorted file snapshot.
    pub async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        match self {
            Self::Local => crate::source::resolve_files(pattern),
            Self::S3(s3) => s3.list(pattern).await,
        }
    }

    /// Open a sequential byte reader positioned at `start`.
    pub async fn open_from(&self, name: &str, start: u64) -> Result<ByteReader, SourceError> {
        match self {
            Self::Local => {
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

/// A sequential reader over either location kind. Local reads stay plain
/// `std::fs` (the pre-015 hot path, byte-identical behavior); S3 reads
/// drain a streaming GET.
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

#[cfg(test)]
mod tests {
    use super::*;

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

    /// The grep-proof (FF6): credentials never render in Debug (serde
    /// serialization intentionally round-trips — same posture as 014).
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

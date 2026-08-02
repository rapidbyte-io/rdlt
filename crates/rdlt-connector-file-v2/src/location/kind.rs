//! The unified Location: one enum over the local filesystem and S3,
//! serving the read half's list/open primitives. (The write half's
//! primitives join with the destination.)

use rdlt_connector_sdk::spi::SourceError;

use super::options::LocationOptions;
use super::s3::S3Location;
use crate::source::cursor::FileMeta;
use crate::source::list::local_listing;

/// A connected location.
#[derive(Debug, Clone)]
pub enum Location {
    Local,
    S3(S3Location),
}

impl Location {
    /// The read-half constructor: full paths/keys, no prefix.
    pub(crate) fn from_options(options: Option<&LocationOptions>) -> Result<Self, SourceError> {
        match options.and_then(|o| o.s3.as_ref()) {
            None => Ok(Self::Local),
            Some(s3) => Ok(Self::S3(S3Location::connect(s3)?)),
        }
    }

    /// The complete-or-fail listing, either kind.
    pub(crate) async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        match self {
            Self::Local => local_listing(pattern),
            Self::S3(s3) => s3.list(pattern).await,
        }
    }

    /// A sequential byte reader positioned at `start`.
    pub(crate) async fn open_from(
        &self,
        name: &str,
        start: u64,
    ) -> Result<ByteReader, SourceError> {
        match self {
            Self::Local => {
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
}

/// A sequential reader over either kind: plain std::fs on the fast
/// local path, a drained streaming GET on S3.
pub enum ByteReader {
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

/// Classify a `read_full` failure: the retryable kind (a mid-object
/// transport reset carried through the io seam) rides the engine
/// budget; everything else is fatal, subject named.
pub(crate) fn classify_read_error(context: &str, e: std::io::Error) -> SourceError {
    if e.kind() == std::io::ErrorKind::ConnectionReset {
        SourceError::transient(format!("{context}: {e}"))
    } else {
        SourceError::fatal(format!("{context}: {e}"))
    }
}

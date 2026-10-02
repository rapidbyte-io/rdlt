//! The format of the files destination's own files, its manifests and table catalogs: each says
//! which it is, and a reader takes only its own.

use serde::{Deserialize, Serialize};

/// The format this build writes and reads.
const FORMAT: u16 = 1;

/// A file's format: written as this build's, and read only as it, so a file of another build is
/// refused rather than read as one it is not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
pub(super) struct Format;

/// A file of another format.
#[derive(Debug, thiserror::Error)]
#[error("the file is of format {0}; this build reads format {FORMAT}")]
pub(super) struct OtherFormat(u16);

impl TryFrom<u16> for Format {
    type Error = OtherFormat;

    fn try_from(format: u16) -> Result<Self, OtherFormat> {
        if format == FORMAT {
            Ok(Self)
        } else {
            Err(OtherFormat(format))
        }
    }
}

impl From<Format> for u16 {
    fn from(Format: Format) -> Self {
        FORMAT
    }
}

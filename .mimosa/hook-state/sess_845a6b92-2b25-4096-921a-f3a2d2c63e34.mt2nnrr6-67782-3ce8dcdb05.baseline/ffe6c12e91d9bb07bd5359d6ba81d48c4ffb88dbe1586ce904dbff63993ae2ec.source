//! The harness's one error type: a message that always names the
//! offender — a bad cell file says which file and which cell.

use std::path::Path;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(pub String);

impl Error {
    /// An io failure with the path it concerns — a bare `?` on `std::fs`
    /// yields only "io: {e}" with no offender named.
    pub(crate) fn io(path: &Path, e: std::io::Error) -> Self {
        Self(format!("{}: {e}", path.display()))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self(format!("io: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Read + parse one TOML config file, naming the file in either failure —
/// the one place the "reading {path}" / "parsing {path}" wording lives.
pub(crate) fn load_toml<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| Error(format!("reading {}: {e}", path.display())))?;
    toml::from_str(&raw).map_err(|e| Error(format!("parsing {}: {e}", path.display())))
}

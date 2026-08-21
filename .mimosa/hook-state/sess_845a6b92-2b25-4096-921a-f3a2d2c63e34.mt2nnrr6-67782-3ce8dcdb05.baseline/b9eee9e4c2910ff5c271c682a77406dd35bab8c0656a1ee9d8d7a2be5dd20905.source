//! The destination's configuration document and its gate.

use rdlt_connector_sdk::config::Document;

/// The reference destination document: ONE output directory.
/// `{ "path": "out/dir" }`
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The directory the parts, receipts, and state land in; created at
    /// the first connect.
    pub path: String,
}

/// The destination's configuration error — parser framings plus the
/// config gate's own refusals, every spelling owned here.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// YAML did not parse as the config document.
    #[error("invalid reference destination YAML: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    /// JSON did not parse as the config document.
    #[error("invalid reference destination JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The document parsed but violates an invariant.
    #[error("invalid reference destination config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = Error;

    fn validate(&self) -> Result<(), Error> {
        if self.path.is_empty() {
            return Err(Error::Invalid(
                "`path` is empty — one output directory is required".into(),
            ));
        }
        Ok(())
    }
}

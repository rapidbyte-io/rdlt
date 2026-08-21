//! The source's configuration document and its gate.

use rdlt_connector_sdk::config::Document;

/// The reference source document: ONE jsonl file, nothing else.
/// `{ "path": "/abs/or/rel/file.jsonl" }`
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The jsonl file to read. Its stem names the one stream.
    pub path: String,
}

/// The source's configuration error — parser framings plus the config
/// gate's own refusals, every spelling owned here.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// YAML did not parse as the config document.
    #[error("invalid reference source YAML: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    /// JSON did not parse as the config document.
    #[error("invalid reference source JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The document parsed but violates an invariant.
    #[error("invalid reference source config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = Error;

    fn validate(&self) -> Result<(), Error> {
        if self.path.is_empty() {
            return Err(Error::Invalid(
                "`path` is empty — one jsonl file is required".into(),
            ));
        }
        if stem_of(&self.path).is_none() {
            return Err(Error::Invalid(format!(
                "`{}` has no file stem to name the stream",
                self.path
            )));
        }
        Ok(())
    }
}

/// The stream name a path yields: its UTF-8 file stem, when it has one.
pub(crate) fn stem_of(path: &str) -> Option<String> {
    let stem = std::path::Path::new(path).file_stem()?.to_str()?;
    (!stem.is_empty()).then(|| stem.to_owned())
}

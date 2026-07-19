//! Connector self-description.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorSpec {
    pub name: String,
    pub version: String,
    /// JSON Schema of the connector's configuration, for platform UIs.
    pub config_schema: Option<serde_json::Value>,
}

impl ConnectorSpec {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            config_schema: None,
        }
    }
}

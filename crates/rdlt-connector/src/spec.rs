//! Connector self-description.

use serde::{Deserialize, Serialize};

/// How a connector identifies itself to a host.
///
/// Serializable on purpose: a platform embedding rdlt renders a connector
/// picker and a configuration form from exactly this, without linking the
/// connector crate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorSpec {
    /// Stable connector identifier (`postgres`, `rest`, `iceberg`).
    pub name: String,
    /// The connector's own version, independent of the engine's.
    pub version: String,
    /// JSON Schema of the connector's configuration, for platform UIs.
    pub config_schema: Option<serde_json::Value>,
}

impl ConnectorSpec {
    /// A spec with no configuration schema. Add one with a struct update when
    /// the connector can generate it.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            config_schema: None,
        }
    }
}

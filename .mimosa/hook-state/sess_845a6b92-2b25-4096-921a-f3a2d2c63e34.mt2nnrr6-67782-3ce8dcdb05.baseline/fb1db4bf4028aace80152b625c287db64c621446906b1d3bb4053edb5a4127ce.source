//! Connector self-description.

use serde::{Deserialize, Serialize};

/// How a connector identifies itself to a host.
///
/// Serializable on purpose: a platform embedding rdlt renders its
/// connector picker and configuration form from exactly this value,
/// without linking the connector crate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorSpec {
    /// Stable connector identifier (`postgres`, `rest`, `iceberg`).
    pub name: String,
    /// The connector's own version, independent of the host's.
    pub version: String,
    /// JSON Schema of the connector's configuration, for platform UIs.
    /// `None` means the connector does not describe its configuration.
    pub config_schema: Option<serde_json::Value>,
}

impl ConnectorSpec {
    /// A spec without a configuration schema; set `config_schema` when
    /// the connector can generate one.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            config_schema: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The wire form is what a platform stores and renders — field names
    /// are part of the contract.
    #[test]
    fn the_wire_form_keeps_its_field_names() {
        let mut spec = ConnectorSpec::new("postgres", "1.2.3");
        spec.config_schema = Some(serde_json::json!({"type": "object"}));
        let wire = serde_json::to_value(&spec).expect("serializes");
        assert_eq!(wire["name"], "postgres");
        assert_eq!(wire["version"], "1.2.3");
        assert_eq!(wire["config_schema"]["type"], "object");
        let back: ConnectorSpec = serde_json::from_value(wire).expect("round-trips");
        assert_eq!(back, spec);
    }
}

//! Connector configuration: parsing with field paths, and the published JSON Schema.

#[cfg(test)]
mod tests;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;

use crate::error::{ConnectorError, Result};

/// Deserializes a connector's configuration; errors name the offending field and carry code
/// `config_invalid`.
pub(crate) fn parse<C: DeserializeOwned>(config: serde_json::Value) -> Result<C> {
    serde_path_to_error::deserialize(config).map_err(|error| {
        let message = format!("config field {}: {}", error.path(), error.inner());
        ConnectorError::config(message).with_code("config_invalid")
    })
}

/// The JSON Schema of `C`.
pub(crate) fn schema<C: JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(C)).expect("JSON Schemas serialize to JSON")
}

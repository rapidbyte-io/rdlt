//! Connector configuration: parsing with field paths, and the published JSON Schema.

#[cfg(test)]
mod tests;

use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_path_to_error::Segment;

use crate::error::{ConnectorError, Result};

/// Bytes: bounds the field path a configuration error names.
const PATH_BYTES: usize = 256;

/// Deserializes a connector's configuration; errors name the offending field, say what is wrong
/// with it and what kind of value it holds, never the value, and carry code `config_invalid`.
pub(crate) fn parse<C: DeserializeOwned>(config: &serde_json::Value) -> Result<C> {
    serde_path_to_error::deserialize(config).map_err(|error| {
        let path = crate::text::shown(error.path(), PATH_BYTES);
        let fault = Fault::of(&error.inner().to_string());
        let message = match found(config, error.path()) {
            Some(kind) => format!("config field {path}: {fault} (it holds {kind})"),
            None => format!("config field {path}: {fault}"),
        };
        ConnectorError::config(message).with_code("config_invalid")
    })
}

/// What is wrong with a field, as the start of serde's message classes it: the rest of the
/// message quotes the value refused, and is never read.
struct Fault(&'static str, Option<String>);

impl Fault {
    fn of(message: &str) -> Self {
        let classes = [
            ("invalid type: ", "its value is of the wrong type"),
            (
                "invalid value: ",
                "its value is not one the connector accepts",
            ),
            ("invalid length ", "it holds the wrong number of items"),
            (
                "unknown variant ",
                "its value names no choice the connector knows",
            ),
            ("unknown field ", "the connector knows no such field"),
            ("missing field ", "it lacks a required field"),
            ("duplicate field ", "it is given twice"),
        ];
        let class = classes.iter().find(|(start, _)| message.starts_with(start));
        match class {
            // The field that is missing is the connector's own name for it: no value.
            Some((start @ "missing field ", said)) => Self(said, named(&message[start.len()..])),
            Some((_, said)) => Self(said, None),
            None => Self("its value is not valid", None),
        }
    }
}

impl std::fmt::Display for Fault {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.1 {
            Some(field) => write!(formatter, "{}, `{field}`", self.0),
            None => formatter.write_str(self.0),
        }
    }
}

/// The name between backticks that `rest` starts with, when it reads as a field's name.
fn named(rest: &str) -> Option<String> {
    let name = rest.strip_prefix('`')?.split('`').next()?;
    let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.');
    (!name.is_empty() && name.len() <= 64 && name.chars().all(plain)).then(|| name.to_owned())
}

/// The kind of value `config` holds at `path`, when the path leads to one.
fn found(config: &serde_json::Value, path: &serde_path_to_error::Path) -> Option<&'static str> {
    use serde_json::Value;
    let mut value = config;
    for segment in path {
        value = match segment {
            Segment::Seq { index } => value.get(index)?,
            Segment::Map { key } => value.get(key)?,
            // An enum's value is what its variant's name leads to, or the name itself.
            Segment::Enum { variant } => value.get(variant).unwrap_or(value),
            Segment::Unknown => return None,
        };
    }
    Some(match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "text",
        Value::Array(_) => "a list",
        Value::Object(_) => "an object",
    })
}

/// The JSON Schema of `C`.
pub(crate) fn schema<C: JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(C)).expect("JSON Schemas serialize to JSON")
}

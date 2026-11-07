//! How long a run reads: until its source is exhausted, forever, or for a while.

use std::borrow::Cow;
use std::time::Duration;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, Unexpected};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::read::Text;
use super::units::{self, SpanText};

/// How long a run reads: `exhausted`, `forever`, or a duration such as `10m`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Until {
    /// Until the source has caught up to where it stood when its read started.
    #[default]
    Exhausted,
    /// Following the source until the run is stopped.
    Forever,
    /// Following the source until this long after the run started.
    For(Duration),
}

impl Until {
    /// Whether the run reads until its source is exhausted, the default.
    pub fn is_exhausted(&self) -> bool {
        matches!(self, Self::Exhausted)
    }
}

impl<'de> Deserialize<'de> for Until {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let Text(text) = Text::deserialize(deserializer)?;
        match text.as_str() {
            "exhausted" => Ok(Self::Exhausted),
            "forever" => Ok(Self::Forever),
            text => units::span(text).map(Self::For).ok_or_else(|| {
                let expected = &"exhausted, forever or a duration such as 10m";
                de::Error::invalid_value(Unexpected::Other("text"), expected)
            }),
        }
    }
}

impl Serialize for Until {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Exhausted => serializer.serialize_str("exhausted"),
            Self::Forever => serializer.serialize_str("forever"),
            Self::For(duration) => serializer.serialize_str(&units::spanned(*duration)),
        }
    }
}

impl JsonSchema for Until {
    fn schema_name() -> Cow<'static, str> {
        "Until".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let span = generator.subschema_for::<SpanText>();
        json_schema!({
            "description": "How long a run reads.",
            "anyOf": [{ "enum": ["exhausted", "forever"] }, span]
        })
    }
}

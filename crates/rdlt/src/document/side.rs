//! A pipeline's two sides: which connector each is, its configuration, and what it may reach.

use std::borrow::Cow;
use std::fmt;
use std::path::PathBuf;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::read::{self, refuse};

/// A connector as one side of a pipeline: which one, its configuration, and what its placement
/// lets it reach.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Side {
    /// The connector: `connector: <id>` or `{ id, version, digest, endpoint }`.
    pub connector: Connector,
    /// Its configuration, inline; text values may refer to secrets the host allows.
    #[serde(default = "no_config", skip_serializing_if = "is_no_config")]
    pub config: serde_json::Value,
    /// What its sandbox may reach, within the roots the host allows.
    #[serde(
        default,
        deserialize_with = "read::mapping",
        skip_serializing_if = "Access::is_empty"
    )]
    pub access: Access,
}

fn no_config() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

fn is_no_config(config: &serde_json::Value) -> bool {
    config.as_object().is_some_and(serde_json::Map::is_empty)
}

impl Side {
    /// `connector`, with an empty configuration and no access.
    pub fn new(connector: Connector) -> Self {
        Self {
            connector,
            config: no_config(),
            access: Access::default(),
        }
    }

    /// The side, configured with `config`.
    #[must_use]
    pub fn config(mut self, config: serde_json::Value) -> Self {
        self.config = config;
        self
    }

    /// The side, granted `access`.
    #[must_use]
    pub fn access(mut self, access: Access) -> Self {
        self.access = access;
        self
    }
}

/// What a connector's sandbox may reach beyond what the host gives every connector.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Access {
    /// Paths it may read, each absolute and within a root the host made readable or writable.
    #[serde(
        default,
        deserialize_with = "read::paths",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub read: Vec<PathBuf>,
    /// Paths it may read and write, each absolute and within a root the host made writable.
    #[serde(
        default,
        deserialize_with = "read::paths",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub write: Vec<PathBuf>,
    /// Whether it shares the host's network.
    #[serde(
        default,
        deserialize_with = "read::flag",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub network: bool,
}

impl Access {
    /// Nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether nothing is asked for.
    pub fn is_empty(&self) -> bool {
        self.read.is_empty() && self.write.is_empty() && !self.network
    }

    /// The access, reading `path` too.
    #[must_use]
    pub fn read(mut self, path: impl Into<PathBuf>) -> Self {
        self.read.push(path.into());
        self
    }

    /// The access, reading and writing `path` too.
    #[must_use]
    pub fn write(mut self, path: impl Into<PathBuf>) -> Self {
        self.write.push(path.into());
        self
    }

    /// The access, sharing the host's network.
    #[must_use]
    pub fn network(mut self) -> Self {
        self.network = true;
        self
    }
}

/// A connector: its id alone, placed wherever the host finds it, or its id with what its
/// placement must honour.
///
/// ```
/// use rdlt::Connector;
///
/// let far = Connector::new("io.rapidbyte.sqlite").endpoint("grpcs://db.example:7443");
/// assert_eq!(far.id(), "io.rapidbyte.sqlite");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Connector {
    /// `connector: <id>`.
    Id(String),
    /// `connector: { id, version, digest, endpoint }`.
    Reference(ConnectorReference),
}

/// A connector's id with what its placement must honour: the versions it accepts, the binary it
/// must be, or where it listens.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ConnectorReference {
    /// The connector's id.
    #[serde(deserialize_with = "read::text")]
    pub id: String,
    /// The versions accepted, a semver requirement such as `^1.2`.
    #[serde(
        default,
        deserialize_with = "read::optional_text",
        skip_serializing_if = "Option::is_none"
    )]
    pub version: Option<String>,
    /// The SHA-256 of the binary that must run, in hex, for a local process.
    #[serde(
        default,
        deserialize_with = "read::optional_text",
        skip_serializing_if = "Option::is_none"
    )]
    pub digest: Option<String>,
    /// Where it listens, `grpcs://host:port`, for a remote connector.
    #[serde(
        default,
        deserialize_with = "read::optional_text",
        skip_serializing_if = "Option::is_none"
    )]
    pub endpoint: Option<String>,
}

impl Connector {
    /// The connector `id`, wherever the host finds it.
    pub fn new(id: impl Into<String>) -> Self {
        Self::Id(id.into())
    }

    /// The connector's id.
    pub fn id(&self) -> &str {
        match self {
            Self::Id(id) => id,
            Self::Reference(reference) => &reference.id,
        }
    }

    /// The connector, accepting only the versions `requirement` matches.
    #[must_use]
    pub fn version(self, requirement: impl Into<String>) -> Self {
        self.referenced(|reference| reference.version = Some(requirement.into()))
    }

    /// The connector, accepting only a binary of SHA-256 `digest`, in hex.
    #[must_use]
    pub fn digest(self, digest: impl Into<String>) -> Self {
        self.referenced(|reference| reference.digest = Some(digest.into()))
    }

    /// The connector, listening at `endpoint`, `grpcs://host:port`.
    #[must_use]
    pub fn endpoint(self, endpoint: impl Into<String>) -> Self {
        self.referenced(|reference| reference.endpoint = Some(endpoint.into()))
    }

    fn referenced(self, change: impl FnOnce(&mut ConnectorReference)) -> Self {
        let mut reference = match self {
            Self::Id(id) => ConnectorReference {
                id,
                version: None,
                digest: None,
                endpoint: None,
            },
            Self::Reference(reference) => reference,
        };
        change(&mut reference);
        Self::Reference(reference)
    }
}

impl<'de> Deserialize<'de> for Connector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Reading;

        impl<'de> Visitor<'de> for Reading {
            type Value = Connector;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a connector's id, or { id, version, digest, endpoint }")
            }

            fn visit_str<E: de::Error>(self, id: &str) -> Result<Connector, E> {
                Ok(Connector::Id(id.to_owned()))
            }

            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Connector, M::Error> {
                // Its keys are checked as `ConnectorReference`'s, so a misspelt one is named.
                let keyed = de::value::MapAccessDeserializer::new(map);
                ConnectorReference::deserialize(keyed).map(Connector::Reference)
            }

            refuse!(bool int fraction);
        }

        deserializer.deserialize_any(Reading)
    }
}

impl Serialize for Connector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Id(id) => serializer.serialize_str(id),
            Self::Reference(reference) => reference.serialize(serializer),
        }
    }
}

impl JsonSchema for Connector {
    fn schema_name() -> Cow<'static, str> {
        "Connector".into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let reference = generator.subschema_for::<ConnectorReference>();
        json_schema!({
            "description": "A connector's id, or its id with what its placement must honour.",
            "anyOf": [{ "type": "string" }, reference]
        })
    }
}

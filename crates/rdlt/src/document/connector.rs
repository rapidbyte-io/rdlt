//! The connector requirement every arm names: the `connector:` document,
//! the ONE rich-spelling ↔ id table with the roles each spelling may
//! fill, and the role-aware arm deserializer that turns either form into
//! a [`Connector`] at parse.

use std::fmt;
use std::path::PathBuf;

use rdlt_connector_client::handshake::Requirement;
use serde::Deserialize;
use serde::de::{self, Deserializer, MapAccess, Visitor};

use super::model::Config;

/// The side of a pipeline an arm fills.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Source,
    Destination,
}

impl Role {
    fn name(self) -> &'static str {
        match self {
            Role::Source => "source",
            Role::Destination => "destination",
        }
    }

    /// The rich spellings this role accepts, table order, spelled for a
    /// refusal message.
    fn accepted(self) -> String {
        TABLE
            .iter()
            .filter(|row| row.roles.contains(&self))
            .map(|row| format!("`{}`", row.spelling))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// One row of the table: a rich spelling, the reverse-DNS id it desugars
/// to, and the roles it may fill.
struct Row {
    spelling: &'static str,
    id: &'static str,
    roles: &'static [Role],
}

/// The ONE desugar table: rich spelling ↔ reverse-DNS connector id, with
/// the roles each spelling fills. The document arms resolve through it,
/// and `rdlt schema`'s short names map through [`id`] over the same rows
/// — so the document language and the CLI cannot drift apart.
const TABLE: &[Row] = &[
    Row {
        spelling: "rest",
        id: "io.rapidbyte.rest",
        roles: &[Role::Source],
    },
    Row {
        spelling: "oracle",
        id: "io.rapidbyte.oracle",
        roles: &[Role::Source],
    },
    Row {
        spelling: "file",
        id: "io.rapidbyte.file",
        roles: &[Role::Source, Role::Destination],
    },
    Row {
        spelling: "postgres",
        id: "io.rapidbyte.postgres",
        roles: &[Role::Source, Role::Destination],
    },
    Row {
        spelling: "duckdb",
        id: "io.rapidbyte.duckdb",
        roles: &[Role::Destination],
    },
    Row {
        spelling: "iceberg",
        id: "io.rapidbyte.iceberg",
        roles: &[Role::Destination],
    },
    Row {
        spelling: "snowflake",
        id: "io.rapidbyte.snowflake",
        roles: &[Role::Destination],
    },
];

/// The reverse-DNS connector id a rich spelling resolves to, in either
/// role — `None` for anything outside the table (such a value is
/// already an id or a binary path, not a short name).
pub fn id(spelling: &str) -> Option<&'static str> {
    TABLE
        .iter()
        .find(|row| row.spelling == spelling)
        .map(|row| row.id)
}

/// Deserialize a `source:` arm.
pub(super) fn source_arm<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Connector, D::Error> {
    deserializer.deserialize_map(Arm(Role::Source))
}

/// Deserialize a `destination:` arm.
pub(super) fn destination_arm<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Connector, D::Error> {
    deserializer.deserialize_map(Arm(Role::Destination))
}

/// One arm of the document: a single-key map. `connector:` is the
/// explicit form, read as written; any other key is a rich spelling
/// looked up in the table for this role — the table's id, no version
/// pin, no path override, the value as the config. Anything else refuses
/// at parse naming the spelling and the accepted set.
struct Arm(Role);

impl<'de> Visitor<'de> for Arm {
    type Value = Connector;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a {} arm: a single-key map, `connector:` or one of {}",
            self.0.name(),
            self.0.accepted()
        )
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Connector, A::Error> {
        let role = self.0;
        let Some(spelling) = map.next_key::<String>()? else {
            return Err(de::Error::custom(format!(
                "no connector: a {} arm is a single-key map, `connector:` or one of {}",
                role.name(),
                role.accepted()
            )));
        };
        let connector = if spelling == "connector" {
            map.next_value::<Connector>()?
        } else {
            let Some(row) = TABLE.iter().find(|row| row.spelling == spelling) else {
                return Err(de::Error::custom(format!(
                    "unknown spelling `{spelling}`: a {} arm names `connector:` or one of {}",
                    role.name(),
                    role.accepted()
                )));
            };
            if !row.roles.contains(&role) {
                return Err(de::Error::custom(format!(
                    "`{spelling}` is not a {}: a {} arm names `connector:` or one of {}",
                    role.name(),
                    role.name(),
                    role.accepted()
                )));
            }
            Connector {
                id: row.id.to_owned(),
                version: None,
                path: None,
                config: map.next_value::<Config>()?,
            }
        };
        if let Some(extra) = map.next_key::<String>()? {
            return Err(de::Error::custom(format!(
                "two connectors, `{spelling}` and `{extra}`: a {} arm is a single-key map, \
                 `connector:` or one of {}",
                role.name(),
                role.accepted()
            )));
        }
        Ok(connector)
    }
}

/// An out-of-process connector requirement, the `connector:` document:
///
/// ```yaml
/// source:
///   connector:
///     id: io.rapidbyte.file
///     version: "0.3.0"      # optional, exact-match
///     path: /explicit/bin   # optional override
///     config: { ... }       # the connector's own document, opaque here
/// ```
///
/// `config` also takes the path form (`config: source.yaml`) — the
/// same [`Config`] rule as every rich spelling's value. A rich spelling
/// (`file: {…}`) parses to this same shape: the table's id, no version
/// pin, no path override.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connector {
    /// The connector id, spelled reverse-DNS: `io.rapidbyte.file`. Two
    /// things hang off it: the id's LAST `.`-segment names the binary
    /// discovery looks for (`rdlt-connector-file` on PATH), and the
    /// spawned connector must report EXACTLY this id in its handshake.
    /// A shorthand like `id: file` would therefore discover the same
    /// binary and then be REFUSED as an identity mismatch — the full
    /// reverse-DNS spelling is the id, not a long form of it.
    pub id: String,
    /// Pin the connector's version, exact-match against what its
    /// handshake reports. Absent accepts any.
    #[serde(default)]
    pub version: Option<String>,
    /// Explicit binary path, bypassing PATH discovery entirely.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// The connector's OWN config document — inline, or a path to one.
    /// Opaque here: it crosses the wire in the handshake and the
    /// CONNECTOR's config gate validates it.
    pub config: Config,
}

/// Manual on purpose (the workspace lint demands SOME Debug): the
/// `config` document is the connector's own vocabulary and routinely
/// carries credentials — a derived Debug would print them into any
/// `{:?}` of a `Document`, a log line, or a test failure message. The
/// other fields render normally; the config renders as `<elided>`.
impl fmt::Debug for Connector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connector")
            .field("id", &self.id)
            .field("version", &self.version)
            .field("path", &self.path)
            .field("config", &"<elided>")
            .finish()
    }
}

impl Connector {
    /// The provider-facing half of the document — everything except the
    /// config, which travels beside it.
    pub(super) fn requirement(&self) -> Requirement {
        let mut requirement = Requirement::new(&self.id);
        if let Some(version) = &self.version {
            requirement = requirement.with_version(version);
        }
        if let Some(path) = &self.path {
            requirement = requirement.with_path(path);
        }
        requirement
    }
}

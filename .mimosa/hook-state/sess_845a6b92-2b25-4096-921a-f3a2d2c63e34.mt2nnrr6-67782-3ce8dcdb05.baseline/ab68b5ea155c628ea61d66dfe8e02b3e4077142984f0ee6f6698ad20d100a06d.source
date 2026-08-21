//! The connector requirement every arm names — the `connector:` document,
//! the ONE arm form — and the arm deserializer that admits nothing else.

use std::fmt;
use std::path::PathBuf;

use rdlt_connector_client::handshake::Requirement;
use serde::Deserialize;
use serde::de::{self, Deserializer, MapAccess, Visitor};

use super::model::Config;

/// Deserialize one arm of the document: a map with exactly the key
/// `connector`, whose value is the [`Connector`] document. Anything else
/// — another key, two keys, no key — refuses at parse naming the one
/// accepted form.
pub(super) fn arm<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Connector, D::Error> {
    deserializer.deserialize_map(Arm)
}

/// The one accepted arm form, spelled for every refusal.
const FORM: &str = "an arm is `connector: {id, config, …}` (version and path optional)";

struct Arm;

impl<'de> Visitor<'de> for Arm {
    type Value = Connector;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a map with the one key `connector`")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Connector, A::Error> {
        let Some(key) = map.next_key::<String>()? else {
            return Err(de::Error::custom(format!("no connector: {FORM}")));
        };
        if key != "connector" {
            return Err(de::Error::custom(format!("unknown key `{key}`: {FORM}")));
        }
        let connector = map.next_value::<Connector>()?;
        if let Some(extra) = map.next_key::<String>()? {
            return Err(de::Error::custom(format!(
                "two keys, `connector` and `{extra}`: {FORM}"
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
/// `config` also takes the path form (`config: source.yaml`), read at
/// build time relative to the document's directory. The facade knows no
/// connector by name: `id` is whatever binary the runtime discovers by
/// its last segment (or `path` names outright), and the handshake holds
/// the spawned connector to it.
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
    pub(crate) fn requirement(&self) -> Requirement {
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

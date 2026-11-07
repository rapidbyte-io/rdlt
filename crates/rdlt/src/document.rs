//! A pipeline's document: one model, which a builder builds and a file holds.

mod modes;
mod policies;
mod read;
mod side;
mod stream;
#[cfg(test)]
pub(crate) mod tests;
mod transform;
mod units;
mod until;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use modes::{
    DeleteMode, Hint, Nested, OnTruncate, OnUnsupported, ReadMode, RetentionLoss, SchemaPolicy,
    WriteMode,
};
pub use policies::{Commit, Resources, Retry};
pub use side::{Access, Connector, ConnectorReference, Side};
pub use stream::{Column, Defaults, SchemaRules, Stream};
pub use transform::{MaskStep, MaskWith, TransformStep};
pub use until::Until;

/// A pipeline as its author describes it: one source, one destination, the streams it selects,
/// and how a run commits, uses resources, retries and reads.
///
/// ```
/// use rdlt::{Connector, Document, Side, Stream};
///
/// let source = Side::new(Connector::new("io.rapidbyte.memory"))
///     .config(rdlt::json!({ "streams": { "users": [{ "id": 1 }] } }));
/// let destination = Side::new(Connector::new("io.rapidbyte.memory"))
///     .config(rdlt::json!({ "store": "users" }));
/// let mut document = Document::new("users", source, destination);
/// document.streams.push(Stream::new("users"));
/// let written = serde_json::to_value(&document)?;
/// assert_eq!(serde_json::from_value::<Document>(written)?, document);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Document {
    /// The document's format: 1, the only one.
    #[serde(deserialize_with = "read::count")]
    pub version: u8,
    /// The pipeline's name.
    #[serde(deserialize_with = "read::text")]
    pub pipeline: String,
    /// Where rows come from.
    #[serde(deserialize_with = "read::mapping")]
    pub source: Side,
    /// Where rows go.
    #[serde(deserialize_with = "read::mapping")]
    pub destination: Side,
    /// What every stream takes where it says nothing.
    #[serde(
        default,
        deserialize_with = "read::mapping",
        skip_serializing_if = "Defaults::is_empty"
    )]
    pub defaults: Defaults,
    /// The streams loaded; a name holding `*` or `?` selects each stream of the source's catalog
    /// it matches, and a stream named outright wins over a pattern that matches it too.
    #[serde(deserialize_with = "read::mappings")]
    pub streams: Vec<Stream>,
    /// When a run commits; the engine's default where it says nothing.
    #[serde(
        default,
        deserialize_with = "read::optional_mapping",
        skip_serializing_if = "Option::is_none"
    )]
    pub commit: Option<Commit>,
    /// What a run may use.
    #[serde(
        default,
        deserialize_with = "read::mapping",
        skip_serializing_if = "Resources::is_empty"
    )]
    pub resources: Resources,
    /// How a run retries; the engine's default where it says nothing.
    #[serde(
        default,
        deserialize_with = "read::optional_mapping",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry: Option<Retry>,
    /// How long a run reads.
    #[serde(default, skip_serializing_if = "Until::is_exhausted")]
    pub until: Until,
}

impl Document {
    /// Pipeline `pipeline` from `source` to `destination`, selecting no stream yet.
    pub fn new(pipeline: impl Into<String>, source: Side, destination: Side) -> Self {
        Self {
            version: 1,
            pipeline: pipeline.into(),
            source,
            destination,
            defaults: Defaults::default(),
            streams: Vec::new(),
            commit: None,
            resources: Resources::default(),
            retry: None,
            until: Until::default(),
        }
    }

    /// The JSON Schema every document satisfies, for editors and tools that check one.
    pub fn json_schema() -> serde_json::Value {
        schemars::schema_for!(Self).to_value()
    }
}

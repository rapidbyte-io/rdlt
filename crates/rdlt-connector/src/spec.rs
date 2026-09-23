//! What a connector declares about itself, and what it receives when it connects.

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::id::ConnectorId;

/// A boxed, sendable future: the return type of the engine-facing connector traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Which side of a pipeline a connector serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Reads streams.
    Source,
    /// Writes tables.
    Destination,
}

/// A connector's identity and the JSON Schema of its configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectorSpec {
    /// The connector's id.
    pub id: ConnectorId,
    /// The connector's version.
    pub version: String,
    /// The side it serves.
    pub role: Role,
    /// The JSON Schema its configuration must satisfy.
    pub config_schema: serde_json::Value,
}

/// What the host tells a connector when it connects.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct ConnectContext {}

impl ConnectContext {
    /// A context with nothing to report.
    pub fn new() -> Self {
        Self::default()
    }
}

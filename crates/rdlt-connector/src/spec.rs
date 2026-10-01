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

/// What a connector is told when it connects.
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct ConnectContext {
    host: Option<std::sync::Arc<str>>,
}

impl ConnectContext {
    /// A context with nothing to report.
    pub fn new() -> Self {
        Self::default()
    }

    /// The context of a connector serving the host named `host`.
    pub fn serving(host: impl Into<std::sync::Arc<str>>) -> Self {
        Self {
            host: Some(host.into()),
        }
    }

    /// The host the connector serves, as a listening connector accepted it: the name in the
    /// host's certificate that was named to the connector.
    ///
    /// None in the host's own process, and for a connector its host spawned. What a connector
    /// keeps in its own process, it keeps apart for each host.
    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }
}

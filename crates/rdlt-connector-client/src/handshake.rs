//! The client half of the wire handshake: send the one `Handshake`
//! RPC, verify the connector's reported identity against what the
//! provider resolved (D-039-2), and decode the reply's payloads into
//! SPI vocabulary.

use std::collections::BTreeMap;
use std::path::PathBuf;

use rdlt_connector::{ConnectorSpec, DestinationCapabilities};
use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::proto::{self, handshake_reply};
use tonic::transport::Channel;

use crate::dial::connector_client;
use crate::error::ClientError;

/// Which half of the SPI the handshake asks the connector to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// `expected_role: "source"`.
    Source,
    /// `expected_role: "destination"`.
    Destination,
}

impl Role {
    /// The frozen wire spelling — the proto's `expected_role` values.
    fn wire_name(self) -> &'static str {
        match self {
            Role::Source => "source",
            Role::Destination => "destination",
        }
    }
}

/// What the provider requires of a connector before trusting it: the
/// id it resolved, optionally a pinned version, optionally the
/// executable path it resolved the id to. Defined HERE and re-exported
/// by `rdlt-runtime` — the CLIENT verifies (this module's handshake
/// checks id/version against the reply), the RUNTIME resolves (turning
/// an id into a spawnable path is the provider's job, so `path` rides
/// along for it without this crate reading it).
///
/// `#[non_exhaustive]`: requirements can grow (a checksum, a signature)
/// without breaking constructors — build with [`ConnectorRequirement::new`]
/// plus the `with_*` declarations.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnectorRequirement {
    /// The connector id the handshake's reported identity must equal.
    pub id: String,
    /// A pinned connector version; `None` accepts any.
    pub version: Option<String>,
    /// Where the runtime resolved the connector executable — carried
    /// for the spawner, never read by the handshake.
    pub path: Option<PathBuf>,
}

impl ConnectorRequirement {
    /// Require a connector by id alone.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
            path: None,
        }
    }

    /// Pin the exact connector version the handshake must report.
    #[must_use = "with_version returns the requirement; it does not mutate in place"]
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Record the resolved executable path for the spawner.
    #[must_use = "with_path returns the requirement; it does not mutate in place"]
    pub fn with_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }
}

/// Everything a verified handshake established.
#[derive(Debug, Clone)]
pub struct HandshakeOutcome {
    /// The connector's self-description, decoded from `spec_json`.
    pub spec: ConnectorSpec,
    /// The destination's declared capabilities — `None` for a source
    /// (the proto pins `capabilities_json` empty for sources).
    pub capabilities: Option<DestinationCapabilities>,
    /// Per-state-kind format versions (e.g. `"cursor" -> 2`). v0
    /// servers send an empty map; populated when adapters negotiate
    /// resume.
    pub state_format_versions: BTreeMap<String, u32>,
    /// The protocol version both sides settled on — the one this client
    /// asked for, since the server accepting the request IS the
    /// agreement.
    pub negotiated_protocol: u32,
}

/// Run the handshake on `channel` for `role`, carrying `config`, and
/// verify the reply against `expected` (D-039-2: id must match, and
/// version must match when the requirement pins one) — identity is
/// checked BEFORE any payload is decoded, so a wrong connector is
/// reported as the mismatch it is rather than as whatever decode
/// failure its foreign payloads happen to produce.
pub async fn handshake(
    channel: &Channel,
    role: Role,
    config: &serde_json::Value,
    expected: &ConnectorRequirement,
) -> Result<HandshakeOutcome, ClientError> {
    let mut client = connector_client(channel.clone());
    let reply = client
        .handshake(proto::HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: role.wire_name().to_string(),
            config_json: serde_json::to_vec(config)
                .expect("a serde_json::Value serializes to JSON infallibly"),
        })
        .await
        .map_err(ClientError::Transport)?
        .into_inner();

    let ok = match reply.outcome {
        Some(handshake_reply::Outcome::Ok(ok)) => ok,
        Some(handshake_reply::Outcome::Error(frame)) => {
            return Err(ClientError::handshake_refusal(&frame));
        }
        None => {
            return Err(ClientError::Protocol(
                "the handshake reply carried no outcome".to_string(),
            ));
        }
    };

    if ok.connector_id != expected.id {
        return Err(ClientError::IdMismatch {
            expected: expected.id.clone(),
            reported: ok.connector_id,
        });
    }
    if let Some(required) = &expected.version
        && *required != ok.connector_version
    {
        return Err(ClientError::VersionMismatch {
            required: required.clone(),
            reported: ok.connector_version,
        });
    }

    let spec: ConnectorSpec = serde_json::from_slice(&ok.spec_json).map_err(|error| {
        ClientError::Protocol(format!(
            "undecodable spec_json in the handshake reply: {error}"
        ))
    })?;
    // Empty means "a source" per the proto field's own doc — only a
    // non-empty payload claims to be a capabilities document.
    let capabilities: Option<DestinationCapabilities> = if ok.capabilities_json.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice(&ok.capabilities_json).map_err(|error| {
                ClientError::Protocol(format!(
                    "undecodable capabilities_json in the handshake reply: {error}"
                ))
            })?,
        )
    };

    Ok(HandshakeOutcome {
        spec,
        capabilities,
        // prost generates a HashMap; the outcome holds a BTreeMap so an
        // embedder iterating it (logs, reports) sees a stable order.
        state_format_versions: ok.state_format_versions.into_iter().collect(),
        negotiated_protocol: PROTOCOL_VERSION,
    })
}

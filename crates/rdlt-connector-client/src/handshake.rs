//! The client half of the wire handshake: send the one `Handshake`
//! RPC, verify the connector's reported identity against what the
//! provider resolved, and decode the reply's payloads into SPI
//! vocabulary.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rdlt_connector::{ConnectorSpec, DestinationCapabilities};
use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::proto::{self, handshake_reply};
use tonic::transport::Channel;

use crate::{error, gate, wire};

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
/// by `rdlt-runtime` — the CLIENT verifies (this module's [`run`]
/// checks id/version against the reply), the RUNTIME resolves (turning
/// an id into a spawnable path is the provider's job, so `path` rides
/// along for it without this crate reading it).
///
/// `#[non_exhaustive]`: requirements can grow (a checksum, a signature)
/// without breaking constructors — build with [`Requirement::new`]
/// plus the `with_*` declarations.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Requirement {
    /// The connector id the handshake's reported identity must equal.
    pub id: String,
    /// A pinned connector version; `None` accepts any.
    pub version: Option<String>,
    /// Where the runtime resolved the connector executable — carried
    /// for the spawner, never read by the handshake.
    pub path: Option<PathBuf>,
    /// The liveness half of the requirement: how long any single wire
    /// await on this connector may stay silent before it fails as the
    /// typed [`error::Error::Timeout`]. Defaults to
    /// [`wire::DEFAULT_DEADLINE`]; it bounds the quiet interval of each
    /// await, never a whole stream (see the constant's doc).
    pub rpc_deadline: Duration,
}

impl Requirement {
    /// Require a connector by id alone.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
            path: None,
            rpc_deadline: wire::DEFAULT_DEADLINE,
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

    /// Override the per-await RPC deadline — for embedders whose
    /// connectors legitimately think longer than
    /// [`wire::DEFAULT_DEADLINE`] between answers (or tests that want a
    /// tight one). The deadline bounds each quiet interval, so a
    /// longer stream needs no longer deadline — only a longer SILENCE
    /// does.
    #[must_use = "with_rpc_deadline returns the requirement; it does not mutate in place"]
    pub fn with_rpc_deadline(mut self, deadline: Duration) -> Self {
        self.rpc_deadline = deadline;
        self
    }
}

/// Everything a verified handshake established.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// The identity the wire actually reported, VERIFIED at handshake
    /// against the requirement — as distinct from the unverified
    /// `spec` decode.
    pub connector_id: String,
    /// The connector version the WIRE reported — checked against the
    /// requirement only when it pins a version; with no pin it is
    /// carried as reported. Spec-vs-wire skew (`spec.version`
    /// disagreeing with this value) is not refused here — the
    /// certifier judges identity agreement separately. Either way it
    /// is distinct from the unverified `spec` decode.
    pub connector_version: String,
    /// The connector's self-description, decoded from `spec_json`.
    pub spec: ConnectorSpec,
    /// The destination's declared capabilities — `None` for a source
    /// (the proto pins `capabilities_json` empty for sources).
    pub capabilities: Option<DestinationCapabilities>,
    /// Per-state-kind format versions (e.g. `"cursor" -> 2`). v0
    /// servers send an empty map, carried through to embedders unread;
    /// negotiation is owned by the feature that adds a second format
    /// version.
    pub state_format_versions: BTreeMap<String, u32>,
    /// The protocol version both sides settled on — the one this client
    /// asked for, since the server accepting the request IS the
    /// agreement.
    pub protocol_version: u32,
}

/// Run the handshake on `channel` for `role`, carrying `config`, and
/// verify the reply against `requirement` (the id must match, and the
/// version must match when the requirement pins one) — identity is
/// checked BEFORE any payload is decoded, so a wrong connector is
/// reported as the mismatch it is rather than as whatever decode
/// failure its foreign payloads happen to produce.
///
/// The identity check is a SANITY CHECK, not authentication: it
/// compares what the binary REPORTS against what was asked for, which
/// catches an accidentally-wrong binary (a rename, a stale PATH entry)
/// and never a malicious one — a hostile binary simply reports the
/// required id. Operators who cannot trust PATH discovery pin the
/// binary itself: the pipeline document's `path:` override, or a
/// provider built `with_search_path` restricted to a directory they
/// control. A digest/signature pin is the anticipated future growth of
/// [`Requirement`] for stronger needs.
pub async fn run(
    channel: &Channel,
    role: Role,
    config: &serde_json::Value,
    requirement: &Requirement,
) -> Result<Outcome, error::Error> {
    // The document ceiling enforced before SEND: the serve side refuses
    // an oversized `config_json` after receiving it, but the host's own
    // cap applies to the YAML FILE — and YAML→JSON re-serialization
    // inflates (unicode escapes, quoting), so a just-legal file can
    // cross the wire's ceiling. Refusing here turns the connector's
    // typed post-receive refusal into a host-side error that names what
    // actually happened.
    let config_json =
        serde_json::to_vec(config).expect("a serde_json::Value serializes to JSON infallibly");
    if config_json.len() as u64 > rdlt_connector::MAX_DOCUMENT_BYTES {
        return Err(error::Error::Protocol(format!(
            "the config document serializes to {} bytes of JSON, over the protocol's \
             {}-byte document ceiling (YAML→JSON re-serialization can inflate a \
             just-legal source file past it) — shrink the document",
            config_json.len(),
            rdlt_connector::MAX_DOCUMENT_BYTES
        )));
    }
    let mut client = wire::connector_client(channel.clone());
    let reply = wire::bounded(
        requirement.rpc_deadline,
        wire::Operation::Handshake,
        client.handshake(proto::HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            expected_role: role.wire_name().to_string(),
            config_json,
        }),
    )
    .await?
    .map_err(error::Error::Transport)?
    .into_inner();

    let ok = match reply.outcome {
        Some(handshake_reply::Outcome::Ok(ok)) => ok,
        Some(handshake_reply::Outcome::Error(frame)) => {
            return Err(error::Error::handshake_refusal(&frame));
        }
        None => {
            return Err(error::Error::Protocol(
                "the handshake reply carried no outcome".to_string(),
            ));
        }
    };

    // The identifier gate runs BEFORE the equality checks below: the
    // mismatch refusals quote the reported values, so a hostile
    // id/version must be refused inert before any message can carry it.
    gate::identifier("connector_id", &ok.connector_id).map_err(error::Error::Protocol)?;
    gate::identifier("connector_version", &ok.connector_version).map_err(error::Error::Protocol)?;

    if ok.connector_id != requirement.id {
        return Err(error::Error::IdentityMismatch {
            expected: requirement.id.clone(),
            reported: ok.connector_id,
        });
    }
    if let Some(required) = &requirement.version
        && *required != ok.connector_version
    {
        return Err(error::Error::VersionMismatch {
            required: required.clone(),
            reported: ok.connector_version,
        });
    }

    // The spec is a typed shell around one UNTYPED value —
    // `config_schema` is a free-form `serde_json::Value` that the host
    // caches for the session's lifetime — so the document ceiling every
    // untyped parse runs applies here too, on the RAW bytes before the
    // parse whose materialization it bounds. A hand-authored config
    // schema measures in kilobytes; a multi-megabyte one embedded data.
    gate::document("spec_json", &ok.spec_json).map_err(error::Error::Protocol)?;
    let spec: ConnectorSpec = serde_json::from_slice(&ok.spec_json).map_err(|error| {
        error::Error::Protocol(format!(
            "undecodable spec_json in the handshake reply: {}",
            rdlt_connector::json::describe_parse_error(&error)
        ))
    })?;
    // The spec's own name/version are identifiers too — they travel
    // into logs, reports and the certifier's identity-agreement
    // judgment — and ride the same rule as the wire-reported pair.
    gate::identifier("spec name", &spec.name).map_err(error::Error::Protocol)?;
    gate::identifier("spec version", &spec.version).map_err(error::Error::Protocol)?;
    // A count cap beside the content gates — a state-format map of
    // millions of keys passes every content gate within the frame cap
    // otherwise. v0 servers send an empty map; 64 kinds is far past any
    // honest negotiation.
    const MAX_STATE_FORMAT_KINDS: usize = 64;
    gate::count(
        "state-format kinds",
        ok.state_format_versions.len(),
        MAX_STATE_FORMAT_KINDS,
    )
    .map_err(error::Error::Protocol)?;
    for state_format_name in ok.state_format_versions.keys() {
        gate::identifier("state format name", state_format_name).map_err(error::Error::Protocol)?;
    }
    // Empty means "a source" per the proto field's own doc — only a
    // non-empty payload claims to be a capabilities document.
    let capabilities: Option<DestinationCapabilities> = if ok.capabilities_json.is_empty() {
        None
    } else {
        let capabilities: DestinationCapabilities = serde_json::from_slice(&ok.capabilities_json)
            .map_err(|error| {
            error::Error::Protocol(format!(
                "undecodable capabilities_json in the handshake reply: {}",
                rdlt_connector::json::describe_parse_error(&error)
            ))
        })?;
        // The declared `ident_rules.max_len` is untrusted wire input
        // and drives the engine's naming probe loop — validate it HERE,
        // at the trust boundary, so an exhaustible bound can never
        // reach the namer's release-active assert.
        capabilities.ident_rules.validate().map_err(|reason| {
            error::Error::Protocol(format!(
                "the connector's declared identifier rules are out of range: {reason}"
            ))
        })?;
        Some(capabilities)
    };

    Ok(Outcome {
        // The values the identity checks above verified — carried so
        // consumers read what the wire reported, never a re-derivation
        // from the unverified spec payload.
        connector_id: ok.connector_id,
        connector_version: ok.connector_version,
        spec,
        capabilities,
        // prost generates a HashMap; the outcome holds a BTreeMap so an
        // embedder iterating it (logs, reports) sees a stable order.
        state_format_versions: ok.state_format_versions.into_iter().collect(),
        protocol_version: PROTOCOL_VERSION,
    })
}

/// Dial and verify in one motion — the shared first half of both
/// adapters' `connect`.
pub(crate) async fn establish(
    socket_path: &Path,
    budget_bytes: u64,
    config: &serde_json::Value,
    role: Role,
    requirement: &Requirement,
) -> Result<(Channel, Outcome), error::Error> {
    let channel = wire::dial(socket_path, budget_bytes, requirement.rpc_deadline).await?;
    let outcome = run(&channel, role, config, requirement).await?;
    Ok((channel, outcome))
}

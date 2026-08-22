//! The client half of the wire handshake: send the one `Handshake`
//! RPC, verify the connector's reported identity against what the
//! provider resolved, and decode the reply's payloads into SPI
//! vocabulary.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use rdlt_connector::destination::Capabilities;

use rdlt_connector::spec::ConnectorSpec;
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

/// A decoded `ConnectorSpec`'s own name and version, through the
/// identifier rule the wire-reported pair rides: they travel into
/// logs, reports and the certifier's identity-agreement judgment. One
/// seat for the handshake's spec and the config-free `Spec` probe's.
pub fn gate_spec(spec: &ConnectorSpec) -> Result<(), error::Error> {
    gate::identifier("spec name", &spec.name).map_err(error::Error::Protocol)?;
    gate::identifier("spec version", &spec.version).map_err(error::Error::Protocol)?;
    Ok(())
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

    /// Hold the requirement's own text to the identifier rule a
    /// connector's reported identity is held to — the same ceiling and
    /// the same character inventory, plus the id's grammar: a
    /// reverse-DNS name of non-empty ASCII segments (`io.rapidbyte.file`)
    /// whose last segment names the binary discovery looks for. The
    /// id and version are document-authored, render in every provider
    /// error naming the connector, and the id's last segment becomes a
    /// filename; none of that tolerates a control character, a path
    /// separator, or a multi-megabyte name. Checked once, at the seat
    /// every spawn goes through, so no provider has to remember to.
    /// With an explicit `path` the path IS the identity and the id may
    /// be empty (the certifier's path form); whatever id is given
    /// still rides the identifier rule, since it renders.
    pub fn validate(&self) -> Result<(), error::Error> {
        gate::identifier("connector id", &self.id).map_err(error::Error::Requirement)?;
        if self.path.is_some() && self.id.is_empty() {
            return self.validate_version();
        }
        let well_formed = !self.id.is_empty()
            && self.id.split('.').all(|segment| {
                !segment.is_empty()
                    && segment
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            });
        if !well_formed {
            return Err(error::Error::Requirement(format!(
                "connector id `{}` is not a reverse-DNS name — dot-separated, non-empty \
                 segments of ASCII letters, digits, `-` and `_`",
                gate::escape(&self.id)
            )));
        }
        self.validate_version()
    }

    fn validate_version(&self) -> Result<(), error::Error> {
        if let Some(version) = &self.version {
            gate::identifier("connector version", version).map_err(error::Error::Requirement)?;
        }
        Ok(())
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
    pub capabilities: Option<Capabilities>,
    /// Per-state-kind format versions (e.g. `"cursor" -> 2`). Today's
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
    if config_json.len() as u64 > rdlt_connector::gate::MAX_DOCUMENT_BYTES {
        return Err(error::Error::Protocol(format!(
            "the config document serializes to {} bytes of JSON, over the protocol's \
             {}-byte document ceiling (YAML→JSON re-serialization can inflate a \
             just-legal source file past it) — shrink the document",
            config_json.len(),
            rdlt_connector::gate::MAX_DOCUMENT_BYTES
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
    .map_err(|status| error::Error::Transport(status.into()))?
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
            rdlt_connector::gate::describe_parse_error(&error)
        ))
    })?;
    gate_spec(&spec)?;
    // The state-format versions arrive as ONE document, ceilinged on
    // its RAW BYTES before anything parses — the wire retired the map
    // field whose decode materialized a hash table ahead of any gate.
    // The ceiling's arithmetic: a maximal HONEST document — ≤64 kinds
    // of ≤1024-byte keys plus a u32 and JSON punctuation each — measures
    // ≈66 KiB, so 128 KiB admits every honest document with ~2×
    // headroom; a gate-legal ADVERSARIAL document of quote-heavy keys
    // can double under JSON escaping to ~132 KB and is refused loudly.
    // Empty means the empty map, the proto field's own convention.
    const MAX_STATE_FORMAT_VERSIONS_BYTES: usize = 128 * 1024;
    if ok.state_format_versions_json.len() > MAX_STATE_FORMAT_VERSIONS_BYTES {
        return Err(error::Error::Protocol(format!(
            "an inbound state_format_versions_json of {} bytes exceeds the \
             {MAX_STATE_FORMAT_VERSIONS_BYTES}-byte ceiling — a maximal honest \
             document (64 kinds of 1024-byte keys) measures about half of it",
            ok.state_format_versions_json.len()
        )));
    }
    let state_format_versions: BTreeMap<String, u32> = if ok.state_format_versions_json.is_empty() {
        BTreeMap::new()
    } else {
        serde_json::from_slice(&ok.state_format_versions_json).map_err(|error| {
            error::Error::Protocol(format!(
                "undecodable state_format_versions_json in the handshake reply: {}",
                rdlt_connector::gate::describe_parse_error(&error)
            ))
        })?
    };
    // The count and content gates on the PARSED map — its
    // materialization now bounded by the byte ceiling above. 64 kinds
    // is far past any honest negotiation.
    const MAX_STATE_FORMAT_KINDS: usize = 64;
    gate::count(
        "state-format kinds",
        state_format_versions.len(),
        MAX_STATE_FORMAT_KINDS,
    )
    .map_err(error::Error::Protocol)?;
    for state_format_name in state_format_versions.keys() {
        gate::identifier("state format name", state_format_name).map_err(error::Error::Protocol)?;
    }
    // Empty means "a source" per the proto field's own doc — only a
    // non-empty payload claims to be a capabilities document. No byte
    // ceiling ahead of this parse, deliberately: `Capabilities` is an
    // all-scalar typed sheet whose materialization is ~1× its wire
    // bytes — re-evaluate the moment the type grows a collection or
    // untyped field, the way `state_doc_json` earned its ceiling.
    let capabilities: Option<Capabilities> = if ok.capabilities_json.is_empty() {
        None
    } else {
        let capabilities: Capabilities =
            serde_json::from_slice(&ok.capabilities_json).map_err(|error| {
                error::Error::Protocol(format!(
                    "undecodable capabilities_json in the handshake reply: {}",
                    rdlt_connector::gate::describe_parse_error(&error)
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
        state_format_versions,
        protocol_version: PROTOCOL_VERSION,
    })
}

/// Dial and verify in one motion — the shared first half of both
/// adapters' `connect`, over either transport (see
/// [`crate::endpoint::Endpoint`] for the per-transport trust models).
pub(crate) async fn establish(
    endpoint: crate::endpoint::Endpoint,
    budget_bytes: u64,
    config: &serde_json::Value,
    role: Role,
    requirement: &Requirement,
) -> Result<(Channel, Outcome), error::Error> {
    let channel = wire::dial(endpoint, budget_bytes, requirement.rpc_deadline).await?;
    let outcome = run(&channel, role, config, requirement).await?;
    Ok((channel, outcome))
}

#[cfg(test)]
mod requirement_tests {
    use super::*;

    /// The id grammar: reverse-DNS of ASCII segments, nothing a
    /// filename or a log line could be forged with; the version rides
    /// the identifier rule alone.
    #[test]
    fn a_requirement_is_held_to_the_id_grammar_and_the_identifier_rule() {
        for good in ["io.rapidbyte.file", "rogue", "echo-source", "a_b.c-d.e1"] {
            Requirement::new(good).validate().expect(good);
        }
        for bad in [
            "",
            "io..file",
            ".file",
            "io/rapidbyte/file",
            "io.rapidbyte.fi le",
            "io.rapidbyte.\u{1b}file",
            "io.rapidbyte.fïle",
            "../etc",
        ] {
            let error = Requirement::new(bad).validate().expect_err(bad);
            assert!(
                matches!(error, error::Error::Requirement(_)),
                "{bad}: {error}"
            );
        }
        Requirement::new("")
            .with_path("/usr/bin/rdlt-connector-x")
            .validate()
            .expect("the path form carries its identity in the path");
        let long = "a".repeat(rdlt_connector::gate::MAX_WIRE_IDENTIFIER_BYTES + 1);
        assert!(Requirement::new(long).validate().is_err());
        assert!(
            Requirement::new("io.rapidbyte.file")
                .with_version("1.0\n2.0")
                .validate()
                .is_err()
        );
    }
}

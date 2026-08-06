//! Target resolution: what a certification session points at — a
//! connector id resolved by the provider's PATH convention (D-039-1),
//! or an explicit binary path.

use std::path::PathBuf;

use rdlt_runtime::ConnectorRequirement;
use serde_json::Value;

/// What to certify: the connector requirement plus the config document
/// the certification session hands it. The config is CARRIED, never
/// printed — report entries name clauses, not config bytes.
#[derive(Debug, Clone)]
pub struct Target {
    /// Which connector, and how the provider resolves it.
    pub requirement: ConnectorRequirement,
    /// The connector's own config document for the honest (non-probe)
    /// spawns.
    pub config: Value,
}

impl Target {
    /// Certify the binary at `path`. The requirement's id is
    /// deliberately left EMPTY: the operator named a binary, not an
    /// identity, so certification learns the id from the connector's own
    /// Spec reply before any identity-verified handshake (the explicit
    /// path bypasses discovery, D-039-1).
    pub fn resolve_path(path: PathBuf, config: Value) -> Self {
        Self {
            requirement: ConnectorRequirement::new("").with_path(path),
            config,
        }
    }

    /// Certify connector `id`, resolved to a binary by the provider's
    /// PATH convention (D-039-1) and identity-verified strictly against
    /// this id at handshake (D-039-2).
    pub fn resolve_id(id: &str, config: Value) -> Self {
        Self {
            requirement: ConnectorRequirement::new(id),
            config,
        }
    }
}

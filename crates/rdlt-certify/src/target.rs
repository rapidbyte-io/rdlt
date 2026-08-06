//! Target resolution: what a certification session points at — a
//! connector id resolved by the provider's PATH convention (D-039-1),
//! or an explicit binary path.

use std::path::PathBuf;

use rdlt_runtime::ConnectorRequirement;
use serde_json::Value;

/// What to certify: the connector requirement plus the config document
/// the certification session hands it. The config is CARRIED, never
/// printed — report entries name clauses, not config bytes.
#[derive(Clone)]
pub struct Target {
    /// Which connector, and how the provider resolves it.
    pub requirement: ConnectorRequirement,
    /// The connector's own config document for the honest (non-probe)
    /// spawns.
    pub config: Value,
}

/// Manual, not derived: the config document is a connector's own
/// credentials-bearing text, and a derived `Debug` would print it into
/// whatever log or panic message renders a `Target` (the 022 D-21
/// class — a derived `Debug` leaked inline private keys). The document
/// is elided wholesale rather than field-filtered: this type cannot
/// know which of a foreign connector's config fields are secret.
impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("requirement", &self.requirement)
            .field("config", &format_args!("<elided>"))
            .finish()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The negative pin for the manual `Debug`: a marker planted inside
    /// the config document never reaches the rendered output — the
    /// document is elided, not filtered.
    #[test]
    fn debug_never_renders_the_config_document() {
        let marker = "certify-debug-leak-canary";
        let target = Target::resolve_id(
            "io.rapidbyte.file",
            serde_json::json!({ "password": marker }),
        );
        let rendered = format!("{target:?}");
        assert!(
            !rendered.contains(marker),
            "the config document leaked into Debug: {rendered}"
        );
        assert!(
            rendered.contains("config: <elided>"),
            "the elision spelling must name the withheld field: {rendered}"
        );
    }
}

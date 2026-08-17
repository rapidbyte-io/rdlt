//! What a certification session points at — a connector id resolved
//! by the provider's PATH convention, or an explicit binary path — and
//! the spawn/resolve substrate every probe shares: binary resolution,
//! the `--role=` argument, the Spec fetch and the identity a path-only
//! target learns from it, the per-invocation load entropy, and the
//! handshake-line read bounds (line timeout, byte cap).

use std::path::{Path, PathBuf};
use std::time::Duration;

use rdlt_connector::spec::ConnectorSpec;
use rdlt_connector_client::handshake::{Requirement, Role};
use rdlt_runtime::local::Local;

use serde_json::Value;

use crate::report;

/// What to certify: the connector requirement plus the config document
/// the certification session hands it. The config is CARRIED, never
/// printed — report entries name clauses, not config bytes.
#[derive(Clone)]
pub struct Target {
    /// Which connector, and how the provider resolves it.
    pub requirement: Requirement,
    /// The connector's own config document for the honest (non-probe)
    /// spawns.
    pub config: Value,
}

/// Manual, not derived: the config document is a connector's own
/// credentials-bearing text, and a derived `Debug` would print it into
/// whatever log or panic message renders a `Target`. The document is
/// elided wholesale rather than field-filtered: this type cannot know
/// which of a foreign connector's config fields are secret.
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
    /// path bypasses discovery).
    pub fn resolve_path(path: PathBuf, config: Value) -> Self {
        Self {
            requirement: Requirement::new("").with_path(path),
            config,
        }
    }

    /// Certify connector `id`, resolved to a binary by the provider's
    /// PATH convention and identity-verified strictly against this id
    /// at handshake.
    pub fn resolve_id(id: &str, config: Value) -> Self {
        Self {
            requirement: Requirement::new(id),
            config,
        }
    }
}

/// How long a probe waits for the one handshake line — the provider's
/// own figure: not a performance budget but a "this is not a
/// connector" detector. Shared with the wire probe's attach
/// ([`crate::wire`]), which reads the same line on its own spawn.
pub(crate) const LINE_TIMEOUT: Duration = Duration::from_secs(10);

/// The handshake-line byte cap, terminator included — the provider's own
/// flood detector, applied to every probe's read too.
pub(crate) const MAX_LINE_BYTES: u64 = 64 * 1024;

/// One certification invocation's load-id entropy suffix: certify's
/// loads meet DURABLE load-keyed receipts in real warehouses, so a
/// deterministic id would let a PREVIOUS certification's receipts
/// replay-mask this one into a vacuous pass against the same
/// warehouse. Minted once per certification entry call — the
/// debuggable `certify-<slug>` prefix stays, the suffix isolates
/// invocations, and WITHIN one invocation the ids are stable (the kill
/// matrix's convergence re-run must reuse its arm's exact id).
pub(crate) fn mint_run_entropy() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    rdlt_connector::core::schema::ident_hash(&format!("{}:{nanos}", std::process::id()), 12)
}

/// The bin contract's `--role=` argument for `role` — the ONE place the
/// certifier spells the role words, matching the provider's own spawn
/// contract.
pub(crate) fn role_arg(role: Role) -> &'static str {
    match role {
        Role::Source => "--role=source",
        Role::Destination => "--role=destination",
    }
}

/// Fetch the connector's Spec through the provider, timeout-bounded.
/// The reply feeds P4 and — for a path-only target — identity
/// resolution; either consumer renders the error its own way, so the
/// failure is carried as the message string.
pub(crate) async fn fetch_spec(
    provider: &Local,
    requirement: &Requirement,
    role: Role,
) -> Result<ConnectorSpec, String> {
    match tokio::time::timeout(
        report::CLAUSE_TIMEOUT,
        provider.spec(requirement, Some(role)),
    )
    .await
    {
        Ok(Ok(spec)) => Ok(spec),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_elapsed) => Err(report::timed_out()),
    }
}

/// The requirement the wire spawns run under: the target's own when it
/// carries an id, else (a path-only target) the id the Spec reply
/// reported — so the handshake's strict identity check binds the run
/// to the connector's OWN claim, and any skew between its Spec and its
/// handshake surfaces as a refusal.
pub(crate) fn resolved_requirement(
    requirement: &Requirement,
    spec: &Result<ConnectorSpec, String>,
) -> Result<Requirement, String> {
    if !requirement.id.is_empty() {
        return Ok(requirement.clone());
    }
    match spec {
        Ok(spec) => {
            let mut resolved = requirement.clone();
            resolved.id = spec.name.clone();
            Ok(resolved)
        }
        Err(why) => Err(format!(
            "the target names a binary, not a connector id, and the connector's identity \
             could not be learned from its Spec reply: {why}"
        )),
    }
}
/// Resolve the requirement to a spawnable path for a direct probe —
/// the ONE resolution helper every direct spawn rides: the explicit
/// path as given, else the provider's own convention (last
/// `.`-segment, `rdlt-connector-` prefix) walked over `$PATH`.
pub(crate) fn resolve_binary(requirement: &Requirement) -> Result<PathBuf, String> {
    if let Some(path) = &requirement.path {
        return Ok(path.clone());
    }
    let segment = requirement.id.rsplit('.').next().unwrap_or(&requirement.id);
    let binary = format!("rdlt-connector-{segment}");
    if let Some(search) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&search) {
            if dir.as_os_str().is_empty() {
                continue;
            }
            let candidate = dir.join(&binary);
            if is_executable_file(&candidate) {
                return Ok(candidate);
            }
        }
    }
    Err(format!(
        "no binary `{binary}` on PATH and no explicit path was given — install it or \
         certify by explicit path"
    ))
}

/// `which`-style candidacy: a regular file with any execute bit set.
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    //! The `Debug` elision pin.

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

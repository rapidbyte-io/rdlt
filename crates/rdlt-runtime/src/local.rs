//! [`Local`] — the provider for binaries on this machine: a connector
//! id resolves to a binary on PATH by convention, the binary is
//! spawned, and its one stdout handshake line says where to dial.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use rdlt_connector::spec::ConnectorSpec;
use rdlt_connector_client::error::Error as ClientError;
use rdlt_connector_client::handshake::{Requirement, Role};
use rdlt_connector_client::wire::{self, connector_client, dial};
use rdlt_connector_client::{destination, source};
use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::proto::SpecRequest;

use crate::managed::Managed;
use crate::provider::{Error, Provider};
use crate::spawn;

/// Spawns connector binaries and manages their lifecycle.
///
/// Discovery: the requirement id's LAST `.`-segment names the binary —
/// `io.rapidbyte.reference` → `rdlt-connector-reference` — found by a
/// `which`-style walk of PATH. An explicit `path` on the requirement
/// bypasses discovery entirely (no managed directory tree, no
/// registry — that layer belongs to products above).
///
/// Discovery and the handshake's identity check are SANITY CHECKS,
/// not authentication: string equality on the reported id catches an
/// accidental wrong binary, never a malicious one, and running a
/// connector means running its code with this process's privileges.
/// Trust is decided by what is ON PATH — pin `path:` in the document
/// or [`Self::with_search_path`] to a directory only trusted tooling
/// writes; a content pin (digest verification) is the anticipated
/// door for installers that want more.
///
/// Spawn contract: `<binary> --role=<source|destination>`, stdout
/// piped (the handshake line), stderr inherited (the connector's human
/// log channel), stdin null. EXACTLY ONE stdout line is read, under
/// [`Self::with_line_timeout`]'s budget; everything after — dial,
/// handshake, identity and version verification — is the client
/// crate's, and the resulting adapter is wrapped in a
/// [`crate::managed::Managed`] with its [`crate::managed::Guard`] so
/// the process dies with the managed object.
#[derive(Debug, Clone)]
pub struct Local {
    line_timeout: Duration,
    budget_bytes: u64,
    search_path: Option<OsString>,
}

impl Default for Local {
    fn default() -> Self {
        Self {
            line_timeout: spawn::LINE_TIMEOUT,
            // The wire's own per-message ceiling, which `dial` also
            // caps windows at — i.e. "let the wire cap pace" until an
            // embedder threads a real engine budget through
            // `with_budget_bytes` (the facade does, with the engine's
            // own byte budget).
            budget_bytes: MAX_FRAME_BYTES as u64,
            search_path: None,
        }
    }
}

impl Local {
    /// The default configuration — see the field defaults on
    /// [`Default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the handshake-line timeout (default 10 s).
    #[must_use = "with_line_timeout returns the provider; it does not mutate in place"]
    pub fn with_line_timeout(mut self, timeout: Duration) -> Self {
        self.line_timeout = timeout;
        self
    }

    /// Thread the engine's byte budget into the dial — the h2 windows
    /// are derived from it, so a served connector can never hold more
    /// bytes in flight than the engine's own channel budget.
    #[must_use = "with_budget_bytes returns the provider; it does not mutate in place"]
    pub fn with_budget_bytes(mut self, budget: u64) -> Self {
        self.budget_bytes = budget;
        self
    }

    /// Replace the PATH string discovery walks (defaults to the
    /// process's own `$PATH`, read per call). A seam for tests and for
    /// embedders that sandbox where connectors may come from —
    /// mutating the process environment is not an alternative (env is
    /// process-global and `set_var` is unsafe in this edition).
    #[must_use = "with_search_path returns the provider; it does not mutate in place"]
    pub fn with_search_path(mut self, search_path: impl Into<OsString>) -> Self {
        self.search_path = Some(search_path.into());
        self
    }

    /// Resolve the requirement to a spawnable path plus the label its
    /// errors name: the override as given, or the conventional binary
    /// name discovery found (or failed to).
    fn resolve(&self, requirement: &Requirement) -> Result<(PathBuf, String), Error> {
        // The requirement's own text first: its id becomes a filename
        // below and renders in every error naming the connector.
        requirement.validate().map_err(Error::Client)?;
        if let Some(path) = &requirement.path {
            // The override bypasses discovery ENTIRELY — no existence
            // probe here: a wrong path fails at spawn, typed as Spawn,
            // never as NotFound (whose spelling promises "no explicit
            // path was given").
            return Ok((path.clone(), path.display().to_string()));
        }

        let binary = binary_name(&requirement.id);
        let search = self
            .search_path
            .clone()
            .or_else(|| std::env::var_os("PATH"));
        if let Some(search) = &search {
            for dir in std::env::split_paths(search) {
                if dir.as_os_str().is_empty() {
                    continue;
                }
                let candidate = dir.join(&binary);
                if is_executable_file(&candidate) {
                    return Ok((candidate, binary));
                }
            }
        }
        Err(Error::NotFound {
            id: requirement.id.clone(),
            binary,
        })
    }

    /// The connector's static self-description — name, version,
    /// `config_schema` — via the config-free `Spec` RPC: resolve, spawn,
    /// dial, ask, kill. No handshake and no config, so it works with
    /// nothing but the binary (the CLI's `schema <id>` path).
    ///
    /// `role` picks the half asked. `Some(role)` asks exactly that
    /// half — no probing and no silent retry: a single-role binary
    /// refusing the asked-for half surfaces as the spawn-tier error it
    /// is. `None` probes source-first and retries `destination` when
    /// the source process exits with the usage-error code before
    /// writing any handshake bytes — a dual-role connector therefore
    /// answers with its SOURCE schema, and a destination-only one
    /// (whose arg gate refuses `source`) still answers on the second
    /// attempt. Either way the child's stderr is nulled: its usage
    /// refusal is this method's typed answer, not something to print
    /// at the operator.
    ///
    /// Identity is verified like the run path's (strict equality): a
    /// discovered binary whose reported `name` differs from the
    /// requirement id is refused, never worked around — the
    /// last-segment convention would otherwise resolve
    /// `com.example.reference` to `rdlt-connector-reference` and print
    /// the WRONG connector's schema as if it were the asked-for one. An
    /// explicit `path` on the requirement skips the check: the operator
    /// named a binary, not an id, so whatever it reports IS the answer.
    pub async fn spec(
        &self,
        requirement: &Requirement,
        role: Option<Role>,
    ) -> Result<ConnectorSpec, Error> {
        if let Some(role) = role {
            return self.probe(requirement, role).await;
        }
        match self.probe(requirement, Role::Source).await {
            // Exit 2 before any handshake bytes is the role flag's
            // usage refusal. Every other failure is the source probe's
            // own answer and propagates without trying another role.
            Err(Error::ExitedBeforeHandshake { status, .. }) if status.code() == Some(2) => {
                self.probe(requirement, Role::Destination).await
            }
            outcome => outcome,
        }
    }

    /// [`Self::spec`] for one role: spawn under `--role=<role>`, dial,
    /// ask `Spec`, verify identity, and let the guard kill the child on
    /// the way out.
    async fn probe(&self, requirement: &Requirement, role: Role) -> Result<ConnectorSpec, Error> {
        let (path, binary) = self.resolve(requirement)?;
        // The guard exists from the moment the child does — it and its
        // socket die with this scope whether the RPC below answers or
        // refuses.
        let (_guard, line) =
            spawn::spawn(&path, &binary, role_arg(role), true, self.line_timeout).await?;
        let channel = dial(
            (&line.socket_path).into(),
            self.budget_bytes,
            requirement.rpc_deadline,
        )
        .await?;
        // Bounded by the requirement's RPC deadline: a connector that
        // dials fine but never answers Spec is silent-but-ALIVE — its
        // stack answers h2 pings, so the channel's keep-alive can never
        // fire and only this deadline keeps the schema path from
        // hanging (the same law as the client's own awaits: typed
        // within the deadline, never a hang).
        let reply = tokio::time::timeout(
            requirement.rpc_deadline,
            connector_client(channel).spec(SpecRequest {}),
        )
        .await
        .map_err(|_elapsed| {
            Error::Client(ClientError::Timeout {
                operation: wire::Operation::Reply,
                deadline: requirement.rpc_deadline,
            })
        })?
        .map_err(|status| Error::Client(ClientError::Transport(status.into())))?
        .into_inner();
        // The same untyped `config_schema` document the client's
        // handshake gates, from the same untrusted process: the ceiling
        // ahead of any parse, then the shared kind-and-location render.
        if let Err(message) =
            rdlt_connector::gate::refuse_oversized_document("spec_json", &reply.spec_json)
        {
            return Err(Error::Client(ClientError::Protocol(message)));
        }
        let spec: ConnectorSpec = serde_json::from_slice(&reply.spec_json).map_err(|error| {
            Error::Client(ClientError::Protocol(format!(
                "undecodable spec_json in the Spec reply: {}",
                rdlt_connector::gate::describe_parse_error(&error)
            )))
        })?;
        // The same identifier rule the handshake holds a spec to: the
        // name renders in the mismatch below and the version reaches
        // the schema command's output.
        rdlt_connector_client::handshake::gate_spec(&spec).map_err(Error::Client)?;
        if requirement.path.is_none() && spec.name != requirement.id {
            return Err(Error::Client(ClientError::IdentityMismatch {
                expected: requirement.id.clone(),
                reported: spec.name,
            }));
        }
        Ok(spec)
    }
}

/// The convention: the id's LAST `.`-segment, prefixed
/// `rdlt-connector-` — `io.rapidbyte.reference` →
/// `rdlt-connector-reference`. An id with no dots is its own last
/// segment.
fn binary_name(id: &str) -> String {
    let segment = id.rsplit('.').next().unwrap_or(id);
    format!("rdlt-connector-{segment}")
}

/// The `--role=` spelling a connector binary is spawned under.
fn role_arg(role: Role) -> &'static str {
    match role {
        Role::Source => "source",
        Role::Destination => "destination",
    }
}

/// `which`-style candidacy: a regular file with any execute bit set.
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[async_trait]
impl Provider for Local {
    async fn source(
        &self,
        requirement: &Requirement,
        config: &serde_json::Value,
    ) -> Result<Managed<source::Remote>, Error> {
        let (path, binary) = self.resolve(requirement)?;
        let (guard, line) = spawn::spawn(
            &path,
            &binary,
            role_arg(Role::Source),
            false,
            self.line_timeout,
        )
        .await?;
        // The guard exists from the moment the child does: any failure
        // below drops it, which kills the child AND unlinks whatever
        // the connector may already have bound.
        let (adapter, outcome) =
            source::Remote::connect(&line.socket_path, self.budget_bytes, config, requirement)
                .await?;
        Ok(Managed::new(adapter, outcome, Some(guard)))
    }

    async fn destination(
        &self,
        requirement: &Requirement,
        config: &serde_json::Value,
    ) -> Result<Managed<destination::Remote>, Error> {
        let (path, binary) = self.resolve(requirement)?;
        let (guard, line) = spawn::spawn(
            &path,
            &binary,
            role_arg(Role::Destination),
            false,
            self.line_timeout,
        )
        .await?;
        let (adapter, outcome) =
            destination::Remote::connect(&line.socket_path, self.budget_bytes, config, requirement)
                .await?;
        Ok(Managed::new(adapter, outcome, Some(guard)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The convention, spelled: last segment, dotless ids included.
    #[test]
    fn the_binary_name_convention_takes_the_last_segment() {
        assert_eq!(
            binary_name("io.rapidbyte.reference"),
            "rdlt-connector-reference"
        );
        assert_eq!(binary_name("reference"), "rdlt-connector-reference");
    }
}

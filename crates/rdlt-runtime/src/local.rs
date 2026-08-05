//! [`LocalBinaryConnectorProvider`] — D-039-1's provider: a connector
//! id resolves to a binary on PATH by convention, the binary is
//! spawned, and its one stdout handshake line says where to dial.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use rdlt_connector::ConnectorSpec;
use rdlt_connector_client::{ClientError, RemoteDestination, RemoteSource, connector_client, dial};
use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::SpecRequest;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};

use crate::managed::{LifecycleGuard, ManagedDestination, ManagedSource};
use crate::provider::{ConnectorProvider, ProviderError};
use crate::requirement::ConnectorRequirement;

/// How long a spawned binary gets to write its handshake line before
/// the provider gives up on it. Generous on purpose: the line is the
/// FIRST thing a served connector writes (before any config work), so
/// ten seconds is not a performance budget but a "this is not a
/// connector" detector.
const DEFAULT_LINE_TIMEOUT: Duration = Duration::from_secs(10);

/// The most bytes the one handshake line may occupy, terminator
/// included. A conforming line is tens of bytes (four fixed fields
/// plus a socket path), so 64 KiB is not a budget but a flood
/// detector: without a cap, a binary spewing newline-less stdout
/// would grow the line buffer unboundedly until the timeout.
const MAX_LINE_BYTES: u64 = 64 * 1024;

/// Spawns connector binaries and manages their lifecycle (D-039-1).
///
/// Discovery: the requirement id's LAST `.`-segment names the binary —
/// `io.rapidbyte.file` → `rdlt-connector-file` — found by a
/// `which`-style walk of PATH. An explicit `path` on the requirement
/// bypasses discovery entirely (no managed directory tree, no
/// registry — that layer belongs to products above, ADR 0001 D2).
///
/// Spawn contract: `<binary> --role=<source|destination>`, stdout
/// piped (the handshake line), stderr inherited (the connector's human
/// log channel, ADR 0001 D3), stdin null. EXACTLY ONE stdout line is
/// read, under [`Self::with_line_timeout`]'s budget; everything after
/// — dial, handshake, identity/version verification (D-039-2) — is the
/// client crate's, and the resulting adapter is wrapped with a
/// [`LifecycleGuard`] so the process dies with the managed object.
#[derive(Debug, Clone)]
pub struct LocalBinaryConnectorProvider {
    line_timeout: Duration,
    engine_budget_bytes: u64,
    search_path: Option<OsString>,
}

impl Default for LocalBinaryConnectorProvider {
    fn default() -> Self {
        Self {
            line_timeout: DEFAULT_LINE_TIMEOUT,
            // The wire's own per-message ceiling, which `dial` also
            // caps windows at — i.e. "let the wire cap pace" until an
            // embedder threads a real engine budget through
            // `with_engine_budget_bytes` (the facade does, from the
            // spec's batch policy).
            engine_budget_bytes: MAX_FRAME_BYTES as u64,
            search_path: None,
        }
    }
}

impl LocalBinaryConnectorProvider {
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
    #[must_use = "with_engine_budget_bytes returns the provider; it does not mutate in place"]
    pub fn with_engine_budget_bytes(mut self, budget: u64) -> Self {
        self.engine_budget_bytes = budget;
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
    fn resolve(
        &self,
        requirement: &ConnectorRequirement,
    ) -> Result<(PathBuf, String), ProviderError> {
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
        Err(ProviderError::NotFound {
            id: requirement.id.clone(),
            binary,
        })
    }

    /// Spawn `path` for `role` and read EXACTLY ONE stdout line under
    /// the line timeout. Every error path lets the just-spawned `Child`
    /// drop, and the spawn sets `kill_on_drop` — so a binary that
    /// times out or writes garbage is killed here, before any guard
    /// exists to do it.
    ///
    /// `quiet_stderr` nulls the child's stderr instead of inheriting
    /// it — for [`Self::spec`]'s role probing, where a wrong-role
    /// attempt against a single-role connector would otherwise print
    /// that bin's usage refusal into the caller's terminal.
    async fn spawn_and_read_line(
        &self,
        path: &Path,
        binary: &str,
        role: &str,
        quiet_stderr: bool,
    ) -> Result<(Child, Line), ProviderError> {
        let spawn_error = |source| ProviderError::Spawn {
            binary: binary.to_string(),
            source,
        };
        let mut child = Command::new(path)
            // The T6 bin contract's spelling: `--role=source|destination`.
            .arg(format!("--role={role}"))
            // stdout is the machine channel (the one handshake line);
            // stderr stays the connector's human log channel (ADR 0001
            // D3) unless the caller asked for quiet; stdin is nothing's
            // channel.
            .stdout(Stdio::piped())
            .stderr(if quiet_stderr {
                Stdio::null()
            } else {
                Stdio::inherit()
            })
            .stdin(Stdio::null())
            // Belt beside the guard's own start_kill: a child dropped
            // before a guard exists dies with its `Child`.
            .kill_on_drop(true)
            .spawn()
            .map_err(spawn_error)?;

        let stdout = child
            .stdout
            .take()
            .expect("stdout was piped at spawn, so the child carries it");
        // `take` caps the read at [`MAX_LINE_BYTES`]: a binary flooding
        // stdout without a newline hits EOF at the cap instead of
        // growing the buffer until the timeout — detected below as a
        // full buffer with no line terminator.
        let mut reader = BufReader::new(stdout.take(MAX_LINE_BYTES));
        let mut line = String::new();
        match tokio::time::timeout(self.line_timeout, reader.read_line(&mut line)).await {
            Err(_elapsed) => Err(ProviderError::Timeout {
                binary: binary.to_string(),
            }),
            Ok(Err(source)) => Err(spawn_error(source)),
            // EOF (a process that exited without writing) reaches
            // Line::parse as an empty string and refuses typed there —
            // no separate arm to maintain.
            Ok(Ok(_bytes)) => {
                if !line.ends_with('\n') && line.len() as u64 >= MAX_LINE_BYTES {
                    return Err(ProviderError::HandshakeLineOverflow {
                        binary: binary.to_string(),
                        limit: MAX_LINE_BYTES,
                    });
                }
                let parsed =
                    Line::parse(line.trim_end_matches(['\n', '\r'])).map_err(|source| {
                        ProviderError::HandshakeLine {
                            binary: binary.to_string(),
                            source,
                        }
                    })?;
                Ok((child, parsed))
            }
        }
    }

    /// The connector's static self-description — name, version,
    /// `config_schema` — via the config-free `Spec` RPC: resolve, spawn,
    /// dial, ask, kill. No handshake and no config, so it works with
    /// nothing but the binary (the CLI's `schema <id>` path).
    ///
    /// A served bin only answers under a role it carries, so the probe
    /// tries `--role=source` first and falls back to `--role=destination`
    /// when the first spawn produces no handshake line — a dual-role
    /// connector therefore answers with its SOURCE schema, and a
    /// destination-only one (whose arg gate refuses `source`) still
    /// answers on the second attempt. Both probes null the child's
    /// stderr: a wrong-role usage refusal is this method's mechanism,
    /// not something to print at the operator.
    pub async fn spec(
        &self,
        requirement: &ConnectorRequirement,
    ) -> Result<ConnectorSpec, ProviderError> {
        let (path, binary) = self.resolve(requirement)?;
        let mut refusal = None;
        for role in ["source", "destination"] {
            match self.spawn_and_read_line(&path, &binary, role, true).await {
                Ok((child, line)) => {
                    // The guard exists from the moment a socket path is
                    // known — the child and its socket die with this
                    // scope whether the RPC below answers or refuses.
                    let _guard = LifecycleGuard::new(child, line.socket_path.clone());
                    let channel = dial(&line.socket_path, self.engine_budget_bytes).await?;
                    let reply = connector_client(channel)
                        .spec(SpecRequest {})
                        .await
                        .map_err(|status| ProviderError::Client(ClientError::Transport(status)))?
                        .into_inner();
                    let spec: ConnectorSpec =
                        serde_json::from_slice(&reply.spec_json).map_err(|error| {
                            ProviderError::Client(ClientError::Protocol(format!(
                                "undecodable spec_json in the Spec reply: {error}"
                            )))
                        })?;
                    return Ok(spec);
                }
                Err(error) => refusal = Some(error),
            }
        }
        Err(refusal.expect("both role probes ran, so the last error is recorded"))
    }
}

/// D-039-1's convention: the id's LAST `.`-segment, prefixed
/// `rdlt-connector-` — `io.rapidbyte.file` → `rdlt-connector-file`. An
/// id with no dots is its own last segment.
fn binary_name(id: &str) -> String {
    let segment = id.rsplit('.').next().unwrap_or(id);
    format!("rdlt-connector-{segment}")
}

/// `which`-style candidacy: a regular file with any execute bit set.
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[async_trait]
impl ConnectorProvider for LocalBinaryConnectorProvider {
    async fn source(
        &self,
        requirement: &ConnectorRequirement,
        config: &serde_json::Value,
    ) -> Result<ManagedSource, ProviderError> {
        let (path, binary) = self.resolve(requirement)?;
        let (child, line) = self
            .spawn_and_read_line(&path, &binary, "source", false)
            .await?;
        // The guard exists from the moment a socket path is known: any
        // failure below drops it, which kills the child AND unlinks
        // whatever the connector may already have bound.
        let guard = LifecycleGuard::new(child, line.socket_path.clone());
        let (adapter, outcome) = RemoteSource::connect(
            &line.socket_path,
            self.engine_budget_bytes,
            config,
            requirement,
        )
        .await?;
        Ok(ManagedSource::new(
            adapter,
            requirement.id.clone(),
            &outcome,
            Some(guard),
        ))
    }

    async fn destination(
        &self,
        requirement: &ConnectorRequirement,
        config: &serde_json::Value,
    ) -> Result<ManagedDestination, ProviderError> {
        let (path, binary) = self.resolve(requirement)?;
        let (child, line) = self
            .spawn_and_read_line(&path, &binary, "destination", false)
            .await?;
        let guard = LifecycleGuard::new(child, line.socket_path.clone());
        let (adapter, outcome) = RemoteDestination::connect(
            &line.socket_path,
            self.engine_budget_bytes,
            config,
            requirement,
        )
        .await?;
        Ok(ManagedDestination::new(
            adapter,
            requirement.id.clone(),
            &outcome,
            Some(guard),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The convention, spelled: last segment, dotless ids included.
    #[test]
    fn the_binary_name_convention_takes_the_last_segment() {
        assert_eq!(binary_name("io.rapidbyte.file"), "rdlt-connector-file");
        assert_eq!(
            binary_name("io.rapidbyte.postgres"),
            "rdlt-connector-postgres"
        );
        assert_eq!(binary_name("file"), "rdlt-connector-file");
    }
}

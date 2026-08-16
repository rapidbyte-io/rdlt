//! [`LocalBinaryConnectorProvider`] — D-039-1's provider: a connector
//! id resolves to a binary on PATH by convention, the binary is
//! spawned, and its one stdout handshake line says where to dial.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use rdlt_connector::ConnectorSpec;
use rdlt_connector_client::{
    ClientError, Role, TimedOutOperation, connector_client, destination, dial, source,
};
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::SpecRequest;
use rdlt_connector_protocol::{MAX_FRAME_BYTES, PROTOCOL_VERSION};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

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

/// How long after stdout EOF the provider waits for the child to EXIT
/// before refusing on the empty line. EOF means no handshake can ever
/// arrive, so this is not a handshake budget: it exists only so the
/// overwhelmingly common shape — the process died and EOF is how we
/// learn — reports the exit status rather than an empty-line parse.
/// A child that closed stdout and kept running gets the same typed
/// handshake-line refusal it always did, after this grace instead of
/// the full line deadline. Generous relative to the instant the
/// common case needs, deliberately: a role refusal whose exit status
/// is reaped AFTER the grace would misreport as a malformed handshake
/// — and on the flagless dual-role probe, skip the destination retry
/// keyed to the exit code — so the grace errs far toward waiting.
const EOF_EXIT_GRACE: Duration = Duration::from_secs(2);

/// Observe child exit without reaping it. `WNOWAIT` keeps the direct pid
/// anchored until the process-group kill has run, avoiding a group-id reuse
/// race before descendants are swept.
async fn exited_without_reaping(pid: u32, deadline: tokio::time::Instant) -> bool {
    let Some(pid) = i32::try_from(pid)
        .ok()
        .and_then(rustix::process::Pid::from_raw)
    else {
        return false;
    };
    loop {
        let options = rustix::process::WaitIdOptions::EXITED
            | rustix::process::WaitIdOptions::NOHANG
            | rustix::process::WaitIdOptions::NOWAIT;
        if matches!(
            rustix::process::waitid(rustix::process::WaitId::Pid(pid), options),
            Ok(Some(_))
        ) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Spawns connector binaries and manages their lifecycle (D-039-1).
///
/// Discovery: the requirement id's LAST `.`-segment names the binary —
/// `io.rapidbyte.reference` → `rdlt-connector-reference` — found by a
/// `which`-style walk of PATH. An explicit `path` on the requirement
/// bypasses discovery entirely (no managed directory tree, no
/// registry — that layer belongs to products above, ADR 0001 D2).
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
    /// the line timeout. A lifecycle guard owns the new process group
    /// immediately, so every error kills the direct child and anything
    /// it forked even before a socket path is known.
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
    ) -> Result<(LifecycleGuard, Line), ProviderError> {
        let spawn_error = |source| ProviderError::Spawn {
            binary: binary.to_string(),
            source,
        };
        let mut command = Command::new(path);
        command
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
            // Connector configuration is explicit; the host process's
            // ambient credentials are not an implicit second config
            // channel. Retain only locale/temp/process-discovery data.
            .env_clear()
            // Belt beside the guard's group kill.
            .process_group(0)
            .kill_on_drop(true);
        for key in [
            "PATH", "TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL", "LC_CTYPE", "TZ",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let child = command.spawn().map_err(spawn_error)?;
        // Own the process group immediately: every line-read/parse
        // failure below now kills descendants too, before a socket path
        // is even known.
        let mut guard = LifecycleGuard::new_process_group(child);

        let stdout = guard
            .child_mut()
            .stdout
            .take()
            .expect("stdout was piped at spawn, so the child carries it");
        // `take` caps the read at [`MAX_LINE_BYTES`]: a binary flooding
        // stdout without a newline hits EOF at the cap instead of
        // growing the buffer until the timeout — detected below as a
        // full buffer with no line terminator.
        let mut reader = BufReader::new(stdout.take(MAX_LINE_BYTES));
        let mut line = String::new();
        let deadline = tokio::time::Instant::now() + self.line_timeout;
        match tokio::time::timeout_at(deadline, reader.read_line(&mut line)).await {
            Err(_elapsed) => Err(ProviderError::Timeout {
                binary: binary.to_string(),
            }),
            Ok(Err(source)) => Err(spawn_error(source)),
            Ok(Ok(bytes)) => {
                if bytes == 0 {
                    // EOF: no handshake can ever arrive, so never wait
                    // out the full line deadline here — a child that
                    // closed stdout but kept running would convert this
                    // typed refusal into a Timeout. [`EOF_EXIT_GRACE`]
                    // bounds the wait for an exit status; on elapse the
                    // empty line falls through to the parse refusal
                    // below (the still-running child dies with its
                    // kill_on_drop handle).
                    let grace = deadline.min(tokio::time::Instant::now() + EOF_EXIT_GRACE);
                    let pid = guard.pid();
                    if let Some(pid) = pid
                        && exited_without_reaping(pid, grace).await
                    {
                        guard.kill_process_group();
                        let status = guard.child_mut().wait().await.map_err(spawn_error)?;
                        if !status.success() {
                            return Err(ProviderError::ExitedBeforeHandshake {
                                binary: binary.to_string(),
                                role: role.to_string(),
                                status,
                            });
                        }
                    }
                }
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
                // The range the line advertises is HONORED here, before
                // any dial: this host's protocol version must sit inside
                // it. The SDK's gRPC handshake would refuse a mismatch
                // too, but only after the socket exchange — late, and in
                // whatever shape a version-skewed exchange produces.
                if !(parsed.proto_min..=parsed.proto_max).contains(&PROTOCOL_VERSION) {
                    return Err(ProviderError::ProtocolRange {
                        binary: binary.to_string(),
                        min: parsed.proto_min,
                        max: parsed.proto_max,
                        ours: PROTOCOL_VERSION,
                    });
                }
                guard.set_socket_path(parsed.socket_path.clone());
                Ok((guard, parsed))
            }
        }
    }

    /// The connector's static self-description — name, version,
    /// `config_schema` — via the config-free `Spec` RPC: resolve, spawn,
    /// dial, ask, kill. No handshake and no config, so it works with
    /// nothing but the binary (the CLI's `schema <id>` path).
    ///
    /// A served bin only answers under a role it carries, so this probe
    /// delegates to [`Self::spec_for_role`] source-first and retries
    /// `destination` when the source process exits with the usage-error
    /// code before writing any handshake bytes — a dual-role connector
    /// therefore answers with its SOURCE schema, and a destination-only
    /// one (whose arg gate refuses `source`) still answers on the second
    /// attempt. The CLI's `schema --role` flag is the explicit door when
    /// the destination half of a dual-role connector is the one wanted.
    pub async fn spec(
        &self,
        requirement: &ConnectorRequirement,
    ) -> Result<ConnectorSpec, ProviderError> {
        match self.spec_for_role(requirement, Role::Source).await {
            // Exit 2 before any handshake bytes is the role flag's
            // usage refusal. Every other failure is the source probe's
            // own answer and propagates without trying another role.
            Err(ProviderError::ExitedBeforeHandshake { status, .. })
                if status.code() == Some(2) =>
            {
                self.spec_for_role(requirement, Role::Destination).await
            }
            outcome => outcome,
        }
    }

    /// [`Self::spec`] under ONE explicit role — no probing and no
    /// silent retry: the spawn is `--role=<role>` and a single-role
    /// binary refusing the asked-for half surfaces as the spawn-tier
    /// error it is (the CLI's `schema --role` door, 040 T9 — a
    /// dual-role connector's destination schema is unreachable through
    /// the source-first probe). The child's stderr is nulled like the
    /// probe's: its usage refusal is this method's typed answer, not
    /// something to print at the operator.
    ///
    /// Identity is verified like the run path's (D-039-2, strict
    /// equality): a discovered binary whose reported `name` differs
    /// from the requirement id is refused, never worked around — the
    /// last-segment convention would otherwise resolve
    /// `com.example.reference` to `rdlt-connector-reference` and print the WRONG
    /// connector's schema as if it were the asked-for one. An explicit
    /// `path` on the requirement skips the check: the operator named a
    /// binary, not an id, so whatever it reports IS the answer.
    pub async fn spec_for_role(
        &self,
        requirement: &ConnectorRequirement,
        role: Role,
    ) -> Result<ConnectorSpec, ProviderError> {
        let (path, binary) = self.resolve(requirement)?;
        let role = match role {
            Role::Source => "source",
            Role::Destination => "destination",
        };
        let (guard, line) = self.spawn_and_read_line(&path, &binary, role, true).await?;
        // The guard exists from the moment a socket path is known — the
        // child and its socket die with this scope whether the RPC
        // below answers or refuses.
        let _guard = guard;
        let channel = dial(
            &line.socket_path,
            self.engine_budget_bytes,
            requirement.rpc_deadline,
        )
        .await?;
        // Bounded by the requirement's RPC deadline: a connector that
        // dials fine but never answers Spec is silent-but-ALIVE — its
        // stack answers h2 pings, so the channel's keep-alive can
        // never fire and only this deadline keeps the schema path from
        // hanging (the same law as the client's own awaits: typed
        // within the deadline, never a hang).
        let reply = tokio::time::timeout(
            requirement.rpc_deadline,
            connector_client(channel).spec(SpecRequest {}),
        )
        .await
        .map_err(|_elapsed| {
            ProviderError::Client(ClientError::Timeout {
                operation: TimedOutOperation::Reply,
                deadline: requirement.rpc_deadline,
            })
        })?
        .map_err(|status| ProviderError::Client(ClientError::Transport(status)))?
        .into_inner();
        // The document ceiling and the shared kind-and-location renderer
        // (7M4): the probe parses the SAME untyped `config_schema` field
        // the client's handshake seat gates, from the same adversary —
        // the wave-7 sweep converted every client seat and missed this,
        // the runtime's own probe.
        if let Err(message) =
            rdlt_connector::json::refuse_oversized_document("spec_json", &reply.spec_json)
        {
            return Err(ProviderError::Client(ClientError::Protocol(message)));
        }
        let spec: ConnectorSpec = serde_json::from_slice(&reply.spec_json).map_err(|error| {
            ProviderError::Client(ClientError::Protocol(format!(
                "undecodable spec_json in the Spec reply: {}",
                rdlt_connector::json::describe_parse_error(&error)
            )))
        })?;
        if requirement.path.is_none() && spec.name != requirement.id {
            return Err(ProviderError::Client(ClientError::IdMismatch {
                expected: requirement.id.clone(),
                reported: spec.name,
            }));
        }
        Ok(spec)
    }
}

/// D-039-1's convention: the id's LAST `.`-segment, prefixed
/// `rdlt-connector-` — `io.rapidbyte.reference` → `rdlt-connector-reference`. An
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
        let (guard, line) = self
            .spawn_and_read_line(&path, &binary, "source", false)
            .await?;
        // The guard exists from the moment a socket path is known: any
        // failure below drops it, which kills the child AND unlinks
        // whatever the connector may already have bound.
        let (adapter, outcome) = source::Source::connect(
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
        let (guard, line) = self
            .spawn_and_read_line(&path, &binary, "destination", false)
            .await?;
        let (adapter, outcome) = destination::Destination::connect(
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
        assert_eq!(
            binary_name("io.rapidbyte.reference"),
            "rdlt-connector-reference"
        );
        assert_eq!(binary_name("reference"), "rdlt-connector-reference");
    }

    /// One law, pinned from this side: the ten seconds a spawned
    /// binary gets to write its handshake line is the same ten seconds
    /// the client gives every wire await after it — a dead OR silent
    /// connector yields a typed error within ten seconds, never a
    /// hang. Change either constant and this pin names the fracture.
    #[test]
    fn the_line_timeout_is_the_client_rpc_deadline() {
        assert_eq!(
            DEFAULT_LINE_TIMEOUT,
            rdlt_connector_client::DEFAULT_RPC_DEADLINE
        );
    }
}

//! Target resolution — what a certification session points at: a
//! connector id resolved by the provider's PATH convention (D-039-1),
//! or an explicit binary path — plus the role-generic clause probes
//! both certifications share: P1 (the handshake-line discipline), P2
//! (typed config refusal), and P4 (the pre-handshake Spec). The
//! source and destination certifiers differ only in which SPI half
//! the provider spawns; everything role-generic lives HERE, once.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use rdlt_connector_protocol::handshake::Line;
use rdlt_runtime::{
    ClientError, ConnectorRequirement, LocalBinaryConnectorProvider, ProviderError, Role,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};

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

/// The role-generic clauses this module's probes report — P1 at its
/// probe's call sites, P2 and P4 inside [`report_p2`]/[`report_p4`].
/// Both certifiers build their dead-handshake cascade sets from this
/// constant (P1 filtered out — its probe has already written its
/// entry), and the clause-vocabulary pin folds it into the emittable
/// id set ([`crate::report`]'s table test).
pub(crate) const GENERIC_CLAUSES: [&str; 3] = ["P1", "P2", "P4"];

/// How long a probe waits for the one handshake line — the provider's
/// own figure: not a performance budget but a "this is not a
/// connector" detector. Shared with the wire probe's attach
/// ([`crate::wire`]), which reads the same line on its own spawn.
pub(crate) const LINE_TIMEOUT: Duration = Duration::from_secs(10);

/// The handshake-line byte cap, terminator included — the provider's own
/// flood detector, applied to every probe's read too.
pub(crate) const MAX_LINE_BYTES: u64 = 64 * 1024;

/// How long the P1 probe listens for a SECOND stdout line after the
/// handshake line. Stdout is the machine channel and carries EXACTLY one
/// line; anything more within this window is a P1 violation.
const SECOND_LINE_WINDOW: Duration = Duration::from_millis(500);

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
    provider: &LocalBinaryConnectorProvider,
    requirement: &ConnectorRequirement,
) -> Result<rdlt_connector::ConnectorSpec, String> {
    match tokio::time::timeout(CLAUSE_TIMEOUT, provider.spec(requirement)).await {
        Ok(Ok(spec)) => Ok(spec),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_elapsed) => Err(timed_out()),
    }
}

/// The requirement the wire spawns run under: the target's own when it
/// carries an id, else (a path-only target) the id the Spec reply
/// reported — so the handshake's strict identity check (D-039-2) binds
/// the run to the connector's OWN claim, and any skew between its Spec
/// and its handshake surfaces as a refusal.
pub(crate) fn resolved_requirement(
    requirement: &ConnectorRequirement,
    spec: &Result<rdlt_connector::ConnectorSpec, String>,
) -> Result<ConnectorRequirement, String> {
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

/// Judge P2 from a bogus-config spawn's outcome: an unknown field must
/// come back as a typed handshake refusal (classification carried),
/// never as a dial failure, a dead stream, or an acceptance. Generic
/// over the managed type because the SPI half is the only thing the
/// two certifications' probes differ in.
pub(crate) fn report_p2<T>(
    report: &mut Report,
    outcome: Result<Result<T, ProviderError>, tokio::time::error::Elapsed>,
) {
    match outcome {
        Ok(Ok(_accepted)) => report.fail(
            "P2",
            "the connector accepted a config document consisting of one unknown field — \
             the config gate must refuse unknown fields with a typed handshake refusal"
                .to_string(),
        ),
        Ok(Err(ProviderError::Client(ClientError::Handshake { .. }))) => report.pass("P2"),
        Ok(Err(error)) => report.fail(
            "P2",
            format!(
                "an unknown config field must be refused with a typed handshake refusal — \
                 the connector instead produced: {error}"
            ),
        ),
        Err(_elapsed) => report.fail("P2", timed_out()),
    }
}

/// Judge P4 from the Spec reply: name/version non-empty and a JSON
/// -object config schema, answered with no config at all.
pub(crate) fn report_p4(report: &mut Report, spec: &Result<rdlt_connector::ConnectorSpec, String>) {
    match spec {
        Ok(spec) => {
            let mut problems = Vec::new();
            if spec.name.is_empty() {
                problems.push("`name` is empty".to_string());
            }
            if spec.version.is_empty() {
                problems.push("`version` is empty".to_string());
            }
            match &spec.config_schema {
                Some(schema) if schema.is_object() => {}
                Some(_) => problems.push("`config_schema` is not a JSON object".to_string()),
                None => problems.push(
                    "`config_schema` is absent — the Spec reply must describe the config"
                        .to_string(),
                ),
            }
            if problems.is_empty() {
                report.pass("P4");
            } else {
                report.fail(
                    "P4",
                    format!("the Spec reply is incomplete: {}", problems.join("; ")),
                );
            }
        }
        Err(why) => report.fail("P4", format!("the Spec RPC did not answer: {why}")),
    }
}

/// The P1 probe: spawn the binary directly under `role`, read the FIRST
/// stdout line under the provider's own cap and timeout, parse it as a
/// handshake line, then listen [`SECOND_LINE_WINDOW`] for any further
/// stdout byte — one is a violation (stdout is the machine channel;
/// logs belong on stderr). The probe process is killed either way, and
/// the socket its line advertised is unlinked best-effort (no guard
/// ever owned it).
pub(crate) async fn probe_handshake_line(target: &Target, role: Role) -> Result<(), String> {
    let path = resolve_binary(&target.requirement)?;
    let mut child = Command::new(&path)
        // The bin contract's spelling, as the provider spawns it.
        .arg(role_arg(role))
        // stderr is nulled: this probe's only purpose is stdout
        // discipline, not the connector's human log.
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("spawning `{}`: {error}", path.display()))?;

    let stdout = child
        .stdout
        .take()
        .expect("stdout was piped at spawn, so the child carries it");
    let mut reader = BufReader::new(stdout.take(MAX_LINE_BYTES));
    let mut line = String::new();
    match tokio::time::timeout(LINE_TIMEOUT, reader.read_line(&mut line)).await {
        Err(_elapsed) => {
            return Err(format!(
                "wrote no handshake line within {}s — the first stdout line must be the \
                 handshake line",
                LINE_TIMEOUT.as_secs()
            ));
        }
        Ok(Err(error)) => return Err(format!("reading the handshake line: {error}")),
        Ok(Ok(_bytes)) => {}
    }
    if !line.ends_with('\n') && line.len() as u64 >= MAX_LINE_BYTES {
        return Err(format!(
            "wrote {MAX_LINE_BYTES} bytes of stdout without completing a handshake line"
        ));
    }
    let parsed = Line::parse(line.trim_end_matches(['\n', '\r']))
        .map_err(|error| format!("the first stdout line is not a handshake line: {error}"))?;

    // The second-line poll: silence (or EOF — nothing more CAN be
    // spoken) passes; any byte fails.
    let mut byte = [0u8; 1];
    let verdict = match tokio::time::timeout(SECOND_LINE_WINDOW, reader.read(&mut byte)).await {
        Err(/* window elapsed in silence */ _) | Ok(Ok(0)) => Ok(()),
        Ok(Ok(_more)) => Err(
            "stdout spoke after the handshake line — stdout is the machine channel and \
             carries EXACTLY one line; logs belong on stderr"
                .to_string(),
        ),
        Ok(Err(error)) => Err(format!("reading stdout after the handshake line: {error}")),
    };

    // Kill the probe and reclaim the socket its line advertised — no
    // LifecycleGuard ever owned this child, so the cleanup is manual.
    let _ = child.kill().await;
    let _ = std::fs::remove_file(&parsed.socket_path);
    verdict
}

/// Resolve the requirement to a spawnable path for a direct probe (the
/// P1 line probe and the wire probe's attach both spawn through this —
/// ONE resolution helper, not a fourth copy): the explicit path as
/// given, else the provider's own D-039-1 convention (last
/// `.`-segment, `rdlt-connector-` prefix) walked over `$PATH`.
pub(crate) fn resolve_binary(requirement: &ConnectorRequirement) -> Result<PathBuf, String> {
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

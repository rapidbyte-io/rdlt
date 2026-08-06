//! Source certification: spawn the target's binary and certify it over
//! the wire — the protocol clauses this task probes (P1 handshake-line
//! discipline, P2 typed config refusal, P4 pre-handshake Spec) plus the
//! testkit's source conformance clauses (S1/S2/S4) reused against the
//! managed adapter.
//!
//! Every clause rides under [`CLAUSE_TIMEOUT`] — a stalling connector
//! FAILS the clause, the certifier never hangs — and no failure message
//! ever carries config bytes.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use rdlt_connector_protocol::handshake::Line;
use rdlt_runtime::{
    ClientError, ConnectorProvider, ConnectorRequirement, LocalBinaryConnectorProvider,
    ProviderError,
};
use rdlt_testkit::conformance::source::verify_source;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};
use crate::target::Target;

/// How long the P1 probe waits for the one handshake line — the
/// provider's own figure: not a performance budget but a "this is not a
/// connector" detector.
const LINE_TIMEOUT: Duration = Duration::from_secs(10);

/// The handshake-line byte cap, terminator included — the provider's own
/// flood detector, applied to the probe's read too.
const MAX_LINE_BYTES: u64 = 64 * 1024;

/// How long the P1 probe listens for a SECOND stdout line after the
/// handshake line. Stdout is the machine channel and carries EXACTLY one
/// line; anything more within this window is a P1 violation.
const SECOND_LINE_WINDOW: Duration = Duration::from_millis(500);

/// The S-clauses the reused testkit suite asserts — its module doc's
/// exact set.
const SOURCE_CLAUSES: [&str; 3] = ["S1", "S2", "S4"];

/// Certify `target` as a SOURCE connector. Never hangs and never
/// panics on connector misbehavior: every clause's outcome — including
/// "the binary is not a connector at all" — is a report entry.
pub async fn certify_source(target: &Target) -> Report {
    let mut report = Report::default();

    // P1 — the handshake-line discipline, probed on a direct spawn whose
    // only purpose is P1; certification re-spawns cleanly afterward.
    match tokio::time::timeout(CLAUSE_TIMEOUT, probe_handshake_line(target)).await {
        Ok(Ok(())) => report.pass("P1"),
        Ok(Err(why)) => report.fail("P1", why),
        Err(_elapsed) => report.fail("P1", timed_out()),
    }

    let provider = LocalBinaryConnectorProvider::new();

    // The Spec reply feeds P4 below — and, for a path-only target,
    // identity: the operator named a binary, not an id, so the id the
    // wire handshake verifies strictly (D-039-2) is learned from the
    // connector's own report.
    let spec = match tokio::time::timeout(CLAUSE_TIMEOUT, provider.spec(&target.requirement)).await
    {
        Ok(Ok(spec)) => Ok(spec),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_elapsed) => Err(timed_out()),
    };

    let requirement = match resolved_requirement(&target.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            // No identity means no verified handshake can happen at all:
            // everything past P1 fails with the one cause.
            for clause in ["P2", "P4"].into_iter().chain(SOURCE_CLAUSES) {
                report.fail(clause, why.clone());
            }
            return report;
        }
    };

    // The certification subject: one managed source, spawned honestly
    // through the provider (resolution is part of the bar).
    let managed = tokio::time::timeout(
        CLAUSE_TIMEOUT,
        provider.source(&requirement, &target.config),
    )
    .await;
    let managed = match managed {
        Ok(Ok(managed)) => managed,
        Ok(Err(error)) => {
            let why = format!("the provider could not spawn the connector as a source: {error}");
            for clause in ["P2", "P4"].into_iter().chain(SOURCE_CLAUSES) {
                report.fail(clause, why.clone());
            }
            return report;
        }
        Err(_elapsed) => {
            for clause in ["P2", "P4"].into_iter().chain(SOURCE_CLAUSES) {
                report.fail(clause, timed_out());
            }
            return report;
        }
    };

    // P2 — typed config refusal: an unknown field must come back as a
    // typed handshake refusal (classification carried), never as a dial
    // failure, a dead stream, or an acceptance.
    let bogus = serde_json::json!({ "__rdlt_certify_bogus__": true });
    match tokio::time::timeout(CLAUSE_TIMEOUT, provider.source(&requirement, &bogus)).await {
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

    // P4 — the pre-handshake Spec: name/version non-empty and a JSON
    // -object config schema, answered with no config at all.
    match &spec {
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

    // S-reuse — the testkit's source conformance suite, verbatim,
    // against the managed adapter: the wire is certified by the SAME
    // clauses an in-process connector answers to.
    match tokio::time::timeout(CLAUSE_TIMEOUT, verify_source(&managed)).await {
        Ok(failures) => report.absorb(failures, &SOURCE_CLAUSES),
        Err(_elapsed) => {
            for clause in SOURCE_CLAUSES {
                report.fail(clause, timed_out());
            }
        }
    }

    report
}

/// The requirement the wire spawns run under: the target's own when it
/// carries an id, else (a path-only target) the id the Spec reply
/// reported — so the handshake's strict identity check (D-039-2) binds
/// the run to the connector's OWN claim, and any skew between its Spec
/// and its handshake surfaces as a refusal.
fn resolved_requirement(
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

/// The P1 probe: spawn the binary directly, read the FIRST stdout line
/// under the provider's own cap and timeout, parse it as a handshake
/// line, then listen [`SECOND_LINE_WINDOW`] for any further stdout byte
/// — one is a violation (stdout is the machine channel; logs belong on
/// stderr). The probe process is killed either way, and the socket its
/// line advertised is unlinked best-effort (no guard ever owned it).
async fn probe_handshake_line(target: &Target) -> Result<(), String> {
    let path = resolve_binary(&target.requirement)?;
    let mut child = Command::new(&path)
        // The bin contract's spelling, as the provider spawns it.
        .arg("--role=source")
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

/// Resolve the requirement to a spawnable path for the direct probe:
/// the explicit path as given, else the provider's own D-039-1
/// convention (last `.`-segment, `rdlt-connector-` prefix) walked over
/// `$PATH`.
fn resolve_binary(requirement: &ConnectorRequirement) -> Result<PathBuf, String> {
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

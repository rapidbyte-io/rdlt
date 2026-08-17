//! Target resolution — what a certification session points at: a
//! connector id resolved by the provider's PATH convention (D-039-1),
//! or an explicit binary path — plus the role-generic clause probes
//! both certifications share: P1 (the handshake-line discipline), P2
//! (typed config refusal), P4 (the pre-handshake Spec), and P13 (the
//! unserved role's refusal, probed on a spawn of the OTHER role's
//! `--role=` flag). The source and destination certifiers differ only
//! in which SPI half the provider spawns; everything role-generic
//! lives HERE, once.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use rdlt_connector_client::error::Error as ClientError;
use rdlt_connector_client::handshake::{Requirement, Role};
use rdlt_connector_protocol::handshake::Line;
use rdlt_runtime::local::Local;
use rdlt_runtime::provider;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};
use crate::wire;

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
            requirement: Requirement::new("").with_path(path),
            config,
        }
    }

    /// Certify connector `id`, resolved to a binary by the provider's
    /// PATH convention (D-039-1) and identity-verified strictly against
    /// this id at handshake (D-039-2).
    pub fn resolve_id(id: &str, config: Value) -> Self {
        Self {
            requirement: Requirement::new(id),
            config,
        }
    }
}

/// The role-generic clauses this module's probes report — P1 and P13
/// at their probes' call sites, P2 and P4 inside
/// [`report_p2`]/[`report_p4`]. Both certifiers build their
/// dead-handshake cascade sets from this constant (the
/// [`SELF_PROBED_CLAUSES`] filtered out), and the clause-vocabulary
/// pin folds it into the emittable id set ([`crate::report`]'s table
/// test).
pub(crate) const GENERIC_CLAUSES: [&str; 4] = ["P1", "P13", "P2", "P4"];

/// The generic clauses whose probes write their own entries BEFORE any
/// cascade point — every dead-handshake cascade set filters these out,
/// or a cascade would write a second verdict per clause.
pub(crate) const SELF_PROBED_CLAUSES: [&str; 2] = ["P1", "P13"];

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

/// One certification invocation's load-id entropy suffix (round-13):
/// certify's loads meet DURABLE load-keyed receipts in real warehouses
/// (docs/connector-authoring.md, "Load identity"), so a deterministic
/// id would let a PREVIOUS certification's receipts replay-mask this
/// one into a vacuous pass against the same warehouse. Minted once per
/// certification entry call — the debuggable `certify-<slug>` prefix
/// stays, the suffix isolates invocations, and WITHIN one invocation
/// the ids are stable (the kill matrix's convergence re-run must reuse
/// its arm's exact id).
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
) -> Result<rdlt_connector::spec::ConnectorSpec, String> {
    match tokio::time::timeout(CLAUSE_TIMEOUT, provider.spec(requirement, Some(role))).await {
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
    requirement: &Requirement,
    spec: &Result<rdlt_connector::spec::ConnectorSpec, String>,
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

/// Judge P2 from a bogus-config spawn's outcome: an unknown field must
/// come back as a typed handshake refusal (classification carried),
/// never as a dial failure, a dead stream, or an acceptance. Generic
/// over the managed type because the SPI half is the only thing the
/// two certifications' probes differ in.
pub(crate) fn report_p2<T>(
    report: &mut Report,
    outcome: Result<Result<T, provider::Error>, tokio::time::error::Elapsed>,
) {
    match outcome {
        Ok(Ok(_accepted)) => report.fail(
            "P2",
            "the connector accepted a config document consisting of one unknown field — \
             the config gate must refuse unknown fields with a typed handshake refusal"
                .to_string(),
        ),
        Ok(Err(provider::Error::Client(ClientError::Handshake { .. }))) => report.pass("P2"),
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
pub(crate) fn report_p4(
    report: &mut Report,
    spec: &Result<rdlt_connector::spec::ConnectorSpec, String>,
) {
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
/// stdout line, parse it as a handshake line, then listen
/// [`SECOND_LINE_WINDOW`] for any further stdout byte — one is a
/// violation (stdout is the machine channel; logs belong on stderr).
/// The spawn and first-line read are the wire attach's own funnel
/// ([`wire::spawn_and_read_line`], round-12 — this probe hand-rolled
/// an identical copy), and the child rides the standardized
/// [`wire::ChildSlot`]. `budget` (the caller's clause timeout) wraps
/// the probe I/O ALONE and the [`wire::reap_parked`] sits OUTSIDE it —
/// the P13 probe's shape: a timeout wrapped around the whole probe
/// cancelled the reap with the future, and the P1 child is a live
/// server holding its store while the P2 spawn follows immediately.
/// On every exit, timeout included, the child is dead AND reaped and
/// any socket its line advertised is unlinked.
pub(crate) async fn probe_handshake_line(
    target: &Target,
    role: Role,
    budget: Duration,
) -> Result<(), String> {
    let path = resolve_binary(&target.requirement)?;
    let slot = wire::ChildSlot::default();
    let verdict =
        match tokio::time::timeout(budget, first_line_discipline(&path, role, &slot)).await {
            Ok(verdict) => verdict,
            Err(_elapsed) => Err(timed_out()),
        };
    wire::reap_parked(&slot).await;
    verdict
}

/// The P1 judgments proper, over the shared funnel; the caller owns
/// the one reap on the way out (a helper error path has already
/// reaped — [`wire::reap_parked`] is idempotent).
async fn first_line_discipline(
    path: &std::path::Path,
    role: Role,
    slot: &wire::ChildSlot,
) -> Result<(), String> {
    let (mut reader, line) = wire::spawn_and_read_line(path, role, slot).await?;
    if !line.ends_with('\n') && line.len() as u64 >= MAX_LINE_BYTES {
        return Err(format!(
            "wrote {MAX_LINE_BYTES} bytes of stdout without completing a handshake line"
        ));
    }
    let parsed = Line::parse(line.trim_end_matches(['\n', '\r']))
        .map_err(|error| format!("the first stdout line is not a handshake line: {error}"))?;
    // The advertised socket joins the parked state the moment it is
    // KNOWN, so the caller's reap unlinks it too.
    slot.lock()
        .expect("child slot lock")
        .park_socket(parsed.socket_path);

    // The second-line poll: silence (or EOF — nothing more CAN be
    // spoken) passes; any byte fails.
    let mut byte = [0u8; 1];
    match tokio::time::timeout(SECOND_LINE_WINDOW, reader.read(&mut byte)).await {
        Err(/* window elapsed in silence */ _) | Ok(Ok(0)) => Ok(()),
        Ok(Ok(_more)) => Err(
            "stdout spoke after the handshake line — stdout is the machine channel and \
             carries EXACTLY one line; logs belong on stderr"
                .to_string(),
        ),
        Ok(Err(error)) => Err(format!("reading stdout after the handshake line: {error}")),
    }
}

/// The P13 skip reason a SOURCE certification announces for a
/// dual-role connector. Public as the ONE spelling dual-role
/// connectors' certify cells assert against (the [`NO_MERGE_SKIP`]
/// precedent — a spelling copied into suites drifts).
///
/// [`NO_MERGE_SKIP`]: crate::NO_MERGE_SKIP
pub const SOURCE_DUAL_ROLE_SKIP: &str = "`--role=destination` answered its spawn with a handshake line beside the certified \
     `--role=source` — a connector serving both roles has no unserved role to refuse";

/// [`SOURCE_DUAL_ROLE_SKIP`]'s destination-certification twin.
pub const DESTINATION_DUAL_ROLE_SKIP: &str = "`--role=source` answered its spawn with a handshake line beside the certified \
     `--role=destination` — a connector serving both roles has no unserved role to refuse";

/// The role a certification does NOT certify — the flag the P13 probe
/// spawns.
fn unserved_role(certified: Role) -> Role {
    match certified {
        Role::Source => Role::Destination,
        Role::Destination => Role::Source,
    }
}

/// What the P13 probe observed of the unserved-role spawn.
enum RoleRefusal {
    /// Exit code 2 with zero stdout bytes, AND the served role's
    /// control spawn handshaking from the same bare argv — the
    /// documented refusal, attributed to the role.
    Refused,
    /// The spawn answered with a handshake line: the connector SERVES
    /// that role too, so there is no unserved role to judge.
    Serves,
    /// Anything else, rendered for the Fail entry.
    Violation(String),
}

/// P13 — the unserved role's refusal: a connector binary must refuse a
/// `--role` it does not serve by exiting with code 2 before writing
/// any stdout byte (docs/connector-authoring.md — the flagless
/// `rdlt schema` probe keys on exactly this signal to retry the other
/// role). The probe spawns the OTHER role on its own child; a
/// handshake line instead means the connector serves both roles, and
/// the clause is skipped with the reason naming them.
pub(crate) async fn report_role_refusal(report: &mut Report, target: &Target, certified: Role) {
    let path = match resolve_binary(&target.requirement) {
        Ok(path) => path,
        Err(why) => {
            report.fail("P13", why);
            return;
        }
    };
    match probe_role_refusal(&path, certified, CLAUSE_TIMEOUT).await {
        Ok(RoleRefusal::Refused) => report.pass("P13"),
        Ok(RoleRefusal::Serves) => report.skip(
            "P13",
            match certified {
                Role::Source => SOURCE_DUAL_ROLE_SKIP,
                Role::Destination => DESTINATION_DUAL_ROLE_SKIP,
            }
            .to_string(),
        ),
        Ok(RoleRefusal::Violation(why)) | Err(why) => report.fail("P13", why),
    }
}

/// The P13 probe proper: the unserved-role spawn (and, on its silent
/// exit 2, the served-role control) under `budget`, the children
/// riding the standardized [`wire::ChildSlot`]. The timeout wraps the
/// probe I/O ALONE and the [`wire::reap_parked`] sits OUTSIDE it, so
/// the reap runs on EVERY exit path — timeout included, where a reap
/// inside the timed future would be cancelled with it, leaving a
/// dual-role connector's serving child merely `kill_on_drop`-signalled
/// (dying, not dead — still holding any single-writer store lock the
/// certifier's next spawn races for) and its advertised socket
/// linked. On return the children are dead AND reaped and any parked
/// socket unlinked.
async fn probe_role_refusal(
    path: &Path,
    certified: Role,
    budget: Duration,
) -> Result<RoleRefusal, String> {
    let slot = wire::ChildSlot::default();
    let verdict = match tokio::time::timeout(
        budget,
        unserved_role_discipline(path, certified, &slot),
    )
    .await
    {
        Ok(verdict) => verdict,
        Err(_elapsed) => Err(timed_out()),
    };
    wire::reap_parked(&slot).await;
    verdict
}

/// The P13 judgments proper, over one direct spawn of the unserved
/// role (plus [`served_role_control`] where the refusal shape needs
/// attribution).
async fn unserved_role_discipline(
    path: &Path,
    certified: Role,
    slot: &wire::ChildSlot,
) -> Result<RoleRefusal, String> {
    let unserved = unserved_role(certified);
    let mut child = tokio::process::Command::new(path)
        .arg(role_arg(unserved))
        // The probes' shared stdio discipline: stdout is the subject,
        // stderr is the human log, stdin has nothing to say.
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
    slot.lock().expect("child slot lock").park_child(child);

    // The refusal must come BEFORE any stdout byte, so the first read
    // decides the arm: EOF with nothing read is the refusal shape (the
    // exit code still to check), a handshake line is a served role,
    // any other byte is already half the violation. Byte-capped like
    // every probe read; unbounded in time deliberately — the caller's
    // clause timeout is the bound.
    let mut reader = BufReader::new(stdout.take(MAX_LINE_BYTES));
    let mut line = String::new();
    let first = reader
        .read_line(&mut line)
        .await
        .map_err(|error| format!("reading the unserved role's stdout: {error}"))?;

    if first == 0 {
        let status = wait_parked(slot).await?;
        return match status.code() {
            Some(2) => served_role_control(path, certified, slot).await,
            _ => Ok(RoleRefusal::Violation(role_refusal_violation(
                unserved, 0, &status,
            ))),
        };
    }

    if let Ok(parsed) = Line::parse(line.trim_end_matches(['\n', '\r'])) {
        // The handshake line is written only for a role the binary
        // serves (docs/connector-authoring.md), so this spawn IS a
        // served role — the advertised socket joins the parked state
        // so the caller's reap kills the serving child AND unlinks
        // the socket its line advertised.
        slot.lock()
            .expect("child slot lock")
            .park_socket(parsed.socket_path);
        return Ok(RoleRefusal::Serves);
    }

    // Stdout spoke something else: drain to EOF (a violator's own exit
    // closes the pipe; one that hangs instead rides the clause
    // timeout) so the evidence names the full byte count.
    let mut rest = Vec::new();
    reader
        .read_to_end(&mut rest)
        .await
        .map_err(|error| format!("reading the unserved role's stdout: {error}"))?;
    let status = wait_parked(slot).await?;
    Ok(RoleRefusal::Violation(role_refusal_violation(
        unserved,
        first + rest.len(),
        &status,
    )))
}

/// The evidence rule behind a P13 pass (the probe's control): exit 2
/// with zero stdout is ALSO what a general usage error looks like —
/// the sdk's own arg gate answers a missing required argument through
/// clap with exactly that shape, indistinguishable on this probe's
/// observed channels from the role refusal — so a binary requiring
/// more argv than `--role=<x>` would mint `PASS P13` for a role it
/// serves. Before the refusal is trusted, the SERVED role must answer
/// a handshake line from the same bare argv. A binary whose served
/// role cannot is FAILED, not excused: the flagless schema probe the
/// clause exists for spawns exactly this bare shape, so the ambiguity
/// is itself the defect. The control's child (a live server on
/// success) parks in `slot` — the caller's reap kills it and unlinks
/// its parked socket.
async fn served_role_control(
    path: &Path,
    certified: Role,
    slot: &wire::ChildSlot,
) -> Result<RoleRefusal, String> {
    // spawn_and_read_line's own error paths reap the slot before
    // returning; its success parks the served child for the caller.
    let line = match wire::spawn_and_read_line(path, certified, slot).await {
        Ok((_reader, line)) => line,
        Err(why) => {
            return Ok(RoleRefusal::Violation(ambiguous_refusal_violation(
                certified, &why,
            )));
        }
    };
    let trimmed = line.trim_end_matches(['\n', '\r']);
    match Line::parse(trimmed) {
        Ok(parsed) => {
            slot.lock()
                .expect("child slot lock")
                .park_socket(parsed.socket_path);
            Ok(RoleRefusal::Refused)
        }
        Err(_) if trimmed.is_empty() => Ok(RoleRefusal::Violation(ambiguous_refusal_violation(
            certified,
            "it wrote no stdout either",
        ))),
        Err(_) => Ok(RoleRefusal::Violation(ambiguous_refusal_violation(
            certified,
            "its first stdout line is not a handshake line",
        ))),
    }
}

/// The one spelling for an exit 2 the control could not attribute to
/// the role.
fn ambiguous_refusal_violation(certified: Role, control_outcome: &str) -> String {
    format!(
        "the unserved {unserved} exited with code 2 and no stdout, but the served {served} \
         spawned from the same bare argv did not answer with a handshake line \
         ({control_outcome}) — exit 2 cannot be attributed to a role refusal when the binary \
         refuses the bare `--role=` argv for both roles",
        unserved = role_arg(unserved_role(certified)),
        served = role_arg(certified),
    )
}

/// Claim the parked child and await its exit status.
async fn wait_parked(slot: &wire::ChildSlot) -> Result<std::process::ExitStatus, String> {
    let child = slot.lock().expect("child slot lock").claim_child();
    let mut child = child.expect("the probe parked its child before waiting on it");
    child
        .wait()
        .await
        .map_err(|error| format!("awaiting the unserved role's exit: {error}"))
}

/// The one P13 violation spelling: the flag, the byte count, and how
/// the process ended.
fn role_refusal_violation(
    unserved: Role,
    stdout_bytes: usize,
    status: &std::process::ExitStatus,
) -> String {
    let ended = match status.code() {
        Some(code) => format!("exited with code {code}"),
        None => "was terminated by a signal".to_string(),
    };
    format!(
        "the unserved {role} must be refused with exit code 2 before any stdout byte — the \
         connector wrote {stdout_bytes} stdout byte(s) and {ended}",
        role = role_arg(unserved)
    )
}

/// Resolve the requirement to a spawnable path for a direct probe (the
/// P1 line probe and the wire probe's attach both spawn through this —
/// ONE resolution helper, not a fourth copy): the explicit path as
/// given, else the provider's own D-039-1 convention (last
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
    //! The P2/P4 rogue suite (the T10b carry — both clauses' fail arms
    //! were code-present but unproven against a designated rogue): P2
    //! and P4 probe through the PROVIDER's spawn path, so each rogue
    //! here is two halves — an in-process tonic server bound to a UDS
    //! (the crate's rogue substrate) plus a spawnable script fake that
    //! prints one valid handshake line naming that socket (the 039
    //! runtime T5 idiom). No built bin is needed, so these ride the
    //! bare (ungated) suite, driving the pub(crate) probe seams
    //! directly with the exact strings `certify_source` folds into the
    //! report's Fail entries.

    use rdlt_connector::spec::ConnectorSpec;
    use rdlt_runtime::provider::Provider;

    use super::*;
    use crate::report::Verdict;
    use crate::rogue::{self, HandshakeScript, RogueSource};

    /// Write an executable script fake into `dir`: it prints one valid
    /// handshake line naming `socket` (where an in-process rogue is
    /// already listening) and then stays alive holding the pipes —
    /// `exec` so the pid the provider's guard kills is the process
    /// actually holding them.
    fn write_connector_fake(dir: &Path, name: &str, socket: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\necho 'rdlt-connector|1|0|0|{}'\nexec sleep 30\n",
                socket.display()
            ),
        )
        .expect("the fake script writes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("the fake script becomes executable");
        path
    }

    #[track_caller]
    fn assert_fail(report: &Report, clause: &str, evidence: &str) {
        let verdict = &report
            .entries
            .iter()
            .find(|entry| entry.clause == clause)
            .unwrap_or_else(|| panic!("no {clause} entry:\n{}", report.render_text()))
            .verdict;
        match verdict {
            Verdict::Fail(why) => assert_eq!(why, evidence, "clause {clause}"),
            other => panic!(
                "{clause} must Fail, got {other:?}:\n{}",
                report.render_text()
            ),
        }
    }

    /// P2's designated rogue: a connector whose config gate accepts
    /// ANYTHING — the truthful-identity rogue source never reads
    /// `config_json`, so the bogus one-unknown-field document sails
    /// through its handshake — must fail P2 with the pinned evidence.
    /// The spawn is the certifier's own: the provider spawns the script
    /// fake, follows its handshake line to the rogue's socket, and the
    /// handshake ACCEPTS, which is exactly the outcome shape
    /// `report_p2` must convict.
    #[tokio::test]
    async fn a_connector_accepting_the_bogus_config_fails_p2() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_source(
            &socket,
            RogueSource {
                handshake: HandshakeScript::truthful(),
                streams: vec![],
                read_declared: vec![],
                read_undeclared: vec![],
                read_hold_open: false,
            },
        );
        let script = write_connector_fake(dir.path(), "accepts-any-config", &socket);

        // The requirement a path-resolved certification runs under: the
        // id learned from the connector's own claim (the truthful
        // script reports `rogue`), the binary as the operator named it.
        let requirement = Requirement::new("rogue").with_path(&script);
        let provider = Local::new();
        // The certifier's own probe document — `certify_source`'s exact
        // spelling.
        let bogus = serde_json::json!({ "__rdlt_certify_bogus__": true });

        let mut report = Report::default();
        report_p2(
            &mut report,
            tokio::time::timeout(CLAUSE_TIMEOUT, provider.source(&requirement, &bogus)).await,
        );
        assert_fail(
            &report,
            "P2",
            "the connector accepted a config document consisting of one unknown field — \
             the config gate must refuse unknown fields with a typed handshake refusal",
        );
    }

    /// Serve `spec` from the blank-spec rogue behind a script fake and
    /// run the certifier's own Spec fetch + P4 judgment against it —
    /// the requirement is path-only with an EMPTY id, exactly
    /// [`Target::resolve_path`]'s shape, so the provider's identity
    /// check is bypassed and the incomplete document reaches the
    /// judgment rather than dying earlier as an id mismatch.
    async fn p4_report_for(spec: ConnectorSpec) -> Report {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_spec(&socket, spec);
        let script = write_connector_fake(dir.path(), "blank-spec", &socket);

        let requirement = Requirement::new("").with_path(&script);
        let provider = Local::new();
        let spec = fetch_spec(&provider, &requirement, Role::Source).await;
        let mut report = Report::default();
        report_p4(&mut report, &spec);
        report
    }

    /// P4's designated rogue, primary variant: a Spec reply whose
    /// `name` is blank fails P4 with the incompleteness evidence naming
    /// exactly that field — version and schema are well-formed, so the
    /// blank name is the ONE problem the pin convicts.
    #[tokio::test]
    async fn a_blank_name_spec_reply_fails_p4() {
        let mut spec = ConnectorSpec::new("", "0.0.0");
        spec.config_schema = Some(serde_json::json!({ "type": "object" }));
        let report = p4_report_for(spec).await;
        assert_fail(
            &report,
            "P4",
            "the Spec reply is incomplete: `name` is empty",
        );
    }

    /// P4's schema variant: a `config_schema` that parses but is not a
    /// JSON object (an array here) fails P4 on the schema arm alone.
    #[tokio::test]
    async fn a_non_object_config_schema_fails_p4() {
        let mut spec = ConnectorSpec::new("rogue", "0.0.0");
        spec.config_schema = Some(serde_json::json!(["not", "an", "object"]));
        let report = p4_report_for(spec).await;
        assert_fail(
            &report,
            "P4",
            "the Spec reply is incomplete: `config_schema` is not a JSON object",
        );
    }

    /// Round-9 fix: an EARLY P1 failure (here the fastest one — an
    /// unparseable first line) must kill AND REAP the probe child
    /// before returning. `kill_on_drop` only SENDS SIGKILL, and a
    /// dying-not-dead child of the single-writer class still holds its
    /// store lock while the immediately-following wire spawn opens the
    /// same store. Reaped means no zombie: the child's `/proc` entry is
    /// gone the moment the probe returns.
    #[tokio::test]
    async fn a_failed_handshake_probe_reaps_its_child_before_returning() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let pidfile = dir.path().join("pid");
        let script = dir.path().join("garbage-liner");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho $$ > {}\necho 'not a handshake line'\nexec sleep 30\n",
                pidfile.display()
            ),
        )
        .expect("the script writes");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("the script becomes executable");

        let target = Target::resolve_path(script, serde_json::json!({}));
        let error = probe_handshake_line(&target, Role::Source, CLAUSE_TIMEOUT)
            .await
            .expect_err("a garbage first line fails P1");
        assert!(
            error.contains("not a handshake line"),
            "the failure names the parse refusal: {error}"
        );

        let pid = std::fs::read_to_string(&pidfile)
            .expect("the script wrote its pid before its first line")
            .trim()
            .to_owned();
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "the probe child (pid {pid}) must be dead AND reaped when the probe returns — \
             a zombie or a dying process still holds single-writer store locks"
        );
    }

    /// Write an executable script fake for the P13 probes: `body` is
    /// the whole script after the `#!/bin/sh` shebang. The probe
    /// spawns the unserved role and — on its silent exit 2 — the
    /// served role as the control, so a fake standing in for a
    /// conforming single-role binary must script BOTH arms.
    fn write_role_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("the fake script writes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("the fake script becomes executable");
        path
    }

    /// Run the P13 report arm against `body`'s script, certifying
    /// `certified` — the probe spawns the OTHER role.
    async fn p13_report_for(body: &str, certified: Role) -> Report {
        let dir = tempfile::tempdir().expect("tempdir");
        let script = write_role_script(dir.path(), "role-script", body);
        let target = Target::resolve_path(script, serde_json::json!({}));
        let mut report = Report::default();
        report_role_refusal(&mut report, &target, certified).await;
        report
    }

    #[track_caller]
    fn p13_verdict(report: &Report) -> &Verdict {
        &report
            .entries
            .iter()
            .find(|entry| entry.clause == "P13")
            .unwrap_or_else(|| panic!("no P13 entry:\n{}", report.render_text()))
            .verdict
    }

    /// The positive arm: a single-role connector refusing the unserved
    /// role the documented way — exit code 2, zero stdout bytes, while
    /// the served role handshakes from the same bare argv (the
    /// control that attributes the exit to the role) — passes P13.
    #[tokio::test]
    async fn a_silent_exit_2_on_the_unserved_role_passes_p13() {
        let dir = tempfile::tempdir().expect("tempdir");
        let script = write_role_script(
            dir.path(),
            "single-role",
            &format!(
                "case \"$1\" in\n\
                 --role=source) echo 'rdlt-connector|1|0|0|{socket}'; exec sleep 30;;\n\
                 *) exit 2;;\n\
                 esac",
                socket = dir.path().join("served.sock").display()
            ),
        );
        let target = Target::resolve_path(script, serde_json::json!({}));
        let mut report = Report::default();
        report_role_refusal(&mut report, &target, Role::Source).await;
        assert!(
            matches!(p13_verdict(&report), Verdict::Pass),
            "P13 must Pass:\n{}",
            report.render_text()
        );
    }

    /// The final-review red pin behind the control: an exit 2 that is
    /// NOT the role's — the script exits 2 for EVERY argv, the shape
    /// of a binary refusing a missing required argument (the sdk's
    /// clap gate answers exactly so) — must FAIL P13 rather than mint
    /// the pass: the served role cannot handshake from the same bare
    /// argv, so the exit cannot be attributed to a role refusal.
    #[tokio::test]
    async fn an_unconditional_exit_2_fails_p13_as_unattributable() {
        let report = p13_report_for("exit 2", Role::Source).await;
        match p13_verdict(&report) {
            Verdict::Fail(why) => assert_eq!(
                why,
                "the unserved --role=destination exited with code 2 and no stdout, but the \
                 served --role=source spawned from the same bare argv did not answer with a \
                 handshake line (it wrote no stdout either) — exit 2 cannot be attributed to \
                 a role refusal when the binary refuses the bare `--role=` argv for both roles"
            ),
            other => panic!("P13 must Fail, got {other:?}:\n{}", report.render_text()),
        }
    }

    /// The P1 twin of the P13 timeout-reap pin below (the re-review
    /// ruling): probe_handshake_line's reap also survives its budget —
    /// this child is a live server holding its store, and the P2 spawn
    /// follows immediately, so a cancelled reap here is the WORSE
    /// instance of the same hazard.
    #[tokio::test]
    async fn a_timed_out_handshake_probe_still_reaps_its_child() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pidfile = dir.path().join("pid");
        let script = write_role_script(
            dir.path(),
            "staller",
            &format!("echo $$ > {}\nexec sleep 30", pidfile.display()),
        );
        let target = Target::resolve_path(script, serde_json::json!({}));
        let error = probe_handshake_line(&target, Role::Source, Duration::from_millis(300))
            .await
            .expect_err("a stalled probe times out");
        assert_eq!(error, timed_out());
        let pid = std::fs::read_to_string(&pidfile)
            .expect("the script wrote its pid before stalling")
            .trim()
            .to_owned();
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "the probe child (pid {pid}) must be dead AND reaped when the probe returns"
        );
    }

    /// Final-review fix: the reap survives the clause timeout — the
    /// timeout wraps the probe I/O alone, so an unserved-role spawn
    /// stalling past the budget still leaves its child dead AND reaped
    /// when the probe returns. (The reap used to sit INSIDE the timed
    /// future, so a timeout cancelled it, leaving the child merely
    /// kill_on_drop-signalled — dying, not dead, while the very next
    /// wire spawn raced it for any single-writer store it held.)
    #[tokio::test]
    async fn a_timed_out_probe_still_reaps_its_child() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pidfile = dir.path().join("pid");
        let script = write_role_script(
            dir.path(),
            "staller",
            &format!("echo $$ > {}\nexec sleep 30", pidfile.display()),
        );
        let Err(error) =
            probe_role_refusal(&script, Role::Source, Duration::from_millis(300)).await
        else {
            panic!("a stalled probe must time out, not conclude");
        };
        assert_eq!(error, timed_out());
        let pid = std::fs::read_to_string(&pidfile)
            .expect("the script wrote its pid before stalling")
            .trim()
            .to_owned();
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "the probe child (pid {pid}) must be dead AND reaped when the probe returns"
        );
    }

    /// The designated violator: stdout noise and a wrong exit code —
    /// the clause fails naming the byte count and the exit code, the
    /// evidence pinned full-string (`echo 'stdout noise'` is 13 bytes
    /// with its newline).
    #[tokio::test]
    async fn a_noisy_wrong_exit_fails_p13_naming_bytes_and_code() {
        let report = p13_report_for("echo 'stdout noise'\nexit 3", Role::Source).await;
        match p13_verdict(&report) {
            Verdict::Fail(why) => assert_eq!(
                why,
                "the unserved --role=destination must be refused with exit code 2 before any \
                 stdout byte — the connector wrote 13 stdout byte(s) and exited with code 3"
            ),
            other => panic!("P13 must Fail, got {other:?}:\n{}", report.render_text()),
        }
    }

    /// A silent spawn with the WRONG exit code is still a violation:
    /// only exit 2 is the documented refusal (a clean exit 0 would
    /// make the flagless schema probe read a dead process as a served
    /// role gone quiet).
    #[tokio::test]
    async fn a_silent_wrong_exit_code_fails_p13() {
        let report = p13_report_for("exit 0", Role::Source).await;
        match p13_verdict(&report) {
            Verdict::Fail(why) => assert_eq!(
                why,
                "the unserved --role=destination must be refused with exit code 2 before any \
                 stdout byte — the connector wrote 0 stdout byte(s) and exited with code 0"
            ),
            other => panic!("P13 must Fail, got {other:?}:\n{}", report.render_text()),
        }
    }

    /// The dual-role arm: an unserved-role spawn that answers with a
    /// handshake line IS a served role — the clause skips with the
    /// announced reason naming both roles, and the serving child is
    /// dead AND reaped when the probe returns (the P1 probe's round-9
    /// discipline).
    #[tokio::test]
    async fn a_dual_role_connector_earns_the_announced_p13_skip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pidfile = dir.path().join("pid");
        let script = write_role_script(
            dir.path(),
            "dual-role",
            &format!(
                "echo $$ > {pid}\necho 'rdlt-connector|1|0|0|{socket}'\nexec sleep 30",
                pid = pidfile.display(),
                socket = dir.path().join("served.sock").display()
            ),
        );
        let target = Target::resolve_path(script, serde_json::json!({}));

        let mut report = Report::default();
        report_role_refusal(&mut report, &target, Role::Source).await;
        match p13_verdict(&report) {
            Verdict::Skip(reason) => assert_eq!(reason, SOURCE_DUAL_ROLE_SKIP),
            other => panic!("P13 must Skip, got {other:?}:\n{}", report.render_text()),
        }

        let pid = std::fs::read_to_string(&pidfile)
            .expect("the script wrote its pid before its handshake line")
            .trim()
            .to_owned();
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "the serving child (pid {pid}) must be dead AND reaped when the probe returns"
        );
    }

    /// The destination-certification direction announces ITS twin
    /// spelling — the probed flag is `--role=source`.
    #[tokio::test]
    async fn a_destination_certification_announces_the_twin_skip_spelling() {
        let dir = tempfile::tempdir().expect("tempdir");
        let script = write_role_script(
            dir.path(),
            "dual-role",
            &format!(
                "echo 'rdlt-connector|1|0|0|{socket}'\nexec sleep 30",
                socket = dir.path().join("served.sock").display()
            ),
        );
        let target = Target::resolve_path(script, serde_json::json!({}));

        let mut report = Report::default();
        report_role_refusal(&mut report, &target, Role::Destination).await;
        match p13_verdict(&report) {
            Verdict::Skip(reason) => assert_eq!(reason, DESTINATION_DUAL_ROLE_SKIP),
            other => panic!("P13 must Skip, got {other:?}:\n{}", report.render_text()),
        }
    }

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

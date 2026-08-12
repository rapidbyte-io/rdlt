//! Target resolution — what a certification session points at: a
//! connector id resolved by the provider's PATH convention (D-039-1),
//! or an explicit binary path — plus the role-generic clause probes
//! both certifications share: P1 (the handshake-line discipline), P2
//! (typed config refusal), and P4 (the pre-handshake Spec). The
//! source and destination certifiers differ only in which SPI half
//! the provider spawns; everything role-generic lives HERE, once.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rdlt_connector_protocol::handshake::Line;
use rdlt_runtime::{
    ClientError, ConnectorRequirement, LocalBinaryConnectorProvider, ProviderError, Role,
};
use serde_json::Value;
use tokio::io::AsyncReadExt;

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};
use crate::wire;

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
    rdlt_connector::core::naming::ident_hash(&format!("{}:{nanos}", std::process::id()), 12)
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
    provider: &LocalBinaryConnectorProvider,
    requirement: &ConnectorRequirement,
    role: Role,
) -> Result<rdlt_connector::ConnectorSpec, String> {
    match tokio::time::timeout(CLAUSE_TIMEOUT, provider.spec_for_role(requirement, role)).await {
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
/// stdout line, parse it as a handshake line, then listen
/// [`SECOND_LINE_WINDOW`] for any further stdout byte — one is a
/// violation (stdout is the machine channel; logs belong on stderr).
/// The spawn and first-line read are the wire attach's own funnel
/// ([`wire::spawn_and_read_line`], round-12 — this probe hand-rolled
/// an identical copy), and the child rides the standardized
/// [`wire::ChildSlot`]: the unconditional [`wire::reap_parked`] on the
/// way out kills AND REAPS the process and unlinks any socket its line
/// advertised on EVERY exit, pass and fail alike (the round-9
/// discipline).
pub(crate) async fn probe_handshake_line(target: &Target, role: Role) -> Result<(), String> {
    let path = resolve_binary(&target.requirement)?;
    let slot = wire::ChildSlot::default();
    let verdict = first_line_discipline(&path, role, &slot).await;
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

    use rdlt_connector::ConnectorSpec;
    use rdlt_runtime::ConnectorProvider;

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
        let requirement = ConnectorRequirement::new("rogue").with_path(&script);
        let provider = LocalBinaryConnectorProvider::new();
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

        let requirement = ConnectorRequirement::new("").with_path(&script);
        let provider = LocalBinaryConnectorProvider::new();
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
        let error = probe_handshake_line(&target, Role::Source)
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

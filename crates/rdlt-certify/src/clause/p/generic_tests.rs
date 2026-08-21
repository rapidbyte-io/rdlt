//! The P1 reap pins and the P2/P4/P13 rogue suite: each designated
//! rogue proves its clause CAN fail, with the evidence pinned
//! full-string. P2 and P4
//! probe through the PROVIDER's spawn path, so each rogue is two
//! halves — an in-process tonic server bound to a UDS plus a
//! spawnable script fake that prints one valid handshake line
//! naming that socket. No built bin is needed, so these ride the
//! bare (ungated) suite.

use std::path::PathBuf;

use rdlt_runtime::local::Local;
use rdlt_runtime::provider::Provider;

use super::support::{assert_fail, verdict};
use super::*;
use crate::report::Verdict;
use crate::rogue::{self, HandshakeScript, RogueSource};

/// Write an executable script fake into `dir`: it prints one valid
/// handshake line naming `socket` (where an in-process rogue is
/// already listening) and then stays alive holding the pipes —
/// `exec` so the pid the provider's guard kills is the process
/// actually holding them.
pub(super) fn write_connector_fake(dir: &Path, name: &str, socket: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(
        &path,
        format!(
            "#!/bin/sh\necho 'rdlt-connector|1|{v}|{v}|{}'\nexec sleep 30\n",
            socket.display(),
            v = rdlt_connector_protocol::PROTOCOL_VERSION
        ),
    )
    .expect("the fake script writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("the fake script becomes executable");
    path
}

/// Write an executable script fake into `dir`: `body` is the whole
/// script after the `#!/bin/sh` shebang.
fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("the fake script writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("the fake script becomes executable");
    path
}

/// An EARLY P1 failure (here the fastest one — an unparseable
/// first line) must kill AND REAP the probe child
/// before returning. `kill_on_drop` only SENDS SIGKILL, and a
/// dying-not-dead child of the single-writer class still holds its
/// store lock while the immediately-following wire spawn opens the
/// same store. Reaped means no zombie: the child's `/proc` entry is
/// gone the moment the probe returns.
#[tokio::test]
async fn a_failed_handshake_probe_reaps_its_child_before_returning() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pidfile = dir.path().join("pid");
    let script = write_script(
        dir.path(),
        "garbage-liner",
        &format!(
            "echo $$ > {}\necho 'not a handshake line'\nexec sleep 30",
            pidfile.display()
        ),
    );

    let target = Target::resolve_path(script, serde_json::json!({}));
    let error = probe_handshake_line(&target, Role::Source, report::CLAUSE_TIMEOUT)
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

/// The P1 twin of the P13 timeout-reap pin: probe_handshake_line's
/// reap also survives its budget — this child is a live server
/// holding its store, and the P2 spawn follows immediately, so a
/// cancelled reap here is the WORSE instance of the same hazard.
#[tokio::test]
async fn a_timed_out_handshake_probe_still_reaps_its_child() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pidfile = dir.path().join("pid");
    let script = write_script(
        dir.path(),
        "staller",
        &format!("echo $$ > {}\nexec sleep 30", pidfile.display()),
    );
    let target = Target::resolve_path(script, serde_json::json!({}));
    let error = probe_handshake_line(&target, Role::Source, Duration::from_millis(300))
        .await
        .expect_err("a stalled probe times out");
    assert_eq!(error, report::timed_out());
    let pid = std::fs::read_to_string(&pidfile)
        .expect("the script wrote its pid before stalling")
        .trim()
        .to_owned();
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "the probe child (pid {pid}) must be dead AND reaped when the probe returns"
    );
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
            streams_raw: None,
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
    // The certifier's own probe document — the source certifier's
    // exact spelling.
    let bogus = serde_json::json!({ "__rdlt_certify_bogus__": true });

    let mut report = Report::default();
    report_p2(
        &mut report,
        tokio::time::timeout(
            report::CLAUSE_TIMEOUT,
            provider.source(&requirement, &bogus),
        )
        .await,
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
    let spec = target::fetch_spec(&provider, &requirement, Role::Source).await;
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
    report_p13(&mut report, &target, certified).await;
    report
}

#[track_caller]
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
             --role=source) echo 'rdlt-connector|1|{v}|{v}|{socket}'; exec sleep 30;;\n\
             *) exit 2;;\n\
             esac",
            socket = dir.path().join("served.sock").display(),
            v = rdlt_connector_protocol::PROTOCOL_VERSION
        ),
    );
    let target = Target::resolve_path(script, serde_json::json!({}));
    let mut report = Report::default();
    report_p13(&mut report, &target, Role::Source).await;
    assert!(
        matches!(verdict(&report, "P13"), Verdict::Pass),
        "P13 must Pass:\n{}",
        report.render_text()
    );
}

/// The red pin behind the control: an exit 2 that is
/// NOT the role's — the script exits 2 for EVERY argv, the shape
/// of a binary refusing a missing required argument (the sdk's
/// clap gate answers exactly so) — must FAIL P13 rather than mint
/// the pass: the served role cannot handshake from the same bare
/// argv, so the exit cannot be attributed to a role refusal.
#[tokio::test]
async fn an_unconditional_exit_2_fails_p13_as_unattributable() {
    let report = p13_report_for("exit 2", Role::Source).await;
    match verdict(&report, "P13") {
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

/// The reap survives the clause timeout: the timeout wraps the
/// probe I/O alone, so an unserved-role spawn stalling past the
/// budget still leaves its child dead AND reaped when the probe
/// returns — a reap inside the timed future would be cancelled with
/// it, leaving the child merely signalled while the next wire spawn
/// raced it for any single-writer store it held.
#[tokio::test]
async fn a_timed_out_probe_still_reaps_its_child() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pidfile = dir.path().join("pid");
    let script = write_role_script(
        dir.path(),
        "staller",
        &format!("echo $$ > {}\nexec sleep 30", pidfile.display()),
    );
    let Err(error) = probe_role_refusal(&script, Role::Source, Duration::from_millis(300)).await
    else {
        panic!("a stalled probe must time out, not conclude");
    };
    assert_eq!(error, report::timed_out());
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
    match verdict(&report, "P13") {
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
    match verdict(&report, "P13") {
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
/// dead AND reaped when the probe returns.
#[tokio::test]
async fn a_dual_role_connector_earns_the_announced_p13_skip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pidfile = dir.path().join("pid");
    let script = write_role_script(
        dir.path(),
        "dual-role",
        &format!(
            "echo $$ > {pid}\necho 'rdlt-connector|1|{v}|{v}|{socket}'\nexec sleep 30",
            pid = pidfile.display(),
            socket = dir.path().join("served.sock").display(),
            v = rdlt_connector_protocol::PROTOCOL_VERSION
        ),
    );
    let target = Target::resolve_path(script, serde_json::json!({}));

    let mut report = Report::default();
    report_p13(&mut report, &target, Role::Source).await;
    match verdict(&report, "P13") {
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
            "echo 'rdlt-connector|1|{v}|{v}|{socket}'\nexec sleep 30",
            socket = dir.path().join("served.sock").display(),
            v = rdlt_connector_protocol::PROTOCOL_VERSION
        ),
    );
    let target = Target::resolve_path(script, serde_json::json!({}));

    let mut report = Report::default();
    report_p13(&mut report, &target, Role::Destination).await;
    match verdict(&report, "P13") {
        Verdict::Skip(reason) => assert_eq!(reason, DESTINATION_DUAL_ROLE_SKIP),
        other => panic!("P13 must Skip, got {other:?}:\n{}", report.render_text()),
    }
}

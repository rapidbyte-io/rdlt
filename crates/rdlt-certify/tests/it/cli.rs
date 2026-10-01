//! The `rdlt-certify` binary: what it certifies, how it reports, and its exit codes.

use std::process::Output;

use rdlt_testkit::tls::Pki;
use tokio::process::Command;

use crate::listening::listening;
use crate::spawned::example;

async fn certify(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rdlt-certify"))
        .args(args)
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .output()
        .await
        .expect("rdlt-certify runs")
}

fn code(output: &Output) -> Option<i32> {
    output.status.code()
}

const USERS: &str = r#"{"streams": {"users": [{"id": 1}, {"id": 2}]}}"#;

#[tokio::test(flavor = "multi_thread")]
async fn a_connector_seen_to_keep_every_clause_exits_zero_and_reports_as_json() {
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    // Rows enough that a kill lands as the source still reads.
    let output = certify(&[
        binary,
        "--role",
        "source",
        "--config",
        r#"{"seed": 11, "streams": [{"name": "events", "rows": 20000, "partitions": 2, "batch_rows": 50}]}"#,
        "--env",
        "LLVM_PROFILE_FILE",
        "--kill-seed",
        "1",
        "--output",
        "json",
    ])
    .await;
    assert_eq!(
        code(&output),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("the report is JSON");
    assert_eq!(report["passed"], true, "{report}");
    assert_eq!(report["verdict"], "passed", "{report}");
    assert_eq!(report["reports"][0]["verdict"], "passed", "{report}");
    assert_eq!(report["reports"][0]["connector"], "io.rapidbyte.generator");
    assert_eq!(report["reports"][0]["clauses"][0]["id"], "P-HANDSHAKE");
    assert_eq!(report["reports"][0]["clauses"][0]["outcome"], "passed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_certification_that_could_not_observe_a_clause_exits_two_unless_part_is_required() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    // Two rows end before any kill lands: the kill clause applies, and is not observed.
    let small = [
        binary,
        "--role",
        "source",
        "--config",
        USERS,
        "--env",
        "LLVM_PROFILE_FILE",
        "--kill-seed",
        "515",
        "--output",
        "json",
    ];
    for (require, exits) in [(None, 2), (Some("complete"), 2), (Some("partial"), 0)] {
        let mut args = small.to_vec();
        args.extend(require.iter().flat_map(|require| ["--require", *require]));
        let output = certify(&args).await;
        assert_eq!(code(&output), Some(exits), "{require:?}");
        let report: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("the report is JSON");
        // Whatever is required, the report says what was seen.
        assert_eq!(report["verdict"], "incomplete", "{report}");
        assert_eq!(report["passed"], false, "{report}");
        assert_eq!(report["reports"][0]["verdict"], "incomplete", "{report}");
        let clauses = report["reports"][0]["clauses"].as_array().expect("clauses");
        let unobserved: Vec<_> = clauses
            .iter()
            .filter(|clause| clause["outcome"] == "unobserved")
            .map(|clause| clause["id"].as_str())
            .collect();
        assert_eq!(unobserved, [Some("K-SOURCE")], "{report}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connector_that_breaks_a_clause_exits_one_and_says_which() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    // The SQLite destination needs a path: without one, its handshake fails every clause.
    let output = certify(&[
        binary,
        "--role",
        "destination",
        "--env",
        "LLVM_PROFILE_FILE",
    ])
    .await;
    assert_eq!(code(&output), Some(1));
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("FAIL D-CHECK"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_role_the_connector_does_not_serve_exits_one() {
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    let output = certify(&[
        binary,
        "--role",
        "destination",
        "--env",
        "LLVM_PROFILE_FILE",
    ])
    .await;
    assert_eq!(code(&output), Some(1));
    assert!(!output.stderr.is_empty(), "it says why");
    let json = certify(&[
        binary,
        "--role",
        "destination",
        "--env",
        "LLVM_PROFILE_FILE",
        "--output",
        "json",
    ])
    .await;
    assert_eq!(code(&json), Some(1));
    let report: serde_json::Value =
        serde_json::from_slice(&json.stdout).expect("the report is JSON");
    assert_eq!(report["passed"], false, "{report}");
    assert_eq!(report["reports"][0]["passed"], false, "{report}");
    // Nothing was certified, whatever is required.
    let partial = certify(&[
        binary,
        "--role",
        "destination",
        "--env",
        "LLVM_PROFILE_FILE",
        "--require",
        "partial",
    ])
    .await;
    assert_eq!(code(&partial), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connector_serving_no_role_is_reported_as_certifying_nothing() {
    let binary = example("serve_nothing");
    let binary = binary.to_str().expect("a UTF-8 path");
    let output = certify(&[binary, "--env", "LLVM_PROFILE_FILE", "--output", "json"]).await;
    assert_eq!(code(&output), Some(1));
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("the report is JSON");
    assert_eq!(report["passed"], false, "{report}");
    assert_eq!(
        report["reports"].as_array().map(Vec::len),
        Some(2),
        "{report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connector_serving_one_role_is_certified_in_it_alone() {
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    let config = r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#;
    let args = [binary, "--config", config, "--env", "LLVM_PROFILE_FILE"];
    // Five rows end before a kill lands.
    let output = certify(&[&args[..], &["--require", "partial"]].concat()).await;
    assert_eq!(
        code(&output),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_help_and_the_reports_read_as_they_did() {
    let help = certify(&["--help"]).await;
    insta::assert_snapshot!("help", String::from_utf8_lossy(&help.stdout));
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    for (output, name) in [("plain", "plain_report"), ("json", "json_report")] {
        let certified = certify(&[
            binary,
            "--role",
            "source",
            "--config",
            USERS,
            "--env",
            "LLVM_PROFILE_FILE",
            // Points a load of two rows, committed once, never reaches: after its third
            // commit, and its fifth.
            "--kill-seed",
            "515",
            "--output",
            output,
        ])
        .await;
        insta::assert_snapshot!(name, String::from_utf8_lossy(&certified.stdout));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_destination_binary_that_reads_back_is_certified_in_every_clause() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let config = serde_json::json!({ "path": directory.path().join("store.db") }).to_string();
    let output = certify(&[
        binary,
        "--role",
        "destination",
        "--config",
        &config,
        "--env",
        "LLVM_PROFILE_FILE",
        "--output",
        "json",
    ])
    .await;
    assert_eq!(
        code(&output),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("the report is JSON");
    let clauses = report["reports"][0]["clauses"]
        .as_array()
        .expect("the clauses");
    for id in ["D-COMMIT", "K-DESTINATION"] {
        let clause = clauses
            .iter()
            .find(|clause| clause["id"] == id)
            .expect("the clause is reported");
        assert_eq!(clause["outcome"], "passed", "{id}: {report}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kill_timeout_from_the_command_line_bounds_the_kill_clauses() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let config = serde_json::json!({ "path": directory.path().join("store.db") }).to_string();
    let output = certify(&[
        binary,
        "--role",
        "destination",
        "--config",
        &config,
        "--env",
        "LLVM_PROFILE_FILE",
        "--kill-timeout",
        "0",
        "--output",
        "json",
    ])
    .await;
    assert_eq!(code(&output), Some(1));
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("the report is JSON");
    let clauses = report["reports"][0]["clauses"]
        .as_array()
        .expect("the clauses");
    let killed = clauses
        .iter()
        .find(|clause| clause["id"] == "K-DESTINATION")
        .expect("the clause is reported");
    assert_eq!(killed["outcome"], "failed", "{report}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_certification_that_outlives_its_timeout_fails_what_it_left_and_exits_one() {
    let binary = example("serve_hang");
    let binary = binary.to_str().expect("a UTF-8 path");
    // Its configuration never answers: a connection's own deadline is a minute away.
    for (timeout, roles) in [("2", &["--role", "destination"][..]), ("0", &[])] {
        let args = [binary, "--env", "LLVM_PROFILE_FILE", "--output", "json"];
        let args = [&args[..], roles, &["--timeout", timeout]].concat();
        let began = std::time::Instant::now();
        let output = certify(&args).await;
        assert!(
            began.elapsed() < std::time::Duration::from_secs(30),
            "{timeout}"
        );
        assert_eq!(code(&output), Some(1), "{timeout}");
        let report: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("the report is JSON");
        assert_eq!(report["verdict"], "failed", "{report}");
        let reports = report["reports"].as_array().expect("the reports");
        assert_eq!(
            reports.len(),
            if roles.is_empty() { 2 } else { 1 },
            "{report}"
        );
        for report in reports {
            let clauses = report["clauses"].as_array().expect("the clauses");
            assert!(clauses.len() > 10, "{report}");
            for clause in clauses {
                assert_eq!(clause["outcome"], "failed", "{timeout}: {clause}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_certification_asked_for_no_timeout_runs_unbounded_and_takes_no_timeout_beside() {
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    let config = r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#;
    let args = [binary, "--config", config, "--env", "LLVM_PROFILE_FILE"];
    let unbounded = ["--no-timeout", "--require", "partial"];
    let output = certify(&[&args[..], &unbounded].concat()).await;
    assert_eq!(code(&output), Some(0));
    let both = ["--no-timeout", "--timeout", "5"];
    let output = certify(&[&args[..], &both].concat()).await;
    assert_eq!(code(&output), Some(64));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_timeout_beyond_what_a_clock_holds_is_refused_rather_than_taken_for_none() {
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    let forever = u64::MAX.to_string();
    let output = certify(&[binary, "--timeout", forever.as_str()]).await;
    // Whoever asks for a bound gets one, or is told why not: nothing was certified.
    assert_eq!(code(&output), Some(64));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty(), "it says why");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listening_connector_is_certified_from_the_command_line() {
    let pki = Pki::new("ca");
    let (_connector, endpoint) = listening(&pki).await;
    let host = pki.client("host");
    let ca = pki.ca();
    let (cert, key, ca) = (host.cert.to_str(), host.key.to_str(), ca.to_str());
    let (Some(cert), Some(key), Some(ca)) = (cert, key, ca) else {
        panic!("UTF-8 paths");
    };
    let args = [
        &endpoint,
        "--role",
        "source",
        "--config",
        USERS,
        "--tls-cert",
        cert,
        "--tls-key",
        key,
        "--tls-ca",
        ca,
        // Two rows end before a kill lands.
        "--require",
        "partial",
    ];
    let output = certify(&args).await;
    assert_eq!(
        code(&output),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[tokio::test]
async fn a_wrong_command_line_exits_sixty_four() {
    let tls = [
        "--tls-cert",
        "cert.pem",
        "--tls-key",
        "key.pem",
        "--tls-ca",
        "ca.pem",
    ];
    let no_port = ["grpcs://localhost"]
        .into_iter()
        .chain(tls)
        .collect::<Vec<_>>();
    let tls_with_binary = ["connector"].into_iter().chain(tls).collect::<Vec<_>>();
    let cert_with_binary = ["connector", "--tls-cert", "cert.pem"];
    let key_with_binary = ["connector", "--tls-key", "key.pem"];
    let env_with_endpoint = ["grpcs://localhost:1", "--env", "HOME"]
        .into_iter()
        .chain(tls)
        .collect::<Vec<_>>();
    let cases: [&[&str]; 10] = [
        &[],
        &["connector", "--config", "{not json"],
        &["grpcs://localhost:1"],
        &["connector", "--role", "sink"],
        &no_port,
        &["https://localhost:1"],
        &tls_with_binary,
        &cert_with_binary,
        &key_with_binary,
        &env_with_endpoint,
    ];
    for args in cases {
        let binary = example("serve_reference");
        let binary = binary.to_str().expect("a UTF-8 path");
        let args: Vec<&str> = args
            .iter()
            .map(|arg| if *arg == "connector" { binary } else { arg })
            .collect();
        let output = certify(&args).await;
        assert_eq!(
            code(&output),
            Some(64),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn a_connector_or_configuration_that_cannot_be_read_exits_seventy_four() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let unexecutable = directory.path().join("connector");
    std::fs::write(&unexecutable, "not a program").expect("the file writes");
    let unexecutable = unexecutable.to_str().expect("a UTF-8 path");
    let cases: [&[&str]; 3] = [
        &["/nonexistent/connector"],
        &[binary, "--config-file", "/nonexistent/config.json"],
        &[unexecutable],
    ];
    for args in cases {
        let output = certify(args).await;
        assert_eq!(
            code(&output),
            Some(74),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn an_output_closed_before_it_is_written_ends_quietly() {
    let (reader, writer) = std::io::pipe().expect("a pipe");
    drop(reader);
    let status = Command::new(env!("CARGO_BIN_EXE_rdlt-certify"))
        .arg("--clauses")
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .stdout(writer)
        .status()
        .await
        .expect("rdlt-certify runs");
    assert_eq!(status.code(), Some(0));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_output_that_cannot_be_written_exits_seventy_four() {
    let full = std::fs::File::create("/dev/full").expect("the full device opens");
    let status = Command::new(env!("CARGO_BIN_EXE_rdlt-certify"))
        .arg("--clauses")
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .stdout(full)
        .status()
        .await
        .expect("rdlt-certify runs");
    assert_eq!(status.code(), Some(74));
}

#[tokio::test]
async fn the_clauses_print_as_the_registry_documents_them() {
    let output = certify(&["--clauses"]).await;
    assert_eq!(code(&output), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        rdlt_certify::markdown()
    );
    let help = certify(&["--help"]).await;
    assert_eq!(code(&help), Some(0));
}

#[test]
fn the_committed_clause_documentation_is_generated_from_the_registry() {
    let committed = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/certify/clauses.md"
    ))
    .expect("the clause documentation is committed");
    assert_eq!(
        committed,
        rdlt_certify::markdown(),
        "regenerate it with `rdlt-certify --clauses`"
    );
}

/// A launcher in `directory` that starts two members of its group, one that ignores `SIGTERM`,
/// appends their process ids to `members`, and becomes the example connector `name`.
fn launcher(directory: &std::path::Path, name: &str) -> String {
    let path = directory.join("launcher");
    let script = format!(
        "#!/bin/sh\nsleep 1000 &\necho $! >> '{members}'\n\
         sh -c 'trap \"\" TERM; sleep 1000' &\necho $! >> '{members}'\n\
         exec '{connector}' \"$@\"\n",
        members = directory.join("members").display(),
        connector = example(name).display()
    );
    std::fs::write(&path, script).expect("the launcher writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the launcher is executable");
    path.to_str().expect("a UTF-8 path").to_owned()
}

/// The members the launchers in `directory` started that still live; each is killed, so a
/// failing test leaves nothing behind.
fn surviving(directory: &std::path::Path) -> Vec<i32> {
    let written = std::fs::read_to_string(directory.join("members")).unwrap_or_default();
    let members = written
        .lines()
        .map(|pid| pid.trim().parse::<i32>().expect("a process id"));
    let alive = |member: &i32| {
        let member = nix::unistd::Pid::from_raw(*member);
        let alive = nix::sys::signal::kill(member, None).is_ok();
        nix::sys::signal::kill(member, nix::sys::signal::Signal::SIGKILL).ok();
        alive
    };
    members.filter(alive).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_certification_that_ends_leaves_nothing_its_connectors_started() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launched = launcher(directory.path(), "serve_source");
    let config = r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#;
    let args = [
        launched.as_str(),
        "--config",
        config,
        "--env",
        "LLVM_PROFILE_FILE",
    ];
    let output = certify(&[&args[..], &["--require", "partial"]].concat()).await;
    assert_eq!(
        code(&output),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let started = std::fs::read_to_string(directory.path().join("members")).expect("members");
    assert!(started.lines().count() >= 4, "{started}");
    assert_eq!(surviving(directory.path()), Vec::<i32>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_certification_cut_at_its_timeout_leaves_nothing_its_connectors_started() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    // Its configuration never answers: the certification is cut as a connector waits.
    let launched = launcher(directory.path(), "serve_hang");
    let args = [launched.as_str(), "--role", "destination", "--timeout", "2"];
    let output = certify(&[&args[..], &["--env", "LLVM_PROFILE_FILE"]].concat()).await;
    assert_eq!(code(&output), Some(1));
    assert!(directory.path().join("members").exists());
    assert_eq!(surviving(directory.path()), Vec::<i32>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_or_terminated_certification_stops_its_connectors_before_it_exits() {
    use nix::sys::signal::Signal;
    for (signal, exits) in [(Signal::SIGINT, 130), (Signal::SIGTERM, 143)] {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let launched = launcher(directory.path(), "serve_hang");
        let mut certifying = Command::new(env!("CARGO_BIN_EXE_rdlt-certify"))
            .args([
                launched.as_str(),
                "--role",
                "destination",
                "--env",
                "LLVM_PROFILE_FILE",
            ])
            .env(
                "LLVM_PROFILE_FILE",
                std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
            )
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("rdlt-certify runs");
        // Once a connector has started its members, the certification waits on it.
        let members = directory.path().join("members");
        for _ in 0..600 {
            let started = std::fs::read_to_string(&members).unwrap_or_default();
            if started.lines().count() >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let pid = i32::try_from(certifying.id().expect("it runs")).expect("a process id");
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), signal).expect("it is signalled");
        let status = certifying.wait().await.expect("the certification ends");
        assert_eq!(status.code(), Some(exits), "{signal}");
        assert_eq!(surviving(directory.path()), Vec::<i32>::new(), "{signal}");
    }
}

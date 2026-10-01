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
async fn a_timeout_beyond_what_a_clock_holds_bounds_nothing() {
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    let config = r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#;
    let forever = u64::MAX.to_string();
    let args = [binary, "--config", config, "--env", "LLVM_PROFILE_FILE"];
    let bounded = ["--timeout", forever.as_str(), "--require", "partial"];
    let output = certify(&[&args[..], &bounded].concat()).await;
    assert_eq!(
        code(&output),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
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

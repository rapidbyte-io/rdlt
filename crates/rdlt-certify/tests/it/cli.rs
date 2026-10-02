//! The `rdlt-certify` binary: what it certifies, how it reports, and its exit codes.

use std::process::Output;

use rdlt_testkit::tls::Pki;
use tokio::process::Command;

use crate::listening::listening;
use crate::spawned::example;

/// Runs `rdlt-certify` with `args`, as the tests of what it certifies run it: a configuration
/// given after `--config` is written to its standard input, since it takes none on its command
/// line, and a connector's binary is one the tests trust, unless `args` say how it is confined.
async fn certify(args: &[&str]) -> Output {
    let config = args.iter().position(|arg| *arg == "--config");
    let mut given: Vec<&str> = args.to_vec();
    let config = config.map(|at| {
        let config = given.remove(at + 1);
        given.splice(at..=at, ["--config-file", "-"]);
        config
    });
    let spawned = given
        .first()
        .is_some_and(|target| std::path::Path::new(target).is_file());
    let confined = given
        .iter()
        .any(|arg| arg.starts_with("--grant") || *arg == "--sandboxed");
    given.retain(|arg| *arg != "--sandboxed");
    if spawned && !confined {
        given.push("--trusted");
    }
    certify_given(&given, config.unwrap_or_default(), &[]).await
}

/// Runs `rdlt-certify` with exactly `args`, `input` on its standard input and `env` in its
/// environment.
async fn certify_given(args: &[&str], input: &str, env: &[(&str, &str)]) -> Output {
    use tokio::io::AsyncWriteExt as _;
    let mut certifying = Command::new(env!("CARGO_BIN_EXE_rdlt-certify"))
        .args(args)
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .envs(env.iter().copied())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0)
        .spawn()
        .expect("rdlt-certify runs");
    crate::guarded(&certifying);
    let mut stdin = certifying.stdin.take().expect("its input is piped");
    // It may end without reading its input: a closed pipe is no failure of the test.
    stdin.write_all(input.as_bytes()).await.ok();
    drop(stdin);
    certifying
        .wait_with_output()
        .await
        .expect("rdlt-certify ends")
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
    // A stream of no rows: its read ends within its first credit, sends no checkpoint, and
    // ends before any kill lands. The clauses that need those apply, and are not observed;
    // none of them waits to see it.
    let empty = [
        binary,
        "--role",
        "source",
        "--config",
        r#"{"streams": {"users": []}}"#,
        "--env",
        "LLVM_PROFILE_FILE",
        "--kill-seed",
        "515",
        "--output",
        "json",
    ];
    // Complete is what is required unless part is: the command line's own tests say so.
    let complete = [&empty[..], &["--require", "complete"]].concat();
    let partial = [&empty[..], &["--require", "partial"]].concat();
    let (complete, partial) = tokio::join!(certify(&complete), certify(&partial));
    for (output, exits) in [(complete, 2), (partial, 0)] {
        assert_eq!(code(&output), Some(exits));
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
            .filter_map(|clause| clause["id"].as_str())
            .collect();
        let unseen = ["P-CREDIT", "S-RESUME", "S-PARTITION", "K-SOURCE"];
        assert_eq!(unobserved, unseen, "{report}");
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
async fn a_certification_that_outlives_its_timeout_fails_where_it_was_cut_and_exits_one() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launched = launcher(directory.path(), "serve_hang");
    // Its configuration never answers: a connection's own deadline is a minute away.
    for (timeout, roles) in [("2", &["--role", "destination"][..]), ("0", &[])] {
        std::fs::remove_file(directory.path().join("members")).ok();
        let args = [
            launched.as_str(),
            "--env",
            "LLVM_PROFILE_FILE",
            "--output",
            "json",
        ];
        let args = [&args[..], roles, &["--timeout", timeout]].concat();
        let began = std::time::Instant::now();
        let output = certify(&args).await;
        assert!(
            began.elapsed() < std::time::Duration::from_secs(120),
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
            // The clause the bound cut fails; those it never reached were not observed.
            assert_eq!(clauses[0]["outcome"], "failed", "{timeout}: {report}");
            for clause in &clauses[1..] {
                assert_eq!(clause["outcome"], "unobserved", "{timeout}: {clause}");
            }
        }
        // A bound that has passed starts no connector.
        let spawned = directory.path().join("members").exists();
        assert_eq!(spawned, timeout != "0", "{timeout}");
        assert_eq!(surviving(directory.path()), Vec::<i32>::new(), "{timeout}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_certification_asked_for_no_timeout_runs_unbounded_and_takes_no_timeout_beside() {
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    // A stream of no rows: its read ends within its first credit, so nothing waits to see
    // it hold to one.
    let config = r#"{"seed": 7, "streams": [{"name": "events", "rows": 0}]}"#;
    let args = [binary, "--config", config, "--env", "LLVM_PROFILE_FILE"];
    let unbounded = ["--no-timeout", "--require", "partial", "--role", "source"];
    let unbounded = [&unbounded[..], &["--kill-seed", "1"]].concat();
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
    let cases: [&[&str]; 13] = [
        &[],
        // No configuration is taken on the command line, where every user can read it.
        &["connector", "--trusted", "--config", "{}"],
        &["connector", "--trusted", "--grant-network"],
        &["connector", "--trusted", "--grant-read", "/usr"],
        &["connector", "--trusted", "--grant-write", "/tmp"],
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

/// The members the launchers in `directory` started that still live once whatever adopted
/// them has had time to reap them; each is killed, so a failing test leaves nothing behind.
fn surviving(directory: &std::path::Path) -> Vec<i32> {
    let written = std::fs::read_to_string(directory.join("members")).unwrap_or_default();
    let members: Vec<i32> = written
        .lines()
        .map(|pid| pid.trim().parse().expect("a process id"))
        .collect();
    let alive =
        |member: &i32| nix::sys::signal::kill(nix::unistd::Pid::from_raw(*member), None).is_ok();
    // Ended already, a member answers until it is reaped, which is not this process's to do.
    let until = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while members.iter().any(alive) && std::time::Instant::now() < until {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let left: Vec<i32> = members.into_iter().filter(alive).collect();
    for member in &left {
        let member = nix::unistd::Pid::from_raw(*member);
        nix::sys::signal::kill(member, nix::sys::signal::Signal::SIGKILL).ok();
    }
    left
}

#[tokio::test(flavor = "multi_thread")]
async fn a_certification_that_ends_leaves_nothing_its_connectors_started() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launched = launcher(directory.path(), "serve_source");
    // A stream of no rows: its read ends within its first credit, so nothing waits to see
    // it hold to one.
    let config = r#"{"seed": 7, "streams": [{"name": "events", "rows": 0}]}"#;
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
    let heard = [
        (Signal::SIGINT, 130),
        (Signal::SIGTERM, 143),
        (Signal::SIGHUP, 129),
        (Signal::SIGQUIT, 131),
    ];
    for (signal, exits) in heard {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let launched = launcher(directory.path(), "serve_hang");
        let mut certifying = Command::new(env!("CARGO_BIN_EXE_rdlt-certify"))
            .args([
                launched.as_str(),
                "--role",
                "destination",
                "--env",
                "LLVM_PROFILE_FILE",
                "--trusted",
            ])
            .env(
                "LLVM_PROFILE_FILE",
                std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
            )
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .process_group(0)
            .spawn()
            .expect("rdlt-certify runs");
        crate::guarded(&certifying);
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

/// A launcher in `directory` that becomes no connector: a process that answers nothing and
/// ignores being asked to stop, with a member that does the same, whose ids it appends to
/// `members`.
fn stubborn(directory: &std::path::Path) -> String {
    let path = directory.join("launcher");
    let script = format!(
        "#!/bin/sh\ntrap '' TERM\nsleep 1000 &\necho $! >> '{members}'\n\
         echo $$ >> '{members}'\nexec sleep 1000\n",
        members = directory.join("members").display(),
    );
    std::fs::write(&path, script).expect("the launcher writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the launcher is executable");
    path.to_str().expect("a UTF-8 path").to_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_interrupt_kills_what_a_certification_spawned_and_ends_it_at_once() {
    use nix::sys::signal::Signal;
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launched = stubborn(directory.path());
    let mut certifying = Command::new(env!("CARGO_BIN_EXE_rdlt-certify"))
        .args([launched.as_str(), "--role", "destination", "--trusted"])
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .process_group(0)
        .spawn()
        .expect("rdlt-certify runs");
    crate::guarded(&certifying);
    let members = directory.path().join("members");
    for _ in 0..600 {
        let started = std::fs::read_to_string(&members).unwrap_or_default();
        if started.lines().count() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let pid = i32::try_from(certifying.id().expect("it runs")).expect("a process id");
    let pid = nix::unistd::Pid::from_raw(pid);
    nix::sys::signal::kill(pid, Signal::SIGINT).expect("it is interrupted");
    // What it spawned ignores being asked to stop, and has ten seconds before it is killed.
    let stopping = std::time::Duration::from_secs(1);
    let waited = tokio::time::timeout(stopping, certifying.wait()).await;
    assert!(
        waited.is_err(),
        "the certification ended before its connector's grace"
    );
    let began = std::time::Instant::now();
    nix::sys::signal::kill(pid, Signal::SIGTERM).expect("it is signalled again");
    let ending = std::time::Duration::from_secs(60);
    let status = tokio::time::timeout(ending, certifying.wait()).await;
    let took = began.elapsed();
    let left = surviving(directory.path());
    let status = status.expect("it ends").expect("it is waited for");
    // Asked twice, it waits for nothing: what it spawned is killed, and seen to be.
    assert!(took < std::time::Duration::from_secs(6), "{took:?}");
    assert_eq!(left, Vec::<i32>::new());
    // It exits as the last signal it heard would have ended it.
    assert_eq!(status.code(), Some(143));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_endpoint_refused_is_not_repeated_in_what_is_reported() {
    let tls = [
        "--tls-cert",
        "cert.pem",
        "--tls-key",
        "key.pem",
        "--tls-ca",
        "ca.pem",
    ];
    for endpoint in [
        "grpcs://svc:hunter2@connector:7443",
        "grpcs://connector:7443/?token=hunter2",
        "grpcs://connector:7443#hunter2",
    ] {
        let args: Vec<&str> = [endpoint].into_iter().chain(tls).collect();
        let output = certify(&args).await;
        assert_eq!(code(&output), Some(64), "{endpoint}");
        let said = [output.stdout, output.stderr].concat();
        let said = String::from_utf8_lossy(&said);
        assert!(!said.contains("hunter2"), "{endpoint}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_target_that_is_no_file_is_not_repeated_in_what_is_reported() {
    // Each an endpoint mistyped, so read as a path, with a credential in it.
    for target in [
        "grpcs:/svc:hunter2@connector:7443",
        "grpcs//svc:hunter2@connector:7443",
        "svc:hunter2@connector:7443",
    ] {
        let output = certify(&[target]).await;
        assert_eq!(code(&output), Some(74), "{target}");
        let said = [output.stdout, output.stderr].concat();
        let said = String::from_utf8_lossy(&said);
        assert!(!said.contains("hunter2"), "{target}: {said}");
    }
}

#[tokio::test]
async fn a_configuration_that_is_no_json_document_is_refused_without_being_quoted() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let args = [binary, "--trusted", "--config-file", "-"];
    for unusable in [
        "{not json: hunter2-canary",
        "",
        r#"{"a": "${vault:hunter2-canary}"}"#,
    ] {
        let output = certify_given(&args, unusable, &[]).await;
        assert_eq!(code(&output), Some(64), "{unusable}");
        let said = String::from_utf8_lossy(&output.stderr);
        assert!(said.contains("standard input"), "{said}");
        assert!(
            !said.contains("hunter2") && output.stdout.is_empty(),
            "{said}"
        );
    }
}

#[tokio::test]
async fn a_configuration_is_read_up_to_its_bound_and_refused_as_too_large_beyond() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let args = [binary, "--trusted", "--config-file", "-"];
    let bound = rdlt_host::limits::CONFIG_BYTES;
    // An object of one text field, `{"pad":"…"}`, of `bytes` in all.
    let padded = |bytes: usize| format!(r#"{{"pad":"{}"}}"#, "x".repeat(bytes - 10));
    let within = certify_given(&args, &padded(bound), &[]).await;
    let said = String::from_utf8_lossy(&within.stderr);
    assert!(!said.contains("standard input"), "{said}");
    let beyond = certify_given(&args, &padded(bound + 1), &[]).await;
    assert_eq!(code(&beyond), Some(64));
    let said = String::from_utf8_lossy(&beyond.stderr);
    assert!(
        said.contains(&format!("larger than {bound} bytes")),
        "{said}"
    );
}

/// A canary no output may hold, and the reports of a destination certified with it, as text
/// and as JSON: the SQLite destination is told a path it cannot open, and says so.
async fn reports_with_a_secret(
    config: &str,
    env: &[(&str, &str)],
    allowed: &[&str],
) -> Vec<String> {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let mut reports = Vec::new();
    for output in ["plain", "json"] {
        let args = [
            binary,
            "--trusted",
            "--role",
            "destination",
            "--config-file",
            "-",
            "--output",
            output,
        ];
        let args = [&args[..], allowed].concat();
        let certified = certify_given(&args, config, env).await;
        assert_eq!(code(&certified), Some(1), "{output}");
        let stdout = String::from_utf8_lossy(&certified.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&certified.stderr).into_owned();
        reports.push(format!("{stdout}{stderr}"));
    }
    reports
}

#[tokio::test(flavor = "multi_thread")]
async fn a_secret_the_configuration_refers_to_is_in_no_report_however_the_connector_says_it() {
    let canary = "hunter2-CANARY-0123456789";
    let directory = tempfile::tempdir().expect("a temporary directory");
    let file = directory.path().join("secret");
    std::fs::write(&file, format!("{canary}\n")).expect("the secret writes");
    std::fs::set_permissions(&file, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .expect("the secret is private");
    let by_file = format!("${{file:{}}}", file.display());
    let dir = directory.path().to_str().expect("a UTF-8 path");
    let allowed = ["--secret-env", "RDLT_TEST_CANARY", "--secret-dir", dir];
    for reference in ["${env:RDLT_TEST_CANARY}", by_file.as_str()] {
        // A path that holds the secret, which the connector quotes back in each failure.
        let config = serde_json::json!({ "path": format!("/nonexistent/{reference}/store.db") });
        let env = [("RDLT_TEST_CANARY", canary)];
        for report in reports_with_a_secret(&config.to_string(), &env, &allowed).await {
            assert!(
                report.contains("/nonexistent/***/store.db"),
                "{reference}: {report}"
            );
            assert!(
                !report.contains("hunter2") && !report.contains("CANARY"),
                "{report}"
            );
        }
    }
    // The same path written out is no secret, and is said: the scrub is of what is referred to.
    let literal = serde_json::json!({ "path": format!("/nonexistent/{canary}/store.db") });
    for report in reports_with_a_secret(&literal.to_string(), &[], &[]).await {
        assert!(report.contains(canary), "{report}");
    }
}

#[tokio::test]
async fn a_secret_that_does_not_resolve_ends_the_certification_before_any_connector_runs() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let args = [binary, "--trusted", "--config-file", "-"];
    let config = r#"{"path": "hunter2-canary ${env:RDLT_TEST_UNSET_VARIABLE}"}"#;
    let output = certify_given(&args, config, &[]).await;
    assert_eq!(code(&output), Some(64));
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(said.contains("config field path"), "{said}");
    assert!(
        !said.contains("hunter2") && !said.contains("RDLT_TEST_UNSET"),
        "{said}"
    );
    assert!(output.stdout.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_binary_is_certified_inside_a_sandbox_unless_it_is_said_to_be_trusted() {
    let binary = example("serve_source");
    let binary = binary.to_str().expect("a UTF-8 path");
    let config = r#"{"seed": 7, "streams": [{"name": "events", "rows": 5}]}"#;
    let args = [
        binary,
        "--config-file",
        "-",
        "--require",
        "partial",
        "--kill-seed",
        "515",
    ];
    let output = certify_given(&args, config, &[]).await;
    let said = String::from_utf8_lossy(&output.stderr);
    let usable = rdlt_host::Bubblewrap::new().usable();
    if usable.is_ok() {
        // The generator needs nothing of its host: it keeps every clause in a sandbox.
        assert_eq!(code(&output), Some(0), "{said}");
    } else {
        rdlt_testkit::process::without_sandbox(&usable.expect_err("none is made"));
        // No sandbox here: nothing is run, and the way to run a trusted binary is said.
        assert_eq!(code(&output), Some(74), "{said}");
        assert!(
            said.contains("--trusted") && output.stdout.is_empty(),
            "{said}"
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_sandboxed_destination_writes_only_where_it_is_granted() {
    if let Err(unusable) = rdlt_host::Bubblewrap::new().usable() {
        rdlt_testkit::process::without_sandbox(&unusable);
        return;
    }
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let store = directory.path().to_str().expect("a UTF-8 path");
    let config = serde_json::json!({ "path": directory.path().join("store.db") }).to_string();
    let common = [
        binary,
        "--role",
        "destination",
        "--config-file",
        "-",
        "--output",
        "json",
    ];
    // Confined, the destination finds no such directory, and every clause fails on it.
    let confined = certify_given(&common, &config, &[]).await;
    assert_eq!(code(&confined), Some(1));
    assert!(!directory.path().join("store.db").exists());
    // Granted the directory, it writes its store there.
    let granted = [&common[..], &["--grant-write", store]].concat();
    let granted = certify_given(&granted, &config, &[]).await;
    let report: serde_json::Value =
        serde_json::from_slice(&granted.stdout).expect("the report is JSON");
    assert_ne!(report["verdict"], "failed", "{report}");
    assert!(directory.path().join("store.db").exists());
    // Not where a secret directory it names lies: whoever writes there decides its secrets.
    let guarded = [
        &common[..],
        &["--grant-write", store, "--secret-dir", store],
    ]
    .concat();
    let guarded = certify_given(&guarded, &config, &[]).await;
    assert_eq!(code(&guarded), Some(1));
    let said = String::from_utf8_lossy(&guarded.stdout);
    assert!(said.contains("cannot be sandboxed"), "{said}");
}

#[tokio::test]
async fn a_reference_to_a_secret_the_command_line_does_not_allow_is_refused_unread() {
    let binary = example("serve_reference");
    let binary = binary.to_str().expect("a UTF-8 path");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let file = directory.path().join("secret");
    std::fs::write(&file, "hunter2-file").expect("the secret writes");
    std::fs::set_permissions(&file, std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .expect("the secret is private");
    let env = [("RDLT_TEST_CANARY", "hunter2-env")];
    for reference in [
        "${env:RDLT_TEST_CANARY}".to_owned(),
        format!("${{file:{}}}", file.display()),
    ] {
        let config = serde_json::json!({ "path": reference }).to_string();
        let args = [binary, "--trusted", "--config-file", "-"];
        let output = certify_given(&args, &config, &env).await;
        assert_eq!(code(&output), Some(64), "{reference}");
        let said = String::from_utf8_lossy(&output.stderr);
        assert!(said.contains("config field path"), "{said}");
        assert!(
            !said.contains("hunter2") && !said.contains("RDLT_TEST") && !said.contains("secret"),
            "{said}"
        );
    }
}

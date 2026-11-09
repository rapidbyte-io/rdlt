//! The command line's contract: an exit code for every kind of failure, results on standard
//! output and notes on standard error, JSON where asked, and a closed pipe ending quietly.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, BufReader};

use crate::support::{Scratch, guarded, output_of, said};
use crate::verbs::{memory_document, reference_connectors};

fn contract_connectors() -> Scratch {
    reference_connectors()
        .connector("serve_failing", "failing")
        .connector("serve_log", "log")
}

/// A document of `source` (YAML of a `source:` value) into SQLite, its rest `extra`.
fn document(scratch: &Scratch, name: &str, source: &str, extra: &str) -> String {
    let database = scratch.path().join(format!("{name}.db"));
    let text = format!(
        "version: 1\npipeline: {name}\nsource: {source}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {} }}\n{extra}",
        database.display()
    );
    scratch
        .document(&format!("{name}.yaml"), &text)
        .to_str()
        .expect("a UTF-8 path")
        .to_owned()
}

/// A document whose destination connects and then refuses every session.
fn refused_store(scratch: &Scratch) -> String {
    let text = "version: 1\npipeline: refused\nsource: { connector: io.rapidbyte.failing, config: { fail: never } }\ndestination:\n  connector: io.rapidbyte.failing\n  config: { store: refused }\nstreams: [{ name: rows }]\n";
    scratch
        .document("refused.yaml", text)
        .to_str()
        .expect("a UTF-8 path")
        .to_owned()
}

/// The documents that fail as each kind of failure, with the code each exits.
fn failing_documents(scratch: &Scratch) -> [(&'static str, String, i32); 4] {
    let failing =
        |fail: &str| format!("{{ connector: io.rapidbyte.failing, config: {{ fail: {fail} }} }}");
    let memory = |rows: &str| {
        format!("{{ connector: io.rapidbyte.memory, config: {{ streams: {{ users: {rows} }} }} }}")
    };
    [
        (
            "config",
            document(
                scratch,
                "config",
                &failing("check"),
                "streams: [{ name: rows }]\n",
            ),
            2,
        ),
        (
            "source",
            document(
                scratch,
                "source",
                &failing("read"),
                "streams: [{ name: rows }]\n",
            ),
            4,
        ),
        ("destination", refused_store(scratch), 5),
        (
            "transform",
            document(
                scratch,
                "transform",
                &memory("[{ name: ada }]"),
                "streams: [{ name: users, transform: [{ sql: \"SELECT CAST(name AS BIGINT) AS n FROM input\" }] }]\n",
            ),
            6,
        ),
    ]
}

#[tokio::test]
async fn every_kind_of_failure_exits_with_its_code_and_reports_as_json() {
    let scratch = contract_connectors();
    for (kind, document, code) in failing_documents(&scratch) {
        let output = scratch.rdlt(&["run", &document, "--output", "json"]).await;
        assert_eq!(
            output.status.code(),
            Some(code),
            "{kind}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let said: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("one JSON value on stdout");
        let error = if said["error"].is_object() {
            &said["error"]
        } else {
            &said["outcome"]["error"]
        };
        assert!(error["kind"].is_string(), "{kind}: {said}");
    }
}

#[tokio::test]
async fn a_schema_that_does_not_fit_its_policy_exits_3_and_what_cannot_be_read_74() {
    let scratch = contract_connectors();
    let memory = |rows: &str| {
        format!("{{ connector: io.rapidbyte.memory, config: {{ streams: {{ users: {rows} }} }} }}")
    };
    let frozen = "defaults: { schema: { policy: freeze } }\nstreams: [{ name: users }]\n";
    let first = document(&scratch, "schema", &memory("[{ id: 1 }]"), frozen);
    assert_eq!(scratch.rdlt(&["run", &first]).await.status.code(), Some(0));
    let evolved = document(&scratch, "schema", &memory("[{ id: 2, extra: 3 }]"), frozen);
    assert_eq!(
        scratch.rdlt(&["run", &evolved]).await.status.code(),
        Some(3)
    );
    assert_eq!(
        scratch.rdlt(&["run", "absent.yaml"]).await.status.code(),
        Some(74),
        "a document that cannot be read"
    );
    assert_eq!(
        scratch.rdlt(&["run", "--report"]).await.status.code(),
        Some(64)
    );
    let unreadable_tls = [
        "check",
        &first,
        "--tls-cert",
        "/nonexistent/cert.pem",
        "--tls-key",
        "/nonexistent/key.pem",
        "--tls-ca",
        "/nonexistent/ca.pem",
    ];
    assert_eq!(
        scratch.rdlt(&unreadable_tls).await.status.code(),
        Some(74),
        "a TLS file that cannot be read"
    );
}

/// Export set up from the `OTEL_*` variables needs the runtime, and flushing at the end must
/// neither fail nor hold the process: the collector here refuses every connection.
#[tokio::test]
async fn a_verb_with_a_collector_named_exports_and_ends_by_its_own_outcome() {
    let scratch = Scratch::new();
    let mut command = scratch.command(&["connectors", "--output", "json"]);
    command.env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1");
    let child = command.spawn().expect("rdlt starts");
    guarded(&child);
    let output = tokio::time::timeout(Duration::from_secs(120), child.wait_with_output())
        .await
        .expect("it ends within its bound")
        .expect("it ends");
    let said = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "{said}");
    assert!(!said.contains("rdlt:"), "no failure is said: {said}");
}

/// A plain failure gives the cause JSON holds, whichever verb or stage it failed at.
#[tokio::test]
async fn a_plain_failure_keeps_the_cause_its_json_holds() {
    let scratch = contract_connectors();
    let unreachable =
        "{ connector: io.rapidbyte.memory, config: { streams: { users: [{ id: 1 }] } } }";
    let database = format!("{}/absent/plain.db", scratch.path().display());
    let unwritable = scratch.document(
        "plain.yaml",
        &format!(
            "version: 1\npipeline: plain\nsource: {unreachable}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {database} }}\nstreams: [{{ name: users }}]\n"
        ),
    );
    let failing = "{ connector: io.rapidbyte.failing, config: { fail: read } }";
    let read = document(
        &scratch,
        "plain-read",
        failing,
        "streams: [{ name: rows }]\n",
    );
    for doc in [unwritable.to_str().expect("a UTF-8 path"), read.as_str()] {
        let json = scratch.rdlt(&["run", doc, "--output", "json"]).await;
        let said: serde_json::Value = serde_json::from_slice(&json.stdout).expect("JSON");
        let causes = said["error"]["causes"].as_array().expect("causes");
        assert!(!causes.is_empty(), "{said}");
        let plain = scratch.rdlt(&["run", doc, "--output", "plain"]).await;
        let text = String::from_utf8_lossy(&plain.stderr);
        for cause in causes {
            let cause = cause.as_str().expect("a cause is text");
            assert!(text.contains(cause), "{doc}: {cause:?} not in {text}");
        }
    }
}

/// Logs reach standard error at the level asked, and only then; a collector that cannot be set
/// up is said there, the verb going on without it.
#[tokio::test]
async fn logs_are_written_at_the_level_asked_and_a_collector_that_cannot_be_set_up_is_said() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "logs", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let debug = scratch
        .rdlt(&["run", document, "--log-level", "debug", "--output", "json"])
        .await;
    let debug = String::from_utf8_lossy(&debug.stderr);
    assert!(debug.contains("DEBUG"), "{debug}");
    let warned = scratch.rdlt(&["run", document, "--output", "json"]).await;
    assert!(!String::from_utf8_lossy(&warned.stderr).contains("DEBUG"));
    let mut command = scratch.command(&["connectors", "--output", "json"]);
    command
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "https://127.0.0.1:1")
        .env("OTEL_EXPORTER_OTLP_CERTIFICATE", "/nonexistent/ca.pem");
    let refused = output_of(&mut command).await;
    assert_eq!(refused.status.code(), Some(0), "{refused:?}");
    let said = String::from_utf8_lossy(&refused.stderr);
    assert!(said.contains("rdlt: "), "the failure is said: {said}");
}

/// Standard output that cannot be written, not closed, fails the verb.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_standard_output_that_cannot_be_written_exits_74() {
    let scratch = reference_connectors();
    let full = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .expect("the full device opens");
    let mut command = scratch.command(&["connectors", "--output", "json"]);
    command.stdout(full);
    let output = output_of(&mut command).await;
    assert_eq!(output.status.code(), Some(74), "{output:?}");
}

/// `--log-level` holds with a collector named, and a stderr that is no terminal gets no colour.
#[tokio::test]
async fn the_log_level_holds_under_export_and_redirected_stderr_has_no_colour() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "logged", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let mut command = scratch.command(&["run", document, "--log-level", "error"]);
    command.env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1");
    let output = output_of(&mut command).await;
    let said = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "{said}");
    assert!(
        !said.contains('\u{1b}'),
        "colour on a redirected stderr: {said:?}"
    );
    assert!(
        !said.contains(" INFO ") && !said.contains(" WARN "),
        "under the level asked for: {said}"
    );
}

/// An events file that cannot be written ends the writing and not the run, which exits 74 by
/// the time it is over, and JSON's one value on standard output says so.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn an_events_file_that_fails_leaves_the_run_to_finish_and_exits_74() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "unwritten", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let output = scratch
        .rdlt(&["run", document, "--events", "/dev/full", "--output", "json"])
        .await;
    assert_eq!(
        output.status.code(),
        Some(74),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let said: serde_json::Value = serde_json::from_slice(&output.stdout).expect("one JSON value");
    assert_eq!(said["report"]["status"], "succeeded", "{said}");
    assert_eq!(said["error"]["kind"], "io", "{said}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("rdlt: "),
        "it is said"
    );
}

/// A reset opens the destination, which fences the run in progress: a commit makes the stream
/// one the reset can name, and the run's next commit answers that it is fenced.
#[tokio::test]
async fn a_newer_session_fences_a_running_pipeline_which_exits_7() {
    let scratch = contract_connectors();
    let following = document(
        &scratch,
        "fenced",
        "{ connector: io.rapidbyte.log, config: { seed: 5, streams: [{ name: events, partitions: 1, messages: 100000000, per_second: 1000, bounded: false }] } }",
        "streams: [{ name: events, read: incremental }]\ncommit: { every: 1s }\nuntil: forever\n",
    );
    let mut running = scratch
        .command(&["run", &following])
        .spawn()
        .expect("rdlt starts");
    guarded(&running);
    let mut stderr = BufReader::new(running.stderr.take().expect("piped")).lines();
    said(&mut stderr, "committed").await;
    let reset = scratch
        .rdlt(&["state", "reset", &following, "events"])
        .await;
    assert_eq!(
        reset.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&reset.stderr)
    );
    let status = tokio::time::timeout(Duration::from_secs(120), running.wait())
        .await
        .expect("the fenced run ends")
        .expect("it ends");
    assert_eq!(status.code(), Some(7));
}

#[tokio::test]
async fn results_go_to_stdout_and_notes_to_stderr() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "split", "[{ id: 1 }]");
    let document = document.to_str().expect("a UTF-8 path");
    let plain = scratch.rdlt(&["run", document, "--output", "plain"]).await;
    let out = String::from_utf8_lossy(&plain.stdout);
    let err = String::from_utf8_lossy(&plain.stderr);
    assert_eq!(out.lines().count(), 1, "one result line: {out}");
    assert!(out.starts_with("Succeeded"), "{out}");
    assert!(err.contains("committed"), "progress is human output: {err}");
    let checked = scratch.rdlt(&["check", document, "--output", "json"]).await;
    let mut checked: serde_json::Value = serde_json::from_slice(&checked.stdout).unwrap();
    checked["source"]["placement"]["process"]["path"] = serde_json::json!("<path>");
    checked["destination"]["placement"]["process"]["path"] = serde_json::json!("<path>");
    insta::assert_snapshot!(
        "check_json",
        serde_json::to_string_pretty(&checked).unwrap()
    );
}

/// A closed pipe on the events leaves the run to finish by its outcome.
#[tokio::test]
async fn a_closed_event_pipe_leaves_the_run_to_finish() {
    let scratch = reference_connectors();
    let rows: Vec<String> = (0..200).map(|id| format!("{{ id: {id} }}")).collect();
    let document = memory_document(&scratch, "piped", &format!("[{}]", rows.join(", ")));
    let mut child = scratch
        .command(&["run", document.to_str().unwrap(), "--events", "-"])
        .spawn()
        .expect("rdlt starts");
    guarded(&child);
    drop(child.stdout.take());
    let output = tokio::time::timeout(Duration::from_secs(120), child.wait_with_output())
        .await
        .expect("it ends")
        .expect("it ends");
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("rdlt:"),
        "no error is said"
    );
}

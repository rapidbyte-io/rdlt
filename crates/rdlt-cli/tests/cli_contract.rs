//! The compatibility contract, pinned against the BINARY: argument
//! spellings, the stdout/stderr split, and exit codes. These are what
//! scripts depend on; the 036 re-architecture must be invisible to
//! them, and every later renderer change answers to this file.

use std::process::Command;

fn rdlt() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rdlt"))
}

/// A malformed invocation exits 64 (the historical usage code), with
/// nothing on stdout.
#[test]
fn bad_usage_exits_64_and_stdout_stays_clean() {
    let out = rdlt().output().expect("spawn");
    assert_eq!(out.status.code(), Some(64), "no subcommand");
    assert!(out.stdout.is_empty(), "usage text never lands on stdout");

    let out = rdlt().arg("frobnicate").output().expect("spawn");
    assert_eq!(out.status.code(), Some(64), "unknown subcommand");
}

/// `--help` and `--version` exit 0 — clap conventions, newly promised.
#[test]
fn help_and_version_exit_zero() {
    let out = rdlt().arg("--help").output().expect("spawn");
    assert_eq!(out.status.code(), Some(0));
    let out = rdlt().arg("--version").output().expect("spawn");
    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stdout).contains(env!("CARGO_PKG_VERSION")));
}

/// A spec file the filesystem refuses exits 74; a spec that parses but
/// refuses validation exits 2. Both report on stderr only.
#[test]
fn io_and_config_failures_keep_their_codes() {
    let out = rdlt()
        .args(["run", "definitely-missing.yaml"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(74));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("error: reading"));

    let dir = tempfile::tempdir().expect("tempdir");
    let bad = dir.path().join("bad.yaml");
    std::fs::write(&bad, "pipeline: p\nsource:\n  file:\n    streams: []\n").expect("write");
    let out = rdlt().arg("run").arg(&bad).output().expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{:?}", out);
    assert!(out.stdout.is_empty());
}

fn fresh_pipeline() -> (tempfile::TempDir, std::path::PathBuf) {
    // Fresh per phase: the file source's cursor knows a fully-read
    // file, so re-running against one workdir reads zero rows — which
    // is correct engine behaviour and would vacuously pass the
    // quiet/verbose assertions below.
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("rows.jsonl");
    std::fs::write(&data, "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n").expect("write");
    let spec = dir.path().join("pipeline.yaml");
    std::fs::write(
        &spec,
        format!(
            "pipeline: contract\nworkdir: {}\nsource:\n  file:\n    streams:\n      - name: events\n        format: jsonl\n        path: {}\ndestination:\n  file:\n    path: {}\n    format: jsonl\n",
            dir.path().join(".rdlt").display(),
            data.display(),
            dir.path().join("out").display()
        ),
    )
    .expect("write");
    (dir, spec)
}

/// The whole run contract at once: `rdlt run <spec>` succeeds, the
/// event feed's frozen lines land on STDERR, the report JSON is the
/// ONLY thing on stdout and parses with the expected totals, and
/// `--report` moves it to a file leaving stdout empty. Also the
/// verbosity gates: `-q` silences the feed, `-v` adds read lines.
#[test]
fn a_real_run_holds_the_stdout_stderr_and_report_contract() {
    let (_dir, spec) = fresh_pipeline();
    let out = rdlt().arg("run").arg(&spec).output().expect("spawn");
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    let report: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout is exactly the report JSON: {e}\n{stdout}"));
    assert_eq!(report["tables"]["events"]["rows"], 3);
    assert!(stderr.contains("-> stream events started"), "{stderr}");
    assert!(stderr.contains("events: +3 rows"), "{stderr}");
    assert!(stderr.contains("commit 1 ok"), "{stderr}");
    // Heartbeats and 036 detail stay OUT of the default feed.
    assert!(!stderr.contains("read 3 rows"), "{stderr}");

    // Quiet: stderr carries nothing at all for a clean run.
    let (_dir, spec) = fresh_pipeline();
    let out = rdlt()
        .args(["-q", "run"])
        .arg(&spec)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&out.stderr).is_empty(),
        "quiet means quiet: {:?}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Verbose: the read/commit-start lines appear.
    let (_dir, spec) = fresh_pipeline();
    let out = rdlt()
        .args(["-v", "run"])
        .arg(&spec)
        .output()
        .expect("spawn");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("read 3 rows"), "{stderr}");
    assert!(stderr.contains("commit 1 starting"), "{stderr}");

    // --report: the JSON moves to the file; stdout is empty.
    let (dir, spec) = fresh_pipeline();
    let report_path = dir.path().join("report.json");
    let out = rdlt()
        .arg("run")
        .arg(&spec)
        .arg("--report")
        .arg(&report_path)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0));
    assert!(out.stdout.is_empty(), "--report leaves stdout empty");
    let written: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&report_path).expect("read"))
            .expect("report file parses");
    assert_eq!(written["pipeline"], "contract");
}

/// `validate` runs the real gates and nothing after them: a valid
/// pipeline says so and exits 0 without touching the source; an
/// invalid one carries the same exit code a run would.
#[test]
fn validate_gates_without_running() {
    let (_dir, spec) = fresh_pipeline();
    let out = rdlt().arg("validate").arg(&spec).output().expect("spawn");
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    assert!(out.stdout.is_empty(), "validate writes no machine output");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("ok: pipeline contract is valid"),
        "{:?}",
        String::from_utf8_lossy(&out.stderr)
    );

    // -q silences the ok line; the exit code still answers.
    let (_dir, spec) = fresh_pipeline();
    let out = rdlt()
        .args(["-q", "validate"])
        .arg(&spec)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stderr).is_empty());

    let dir = tempfile::tempdir().expect("tempdir");
    let bad = dir.path().join("bad.yaml");
    std::fs::write(&bad, "pipeline: p\nsource:\n  file:\n    streams: []\n").expect("write");
    let out = rdlt().arg("validate").arg(&bad).output().expect("spawn");
    assert_eq!(out.status.code(), Some(2));
}

/// `schema` prints exactly one JSON document to stdout, for every
/// spelling the flag accepts.
#[test]
fn schema_prints_json_for_every_connector() {
    for name in [
        "rest-source",
        "postgres-source",
        "oracle-source",
        "file-source",
        "file-dest",
        "postgres-dest",
        "duckdb-dest",
        "snowflake-dest",
        "iceberg-dest",
    ] {
        let out = rdlt().args(["schema", name]).output().expect("spawn");
        assert_eq!(out.status.code(), Some(0), "{name}");
        let schema: serde_json::Value = serde_json::from_slice(&out.stdout)
            .unwrap_or_else(|e| panic!("{name}: stdout is one JSON document: {e}"));
        assert!(
            schema.get("properties").is_some() || schema.get("$defs").is_some(),
            "{name}: looks like a JSON Schema: {schema}"
        );
    }
}

/// The compiled tier of `schema` is BYTE-IDENTICAL to the ValueEnum
/// era: exactly the library's own schema export, pretty-printed, one
/// trailing newline — verified against a pre-change golden capture in
/// the 039 T7 record. The arg is a free string now (unknown spellings
/// spawn a connector binary), and this pin is what holds the nine
/// compiled spellings still.
#[test]
fn schema_file_source_stays_byte_identical() {
    let out = rdlt()
        .args(["schema", "file-source"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let expected = serde_json::to_string_pretty(&rdlt::connector::file::source::config_schema())
        .expect("the library schema encodes")
        + "\n";
    assert_eq!(
        String::from_utf8(out.stdout).expect("utf8"),
        expected,
        "the compiled spelling's bytes are the library schema's, unchanged"
    );
}

/// An unrecognized `schema` value that names no existing file is
/// treated as a connector id; with no such binary on PATH it exits 2
/// carrying the provider's frozen NotFound spelling, stdout clean.
#[test]
fn schema_for_an_absent_connector_exits_2_with_the_notfound_spelling() {
    let out = rdlt()
        .args(["schema", "./nonexistent-connector-binary"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(out.stdout.is_empty(), "no machine output on refusal");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("connector `./nonexistent-connector-binary`")
            && stderr.contains("no binary")
            && stderr.contains("on PATH and no explicit path was given"),
        "the frozen NotFound spelling surfaces verbatim: {stderr}"
    );
}

/// `--role` selects which half of an OUT-OF-PROCESS connector to ask;
/// the nine compiled spellings each already name their role, so the
/// flag on one is a contradiction — refused up front (exit 2, the
/// config code), never accepted-and-ignored.
#[test]
fn schema_role_on_a_compiled_spelling_is_refused() {
    let out = rdlt()
        .args(["schema", "file-source", "--role", "destination"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(out.stdout.is_empty(), "no machine output on refusal");
    assert_eq!(
        String::from_utf8_lossy(&out.stderr),
        "error: `file-source` is a compiled-in spelling that already names its \
         role; `--role` selects which half of an out-of-process connector (an id \
         or binary path) to ask — drop the flag\n",
        "the refusal explains itself in full"
    );
}

/// An unknown `--role` value is a malformed invocation: clap's
/// value_enum refuses it at the argument gate with the CLI's
/// historical usage code (64, like every other bad invocation),
/// stdout clean.
#[test]
fn schema_role_rejects_an_unknown_value_as_usage() {
    let out = rdlt()
        .args(["schema", "file-source", "--role", "bogus"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(64), "{out:?}");
    assert!(out.stdout.is_empty(), "usage text never lands on stdout");
}

/// `--role` rides the existing spawn tier: an absent connector with
/// the flag refuses at the same resolve gate as without it — exit 2,
/// the provider's frozen NotFound spelling verbatim — proving the
/// flag routes into the out-of-process path rather than growing one
/// of its own.
#[test]
fn schema_role_for_an_absent_connector_exits_2_with_the_notfound_spelling() {
    let out = rdlt()
        .args([
            "schema",
            "./nonexistent-connector-binary",
            "--role",
            "destination",
        ])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(out.stdout.is_empty(), "no machine output on refusal");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("connector `./nonexistent-connector-binary`")
            && stderr.contains("no binary")
            && stderr.contains("on PATH and no explicit path was given"),
        "the frozen NotFound spelling surfaces verbatim: {stderr}"
    );
}

/// A `connector:` pipeline whose binary exists nowhere refuses at the
/// same gate from BOTH `run` and `validate`: exit 2, the frozen
/// NotFound spelling on stderr, nothing on stdout — the resolve-class
/// contract extended to spawned connectors.
#[test]
fn a_connector_spec_with_a_missing_binary_exits_2_on_run_and_validate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let spec = dir.path().join("pipeline.yaml");
    std::fs::write(
        &spec,
        "pipeline: p\nsource:\n  connector:\n    id: io.rdlt.test.absent\n    config: {}\n\
         destination:\n  connector:\n    id: io.rdlt.test.absent\n    config: {}\n",
    )
    .expect("write");
    for command in ["run", "validate"] {
        let out = rdlt().arg(command).arg(&spec).output().expect("spawn");
        assert_eq!(out.status.code(), Some(2), "{command}: {out:?}");
        assert!(out.stdout.is_empty(), "{command}: stdout stays clean");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("connector `io.rdlt.test.absent`")
                && stderr.contains("no binary `rdlt-connector-absent`"),
            "{command}: the frozen NotFound spelling surfaces: {stderr}"
        );
    }
}

/// `--events` writes the feed as NDJSON: one JSON object per line,
/// run_started first, committed present. `--events -` without
/// `--report` is refused before anything runs — stdout belongs to one
/// machine output at a time.
#[test]
fn events_ndjson_sink_holds_its_contract() {
    let (dir, spec) = fresh_pipeline();
    let events_path = dir.path().join("events.ndjson");
    let out = rdlt()
        .arg("run")
        .arg(&spec)
        .arg("--events")
        .arg(&events_path)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let ndjson = std::fs::read_to_string(&events_path).expect("events file");
    let events: Vec<serde_json::Value> = ndjson
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("each line is JSON: {e}: {l}")))
        .collect();
    assert_eq!(
        events.first().and_then(|e| e["event"].as_str()),
        Some("run_started"),
        "the feed identifies the run first"
    );
    assert!(
        events.iter().any(|e| e["event"] == "committed"),
        "the commit is in the feed"
    );

    let (_dir, spec) = fresh_pipeline();
    let out = rdlt()
        .args(["run"])
        .arg(&spec)
        .args(["--events", "-"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2), "stdout collision refused");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--report"),
        "the refusal names the fix"
    );
}

/// 037 final-review wave, item 5: a run that fails MID-RUN at the
/// destination — after `run_started` has already gone out over the
/// event feed, not at up-front validation — must still leave the
/// `--events` NDJSON sink holding everything written so far, flushed
/// and parseable line-by-line. This pins the half of 22b's flush-on-
/// error guarantee that was previously inspection-only.
///
/// The failure is forced with the file destination's own layout-v2
/// version gate (`destination/layout.rs`'s `version_gate`): a first
/// clean run writes a real v2 `_rdlt_commits.{scope}.json`; planting a
/// v1-format commit log over it makes the SECOND run's first commit
/// refuse with a "predates" error — a genuine destination-side failure
/// reached only after streaming has begun, not a config-time refusal.
#[test]
fn a_mid_run_destination_failure_flushes_the_events_sink_before_exiting() {
    let (dir, spec) = fresh_pipeline();

    // First run: clean, writes a real v2 commit log under `out/`.
    let out = rdlt().arg("run").arg(&spec).output().expect("spawn");
    assert_eq!(
        out.status.code(),
        Some(0),
        "first run must succeed: {out:?}"
    );

    // Plant a v1-format commit log over the real one — captured from the
    // first run's own output listing rather than re-deriving the scope
    // hash, so this test does not need to depend on the naming crate.
    let out_dir = dir.path().join("out");
    let commits_file = std::fs::read_dir(&out_dir)
        .expect("read output dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("_rdlt_commits.") && n.ends_with(".json"))
        })
        .expect("the first run wrote a commit log");
    std::fs::write(&commits_file, br#"{"format_version":1,"receipts":[]}"#)
        .expect("plant a v1 commit log");

    // The source cursor already consumed `rows.jsonl` to EOF, so the
    // second run needs fresh rows to extract anything at all — appended,
    // never rewritten, so the tail-hash resume check still recognizes it
    // as the same file continuing rather than refusing it as rewritten.
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("rows.jsonl"))
            .expect("open rows.jsonl");
        writeln!(f, "{{\"id\": 4}}").expect("append");
        writeln!(f, "{{\"id\": 5}}").expect("append");
    }

    let events_path = dir.path().join("events.ndjson");
    let out = rdlt()
        .arg("run")
        .arg(&spec)
        .arg("--events")
        .arg(&events_path)
        .output()
        .expect("spawn");
    assert_ne!(
        out.status.code(),
        Some(0),
        "the stale commit log must fail the second run: {out:?}"
    );

    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        stderr.contains("predates"),
        "stderr carries the version-gate refusal: {stderr}"
    );

    let ndjson = std::fs::read_to_string(&events_path)
        .expect("the events file exists even though the run failed");
    assert!(
        !ndjson.is_empty(),
        "the sink flushed what it had before exiting, not left empty"
    );
    let events: Vec<serde_json::Value> = ndjson
        .lines()
        .map(|l| {
            serde_json::from_str(l)
                .unwrap_or_else(|e| panic!("every line parses, none left truncated: {e}: {l}"))
        })
        .collect();
    assert_eq!(
        events.first().and_then(|e| e["event"].as_str()),
        Some("run_started"),
        "the feed identifies the run first, even on a run that goes on to fail"
    );
}

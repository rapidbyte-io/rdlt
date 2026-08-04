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

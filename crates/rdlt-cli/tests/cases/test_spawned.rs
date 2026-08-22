//! The live half of the contract: real runs over the spawned reference
//! connector — `run`'s stdout/stderr/report split, the verbosity gates,
//! `check`, the `--events` sink, and `--output`. Everything here
//! needs the built bin, so the whole module rides `spawn-bins`.

use std::path::PathBuf;
use std::process::Command;

use super::support::spec_file;

/// The CLI, with the built reference bin's directory prepended to PATH
/// so the provider's discovery finds `rdlt-connector-reference`. The
/// bin comes through the testkit's ONE spawn scaffold — building under
/// `RDLT_BUILD_CONNECTOR_BINS`, refusing a relative `CARGO_TARGET_DIR`,
/// failing loudly on a missing bin — rather than a local copy of those
/// mechanics: copies diverge, and a diverged copy certifies a stale
/// binary.
fn rdlt() -> Command {
    let bin = rdlt_testkit::spawn::built_connector_bin(
        env!("CARGO_MANIFEST_DIR"),
        "rdlt-connector-reference",
    );
    let bins = bin
        .parent()
        .expect("a built bin has a parent directory")
        .to_path_buf();
    let mut paths = vec![bins];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let mut command = super::support::rdlt();
    command.env(
        "PATH",
        std::env::join_paths(paths).expect("PATH entries join"),
    );
    command
}

/// A three-row jsonl fixture and a reference -> reference pipeline over
/// it. Fresh per phase: the reference source's byte cursor knows a
/// fully-read file, so re-running against one workdir reads zero rows —
/// correct engine behaviour that would vacuously pass the quiet/verbose
/// assertions. The fixture's stem names the stream: `events`.
fn fresh_pipeline() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("events.jsonl");
    std::fs::write(&data, "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n").expect("write");
    let spec = spec_file(
        dir.path(),
        "pipeline.yaml",
        &format!(
            "pipeline: contract\nworkdir: {}\nsource:\n  connector:\n    id: io.rapidbyte.reference\n    config:\n      path: {}\ndestination:\n  connector:\n    id: io.rapidbyte.reference\n    config:\n      path: {}\n",
            dir.path().join(".rdlt").display(),
            data.display(),
            dir.path().join("out").display()
        ),
    );
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
    // The reference source pushes batches and checkpoints at batch
    // boundaries, so the three-row fixture is one frame, one commit,
    // one feed line counting all three rows.
    assert!(stderr.contains("events: +3 rows"), "{stderr}");
    assert!(stderr.contains("commit 1 ok"), "{stderr}");
    // Heartbeats and the verbose detail stay OUT of the default feed.
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

/// The report and the event log carry source-defined cursors: both are
/// born owner-only whatever the umask, and a symlink planted at either
/// name is refused (exit 74) with its target untouched.
#[test]
fn report_and_event_outputs_are_private_and_follow_no_link() {
    use std::os::unix::fs::PermissionsExt as _;
    let (dir, spec) = fresh_pipeline();
    let report_path = dir.path().join("report.json");
    let events_path = dir.path().join("events.ndjson");
    let out = rdlt()
        .env("UMASK", "022")
        .arg("run")
        .arg(&spec)
        .arg("--report")
        .arg(&report_path)
        .arg("--events")
        .arg(&events_path)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    for path in [&report_path, &events_path] {
        let mode = std::fs::metadata(path).expect("meta").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "{}: {mode:o}", path.display());
    }

    let (dir, spec) = fresh_pipeline();
    let victim = dir.path().join("victim");
    std::fs::write(&victim, b"precious").expect("victim");
    let link = dir.path().join("report.json");
    std::os::unix::fs::symlink(&victim, &link).expect("plant");
    let out = rdlt()
        .arg("run")
        .arg(&spec)
        .arg("--report")
        .arg(&link)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(74), "{out:?}");
    assert_eq!(std::fs::read(&victim).expect("victim"), b"precious");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("a symlink — refusing to follow it"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `--output json` is machine mode: stdout is exactly the report JSON
/// and stderr is silent — the redirected-stdout contract, spelled as a
/// flag so a terminal gets it too. `--output plain` keeps the line-per-
/// event feed with the report still on stdout; on a pipe that is what
/// `auto` selects anyway, so this half only proves the flag rides the
/// real run end to end — the mode ladder's unit pin is the proof that
/// `plain` overrides a terminal.
#[test]
fn output_json_prints_the_report_to_stdout() {
    let (_dir, spec) = fresh_pipeline();
    let out = rdlt()
        .args(["--output", "json", "run"])
        .arg(&spec)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let report: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout is exactly the report JSON: {e}\n{stdout}"));
    assert_eq!(report["tables"]["events"]["rows"], 3);
    assert!(
        out.stderr.is_empty(),
        "json silences the feed and the summary: {:?}",
        String::from_utf8_lossy(&out.stderr)
    );

    let (_dir, spec) = fresh_pipeline();
    let out = rdlt()
        .args(["--output", "plain", "run"])
        .arg(&spec)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("commit 1 ok"), "{stderr}");
    let report: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("plain keeps the report on stdout");
    assert_eq!(report["pipeline"], "contract");
}

/// `check` runs the build gates for real — for a spawned connector
/// that is the real spawn and handshake — then the connectors'
/// reachability probes, discovery and the run's plan validation, and
/// nothing after them: the pipeline is discarded, killing the spawns
/// with it. The ok line reports what the check found; a check writes
/// NOTHING — no workdir, and no destination-side artifacts (the
/// reference destination creates its out directory at connect, which a
/// check never reaches).
#[test]
fn check_gates_without_running() {
    let (dir, spec) = fresh_pipeline();
    let out = rdlt().arg("check").arg(&spec).output().expect("spawn");
    assert_eq!(out.status.code(), Some(0), "{out:?}");
    assert!(out.stdout.is_empty(), "check writes no machine output");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains(
            "ok: pipeline contract — connectors reachable, 1 stream discovered, plan valid"
        ),
        "{:?}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The no-write pin: the document's workdir was never created (no
    // lock, no WAL), and neither was the destination's out directory —
    // Destination::check is a pre-connect probe on the connector.
    assert!(
        !dir.path().join(".rdlt").exists(),
        "a check must not create the workdir"
    );
    assert!(
        !dir.path().join("out").exists(),
        "a check must not create destination artifacts"
    );

    // -q silences the ok line; the exit code still answers.
    let (_dir, spec) = fresh_pipeline();
    let out = rdlt()
        .args(["-q", "check"])
        .arg(&spec)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stderr).is_empty());

    // A config the CONNECTOR's own gate refuses exits 2 — the same
    // resolve class as a missing binary, but this one crossed the wire
    // and came back in the connector's wording (the reference source
    // refuses an empty `path`).
    let dir = tempfile::tempdir().expect("tempdir");
    let bad = spec_file(
        dir.path(),
        "bad.yaml",
        "pipeline: p\nsource:\n  connector:\n    id: io.rapidbyte.reference\n    config:\n      path: \"\"\n",
    );
    let out = rdlt().arg("check").arg(&bad).output().expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
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

/// A run that fails MID-RUN at the destination — after `run_started`
/// has already gone out over the event feed, not at up-front validation
/// — must still leave the `--events` NDJSON sink holding everything
/// written so far, flushed and parseable line-by-line.
///
/// The failure is forced with the reference destination's own
/// receipt-log integrity gate: a first clean run writes a real
/// `_reference_receipts.json`; planting a newline-terminated garbage
/// line into its interior makes the SECOND run's first commit refuse
/// with the "corrupt receipt line" error — a genuine destination-side
/// failure reached only after streaming has begun, not a config-time
/// refusal.
#[test]
fn a_mid_run_destination_failure_flushes_the_events_sink_before_exiting() {
    let (dir, spec) = fresh_pipeline();

    // First run: clean, writes a real receipt log under `out/`.
    let out = rdlt().arg("run").arg(&spec).output().expect("spawn");
    assert_eq!(
        out.status.code(),
        Some(0),
        "first run must succeed: {out:?}"
    );

    // Corrupt the receipt log's INTERIOR: a newline-terminated line
    // that is not a receipt is corruption (a newline-less tail would
    // instead read as a torn append and heal), so the next commit's
    // replay check refuses typed.
    let receipts = dir.path().join("out").join("_reference_receipts.json");
    {
        use std::io::Write as _;
        let mut log = std::fs::OpenOptions::new()
            .append(true)
            .open(&receipts)
            .expect("the first run wrote a receipt log");
        writeln!(log, "not a receipt").expect("append the corrupt line");
    }

    // The source's byte cursor already consumed `events.jsonl` to EOF,
    // so the second run needs fresh rows to extract anything at all —
    // appended, never rewritten, so the resume offset still lands on a
    // line boundary of the same file continuing.
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("events.jsonl"))
            .expect("open events.jsonl");
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
        "the corrupt receipt log must fail the second run: {out:?}"
    );

    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        stderr.contains("corrupt receipt line"),
        "stderr carries the receipt-integrity refusal: {stderr}"
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

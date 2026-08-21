//! The compatibility contract, pinned against the BINARY: argument
//! spellings, the stdout/stderr split, and exit codes. These are what
//! scripts depend on; every renderer change answers to this file.
//!
//! Two tiers. The ungated tests need NO connector binaries — they pin
//! argument handling and the refusal paths (a missing binary IS a
//! refusal, and its frozen spelling is the pin). The `spawned_runs`
//! module runs REAL pipelines, which — since the 043 D1 swap — spawn
//! connector binaries; it rides the `spawn-bins` feature and the
//! Makefile's RDLT_BUILD_CONNECTOR_BINS discipline, exactly like
//! rdlt-runtime's spawn suites.

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

/// `--help` and `--version` exit 0 — clap conventions.
#[test]
fn help_and_version_exit_zero() {
    let out = rdlt().arg("--help").output().expect("spawn");
    assert_eq!(out.status.code(), Some(0));
    let out = rdlt().arg("--version").output().expect("spawn");
    assert_eq!(out.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&out.stdout).contains(env!("CARGO_PKG_VERSION")));
}

/// A spec file the filesystem refuses exits 74; a spec that parses but
/// cannot resolve its connectors exits 2. Both report on stderr only.
#[test]
fn io_and_config_failures_keep_their_codes() {
    let out = rdlt()
        .args(["run", "definitely-missing.yaml"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(74));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("error: reading"));

    // The document parses; resolution then refuses — here because no
    // connector binary exists on the emptied PATH. Any resolve-class
    // failure (missing binary, a config the connector's gate refuses)
    // is the same exit-2 contract.
    let dir = tempfile::tempdir().expect("tempdir");
    let bad = dir.path().join("bad.yaml");
    std::fs::write(&bad, "pipeline: p\nsource:\n  file:\n    streams: []\n").expect("write");
    let out = rdlt()
        .env("PATH", dir.path())
        .arg("run")
        .arg(&bad)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{:?}", out);
    assert!(out.stdout.is_empty());
}

/// A short name maps through the desugar table BEFORE discovery: with
/// no binaries reachable, the refusal itself is the proof — the
/// spelling arrived at discovery as its reverse-DNS id, not as a
/// literal binary name. (PATH is emptied so a developer's installed
/// connectors cannot turn this into a live probe; the probed rows are
/// `oracle` and `rest` so the pin needs no binary that this repo
/// builds — the full spelling table is desugar.rs's.)
#[test]
fn schema_maps_a_short_name_through_the_desugar_table() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = rdlt()
        .env("PATH", dir.path())
        .args(["schema", "oracle"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(out.stdout.is_empty(), "no machine output on refusal");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("connector `io.rapidbyte.oracle`")
            && stderr.contains("no binary `rdlt-connector-oracle`"),
        "the short name resolved to its table id before discovery: {stderr}"
    );

    std::fs::write(dir.path().join("rest"), "not a connector binary")
        .expect("the shadowing file writes");
    let out = rdlt()
        .current_dir(dir.path())
        .env("PATH", dir.path())
        .args(["schema", "rest"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(out.stdout.is_empty(), "no machine output on refusal");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("connector `io.rapidbyte.rest`")
            && stderr.contains("no binary `rdlt-connector-rest`"),
        "the short name wins over a same-named working-directory file: {stderr}"
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

/// An unknown `--role` value is a malformed invocation: clap's
/// value_enum refuses it at the argument gate with the CLI's
/// historical usage code (64, like every other bad invocation),
/// stdout clean.
#[test]
fn schema_role_rejects_an_unknown_value_as_usage() {
    let out = rdlt()
        .args(["schema", "file", "--role", "bogus"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(64), "{out:?}");
    assert!(out.stdout.is_empty(), "usage text never lands on stdout");
}

/// `--role` rides the one spawn tier: an absent connector with the
/// flag refuses at the same resolve gate as without it — exit 2, the
/// provider's frozen NotFound spelling verbatim — proving the flag
/// routes into the out-of-process path rather than growing one of its
/// own.
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
/// contract for spawned connectors.
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

/// The live half of the contract: real runs over the spawned reference
/// connector. Everything here needs the built bin, so the whole
/// module rides `spawn-bins` — the Makefile's line builds and runs it.
#[cfg(feature = "spawn-bins")]
mod spawned_runs {
    use std::process::Command;

    /// The CLI, with the built reference bin's directory prepended to
    /// PATH so the provider's discovery finds
    /// `rdlt-connector-reference`. The bin comes through the testkit's
    /// ONE spawn scaffold — building under `RDLT_BUILD_CONNECTOR_BINS`,
    /// refusing a relative `CARGO_TARGET_DIR`, failing loudly on a
    /// missing bin — rather than a local copy of those mechanics (the
    /// 042 lesson: copies diverge, and a diverged copy certifies a
    /// stale binary).
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
        let mut command = super::rdlt();
        command.env(
            "PATH",
            std::env::join_paths(paths).expect("PATH entries join"),
        );
        command
    }

    fn fresh_pipeline() -> (tempfile::TempDir, std::path::PathBuf) {
        // Fresh per phase: the reference source's byte cursor knows a
        // fully-read file, so re-running against one workdir reads zero
        // rows — which is correct engine behaviour and would vacuously
        // pass the quiet/verbose assertions below. The fixture's stem
        // names the stream: `events`.
        let dir = tempfile::tempdir().expect("tempdir");
        let data = dir.path().join("events.jsonl");
        std::fs::write(&data, "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n").expect("write");
        let spec = dir.path().join("pipeline.yaml");
        std::fs::write(
            &spec,
            format!(
                "pipeline: contract\nworkdir: {}\nsource:\n  connector:\n    id: io.rapidbyte.reference\n    config:\n      path: {}\ndestination:\n  connector:\n    id: io.rapidbyte.reference\n    config:\n      path: {}\n",
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
        // The reference source pushes batches and checkpoints at batch
        // boundaries, so the three-row fixture is one frame, one
        // commit, one feed line counting all three rows.
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

    /// `validate` runs the real gates and nothing after them — for a
    /// spawned connector those gates are the real spawn and handshake
    /// — then discards the pipeline, killing the spawns with it.
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

        // A config the CONNECTOR's own gate refuses exits 2 — the same
        // resolve class as a missing binary, but this one crossed the
        // wire and came back in the connector's wording (the reference
        // source refuses an empty `path`).
        let dir = tempfile::tempdir().expect("tempdir");
        let bad = dir.path().join("bad.yaml");
        std::fs::write(
            &bad,
            "pipeline: p\nsource:\n  connector:\n    id: io.rapidbyte.reference\n    config:\n      path: \"\"\n",
        )
        .expect("write");
        let out = rdlt().arg("validate").arg(&bad).output().expect("spawn");
        assert_eq!(out.status.code(), Some(2), "{out:?}");
    }

    /// `--events` writes the feed as NDJSON: one JSON object per line,
    /// run_started first, committed present. `--events -` without
    /// `--report` is refused before anything runs — stdout belongs to
    /// one machine output at a time.
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
            .map(|l| {
                serde_json::from_str(l).unwrap_or_else(|e| panic!("each line is JSON: {e}: {l}"))
            })
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
    /// The failure is forced with the reference destination's own
    /// receipt-log integrity gate: a first clean run writes a real
    /// `_reference_receipts.json`; planting a newline-terminated garbage
    /// line into its interior makes the SECOND run's first commit refuse
    /// with the "corrupt receipt line" error — a genuine
    /// destination-side failure reached only after streaming has begun,
    /// not a config-time refusal.
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
        // that is not a receipt is corruption (a newline-less tail
        // would instead read as a torn append and heal), so the next
        // commit's replay check refuses typed.
        let receipts = dir.path().join("out").join("_reference_receipts.json");
        {
            use std::io::Write as _;
            let mut log = std::fs::OpenOptions::new()
                .append(true)
                .open(&receipts)
                .expect("the first run wrote a receipt log");
            writeln!(log, "not a receipt").expect("append the corrupt line");
        }

        // The source's byte cursor already consumed `events.jsonl` to
        // EOF, so the second run needs fresh rows to extract anything at
        // all — appended, never rewritten, so the resume offset still
        // lands on a line boundary of the same file continuing.
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
}

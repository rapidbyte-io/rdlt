//! The certifier bin's contract, pinned by spawning the BUILT bin
//! (`CARGO_BIN_EXE_rdlt-certify` — cargo builds it for this suite
//! because the gate line enables the `bin` feature): report lines on
//! stdout, diagnostics on stderr, and the exit-code vocabulary — 0
//! all-pass, 1 clause failures, 2 for a resolution/spawn refusal (the
//! runtime's frozen spelling verbatim on stderr) and for bad arguments
//! (clap's default). `--explain` is pinned against the crate's own
//! [`CLAUSES`] table, so the bin cannot ship a vocabulary the library
//! does not speak.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

use rdlt_certify::CLAUSES;

use super::support::bins::built_bin;

/// The built certifier bin this suite spawns.
const CERTIFY_BIN: &str = env!("CARGO_BIN_EXE_rdlt-certify");

/// Run the certifier with `args`, capturing everything.
fn certify(args: &[&str]) -> Output {
    Command::new(CERTIFY_BIN)
        .args(args)
        .output()
        .expect("the certifier bin spawns")
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Write a jsonl source fixture and its config document; hand back the
/// config file's path (the fixture directory rides along).
fn source_config(dir: &Path) -> std::path::PathBuf {
    std::fs::write(
        dir.join("rows.jsonl"),
        "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n",
    )
    .expect("the fixture file writes");
    let config = serde_json::json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": format!("{}/*.jsonl", dir.display()),
        }]
    });
    let path = dir.join("config.json");
    std::fs::write(&path, config.to_string()).expect("the config file writes");
    path
}

/// The happy path: the real file connector bin certifies as a source,
/// exit 0, the clause lines on stdout.
#[test]
fn the_file_source_certifies_all_pass_with_exit_0() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = source_config(dir.path());
    let bin = built_bin("rdlt-connector-file");

    let output = certify(&[
        "--role",
        "source",
        "--config",
        config.to_str().expect("utf-8 path"),
        bin.to_str().expect("utf-8 path"),
    ]);

    let stdout = stdout_of(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{stdout}\nstderr:\n{}",
        stderr_of(&output)
    );
    assert!(stdout.contains("PASS S1"), "stdout:\n{stdout}");
    assert!(stdout.contains("PASS P1"), "stdout:\n{stdout}");
}

/// `--report json`: one JSON document on stdout, entries non-empty.
#[test]
fn the_json_report_parses_with_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = source_config(dir.path());
    let bin = built_bin("rdlt-connector-file");

    let output = certify(&[
        "--role",
        "source",
        "--config",
        config.to_str().expect("utf-8 path"),
        "--report",
        "json",
        bin.to_str().expect("utf-8 path"),
    ]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr:\n{}",
        stderr_of(&output)
    );
    let report: serde_json::Value =
        serde_json::from_str(&stdout_of(&output)).expect("stdout is one JSON document");
    let entries = report["entries"]
        .as_array()
        .expect("the report carries an entries array");
    assert!(!entries.is_empty(), "entries must be non-empty");
}

/// The P1 second-line rogue: a script that follows its handshake line
/// with stdout chatter fails P1 (and, serving nothing, everything
/// downstream) — clause failures are exit 1, listed on stdout.
#[test]
fn a_second_stdout_line_rogue_fails_p1_with_exit_1() {
    let dir = tempfile::tempdir().expect("tempdir");
    let script = dir.path().join("rogue-connector");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho 'rdlt-connector|1|0|0|{}/rogue.sock'\n\
             echo 'chatter after the handshake line'\nexec sleep 30\n",
            dir.path().display()
        ),
    )
    .expect("the rogue script writes");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
        .expect("the rogue script becomes executable");

    let output = certify(&["--role", "source", script.to_str().expect("utf-8 path")]);

    let stdout = stdout_of(&output);
    assert_eq!(output.status.code(), Some(1), "stdout:\n{stdout}");
    assert!(stdout.contains("FAIL P1"), "stdout:\n{stdout}");
}

/// A connector id that resolves to nothing is a refusal, not a report:
/// exit 2 with the runtime's frozen NotFound spelling verbatim on
/// stderr — full-string, nothing else on the stream.
#[test]
fn a_missing_connector_id_refuses_with_exit_2() {
    let output = certify(&["--role", "source", "io.rapidbyte.certify-absent"]);

    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        stderr_of(&output),
        "connector `io.rapidbyte.certify-absent`: no binary \
         `rdlt-connector-certify-absent` on PATH and no explicit path was given — install \
         it (e.g. cargo install rdlt-connector-certify-absent) or set path: in the \
         connector requirement\n"
    );
    assert!(output.stdout.is_empty(), "a refusal writes no report");
}

/// Bad arguments are clap's default exit 2.
#[test]
fn a_bogus_role_exits_2() {
    let output = certify(&["--role", "bogus", "whatever"]);
    assert_eq!(output.status.code(), Some(2));
}

/// `--version` names the crate version.
#[test]
fn version_contains_the_crate_version() {
    let output = certify(&["--version"]);
    assert_eq!(output.status.code(), Some(0));
    assert!(
        stdout_of(&output).contains(env!("CARGO_PKG_VERSION")),
        "--version must carry the crate version"
    );
}

/// `--explain` needs no target, exits 0, and speaks the WHOLE clause
/// vocabulary — every id, its title, and its definition, straight from
/// the library's own table (which is itself pinned against the
/// emittable id set, so this transitively covers every clause the bin
/// can print).
#[test]
fn explain_covers_the_whole_vocabulary_with_exit_0() {
    let output = certify(&["--explain"]);

    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr:\n{}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    for clause in CLAUSES {
        assert!(
            stdout.contains(&format!("{} ({})", clause.id, clause.title)),
            "--explain must head {} with its title",
            clause.id
        );
        assert!(
            stdout.contains(clause.definition),
            "--explain must carry {}'s definition",
            clause.id
        );
    }
}

/// Write a file-destination config document rooted at `out_root`; hand
/// back the config file's path.
fn dest_config(dir: &Path, out_root: &Path) -> std::path::PathBuf {
    let config = serde_json::json!({
        "path": out_root.display().to_string(),
        "format": "jsonl",
    });
    let path = dir.join("config.json");
    std::fs::write(&path, config.to_string()).expect("the config file writes");
    path
}

/// `--kill-matrix` appends the K-clauses to the report. Without
/// `--probe-cmd` the destination K-arms are honest Skips (read-back
/// convergence needs a probe) while the probe-independent session
/// clauses still certify — skips do not refuse, exit 0.
#[test]
fn the_kill_matrix_flag_appends_the_k_clauses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out_root = dir.path().join("out");
    std::fs::create_dir(&out_root).expect("the output root creates");
    let config_path = dest_config(dir.path(), &out_root);
    let bin = built_bin("rdlt-connector-file");

    let output = certify(&[
        "--role",
        "destination",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--kill-matrix",
        bin.to_str().expect("utf-8 path"),
    ]);

    let stdout = stdout_of(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{stdout}\nstderr:\n{}",
        stderr_of(&output)
    );
    assert!(stdout.contains("PASS P10"), "stdout:\n{stdout}");
    for clause in ["K-D1", "K-D2", "K-D3", "K-D4", "K-D5", "K-D6"] {
        assert!(
            stdout.contains(&format!("SKIP {clause}")),
            "without a probe {clause} must Skip:\n{stdout}"
        );
    }
}

/// `--probe-cmd` wires a shell read-back into the D-clauses: the
/// command is CALLED with `{{table}}` substituted — proven by the
/// marker file the command itself appends, never by parsing the probe's
/// echo out of certifier output — and the probed count drives the
/// judgments (a constant 3 contradicts D1's staged-invisibility zero
/// and D4's one visible row, so the run FAILS rather than skipping).
/// No stream may echo the command line: it can carry credentials.
#[test]
fn probe_cmd_drives_the_read_back_clauses_and_is_never_echoed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out_root = dir.path().join("out");
    std::fs::create_dir(&out_root).expect("the output root creates");
    let config_path = dest_config(dir.path(), &out_root);
    let marker = dir.path().join("probe-marker");
    let probe_line = format!("echo {{{{table}}}} >> {}; echo 3", marker.display());
    let bin = built_bin("rdlt-connector-file");

    let output = certify(&[
        "--role",
        "destination",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--probe-cmd",
        &probe_line,
        bin.to_str().expect("utf-8 path"),
    ]);

    let stdout = stdout_of(&output);
    let stderr = stderr_of(&output);
    let marker_text =
        std::fs::read_to_string(&marker).expect("the probe command ran and wrote its marker");
    assert!(
        marker_text.contains("rdlt_conf_t"),
        "{{{{table}}}} must be substituted into the probe line; marker held:\n{marker_text}"
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "a constant-3 probe contradicts the clauses:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("FAIL D1"), "stdout:\n{stdout}");
    assert!(
        !stdout.contains("SKIP D1"),
        "with a probe D1 must be judged, not skipped:\n{stdout}"
    );
    assert!(
        stdout.contains("found 3"),
        "the probed 3 must reach the clause evidence (D4 names the count it found):\n{stdout}"
    );
    for text in [&stdout, &stderr] {
        assert!(
            !text.contains(&probe_line),
            "no output may echo the probe command line:\n{text}"
        );
    }
}

/// A probe that cannot answer FAILS the read-back clause naming the
/// probe — exit 1 — and the failure names the exit status, never the
/// command line.
#[test]
fn a_failing_probe_cmd_fails_the_read_back_clause_naming_the_probe() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out_root = dir.path().join("out");
    std::fs::create_dir(&out_root).expect("the output root creates");
    let config_path = dest_config(dir.path(), &out_root);
    let bin = built_bin("rdlt-connector-file");

    let output = certify(&[
        "--role",
        "destination",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--probe-cmd",
        "exit 1",
        bin.to_str().expect("utf-8 path"),
    ]);

    let stdout = stdout_of(&output);
    let stderr = stderr_of(&output);
    assert_eq!(output.status.code(), Some(1), "stdout:\n{stdout}");
    assert!(
        stdout.contains(
            "FAIL D1 (staging invisibility): probe failed: the probe command \
                         failed: exit status: 1"
        ),
        "the read-back clause must fail naming the probe's exit:\n{stdout}"
    );
    for text in [&stdout, &stderr] {
        assert!(
            !text.contains("exit 1"),
            "no output may echo the probe command line:\n{text}"
        );
    }
}

/// Probe stdout that is not one u64 FAILS the clause naming the
/// unparseable output — the output, never the command that produced it.
#[test]
fn an_unparseable_probe_count_fails_naming_the_output() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out_root = dir.path().join("out");
    std::fs::create_dir(&out_root).expect("the output root creates");
    let config_path = dest_config(dir.path(), &out_root);
    let bin = built_bin("rdlt-connector-file");

    let output = certify(&[
        "--role",
        "destination",
        "--config",
        config_path.to_str().expect("utf-8 path"),
        "--probe-cmd",
        "echo notanumber",
        bin.to_str().expect("utf-8 path"),
    ]);

    let stdout = stdout_of(&output);
    let stderr = stderr_of(&output);
    assert_eq!(output.status.code(), Some(1), "stdout:\n{stdout}");
    assert!(
        stdout.contains("the probe command printed `notanumber`, not one u64 row count"),
        "the failure must name the unparseable output:\n{stdout}"
    );
    for text in [&stdout, &stderr] {
        assert!(
            !text.contains("echo notanumber"),
            "no output may echo the probe command line:\n{text}"
        );
    }
}

/// `--probe-cmd` is a destination read-back seam: beside `--role
/// source` it is a usage error — clap's exit 2, before any spawn — and
/// the refusal names the flag without echoing its value.
#[test]
fn probe_cmd_with_role_source_is_a_usage_error() {
    let output = certify(&["--role", "source", "--probe-cmd", "echo 3", "whatever"]);

    let stderr = stderr_of(&output);
    assert_eq!(output.status.code(), Some(2), "stderr:\n{stderr}");
    assert!(
        stderr.contains("--probe-cmd"),
        "the usage error must name the flag:\n{stderr}"
    );
    assert!(
        !stderr.contains("echo 3"),
        "the usage error must not echo the probe command line:\n{stderr}"
    );
    assert!(output.stdout.is_empty(), "a usage error writes no report");
}

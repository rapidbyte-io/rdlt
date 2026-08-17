//! The compatibility contract, pinned against the BINARY: argument
//! spellings, the stdout/stderr split, and exit codes. These are what
//! scripts depend on; every renderer change answers to this file.
//! Nothing here needs a connector binary — the cells pin argument
//! handling and the refusal paths (a missing binary IS a refusal, and
//! its frozen spelling is the pin); the real runs live in
//! `test_spawned`.

use super::support::{rdlt, spec_file};

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
    let bad = spec_file(
        dir.path(),
        "bad.yaml",
        "pipeline: p\nsource:\n  file:\n    streams: []\n",
    );
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
    let spec = spec_file(
        dir.path(),
        "pipeline.yaml",
        "pipeline: p\nsource:\n  connector:\n    id: io.rdlt.test.absent\n    config: {}\n\
         destination:\n  connector:\n    id: io.rdlt.test.absent\n    config: {}\n",
    );
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

/// `--output` is a global flag: an unknown value is a malformed
/// invocation (64, stdout clean), and an accepted value changes nothing
/// about the exit-code contract — the missing-file refusal is still 74.
#[test]
fn the_output_flag_is_global_and_refuses_unknown_values() {
    let out = rdlt()
        .args(["--output", "yaml", "run", "p.yaml"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(64), "{out:?}");
    assert!(out.stdout.is_empty(), "usage text never lands on stdout");

    let out = rdlt()
        .args(["run", "definitely-missing.yaml", "--output", "json"])
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(74), "{out:?}");
    assert!(out.stdout.is_empty(), "no report without a run");
}

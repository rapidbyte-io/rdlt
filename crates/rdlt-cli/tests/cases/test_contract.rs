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
        "pipeline: p\nsource:\n  connector: {id: io.example.src, config: {}}\n\
         destination:\n  connector: {id: io.example.dst, config: {}}\n",
    );
    let out = rdlt()
        .env("PATH", dir.path())
        .arg("run")
        .arg(&bad)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(2), "{:?}", out);
    assert!(out.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("no binary `rdlt-connector-src`"),
        "the refusal is the resolve step's, after a clean parse: {out:?}"
    );
}

/// `schema` takes the connector id AS WRITTEN — the CLI carries no
/// table of first-party names. `schema oracle` therefore looks for
/// `rdlt-connector-oracle` and, with no binaries reachable, refuses
/// naming `oracle`, never a table's expansion of it. (Were the binary
/// present, the shorthand would be refused next as an identity mismatch
/// against the full reverse-DNS id it reports — the same rule a
/// document's `id` follows; that arm needs a bin and lives in the gated
/// tier. PATH is emptied so a developer's installed connectors cannot
/// turn this into a live probe.)
#[test]
fn schema_takes_an_id_as_written() {
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
        stderr.contains("connector `oracle`")
            && stderr.contains("no binary `rdlt-connector-oracle`"),
        "the id is discovered by its last segment and named as given: {stderr}"
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
/// same gate from BOTH `run` and `check`: exit 2, the frozen
/// NotFound spelling on stderr, nothing on stdout — the resolve-class
/// contract for spawned connectors.
#[test]
fn a_connector_spec_with_a_missing_binary_exits_2_on_run_and_check() {
    let dir = tempfile::tempdir().expect("tempdir");
    let spec = spec_file(
        dir.path(),
        "pipeline.yaml",
        "pipeline: p\nsource:\n  connector:\n    id: io.rdlt.test.absent\n    config: {}\n\
         destination:\n  connector:\n    id: io.rdlt.test.absent\n    config: {}\n",
    );
    for command in ["run", "check"] {
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

/// `validate` is not a subcommand any more — `check` replaced it, with
/// no alias. The old spelling refuses like any other unknown
/// subcommand: exit 64, nothing on stdout.
#[test]
fn the_retired_validate_spelling_is_an_unknown_subcommand() {
    let out = rdlt().args(["validate", "p.yaml"]).output().expect("spawn");
    assert_eq!(out.status.code(), Some(64), "{out:?}");
    assert!(out.stdout.is_empty(), "usage text never lands on stdout");
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

/// `--events <file>` is opened only once the pipeline is built: a
/// document that refuses (here: an unreadable path, 74) leaves no
/// event log behind — and never truncates one from an earlier run.
#[test]
fn a_refused_document_leaves_the_events_file_untouched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let events = dir.path().join("events.ndjson");
    std::fs::write(&events, "earlier run\n").expect("write");
    let out = rdlt()
        .args(["run", "definitely-missing.yaml", "--events"])
        .arg(&events)
        .output()
        .expect("spawn");
    assert_eq!(out.status.code(), Some(74), "{out:?}");
    assert_eq!(
        std::fs::read_to_string(&events).expect("still there"),
        "earlier run\n"
    );
}

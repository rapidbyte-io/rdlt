//! The `rdlt-connector-duckdb` BIN, spawned for real (042): the
//! config-free `Spec` RPC answers with the reverse-DNS identity, plus
//! the bin's pinned arg behavior (exit codes and the version string;
//! clap's usage TEXT is deliberately unasserted — clap owns it).
//!
//! These are the pins for THE IDENTITY RULE (039 T6) reaching duckdb:
//! the connector's `NAME` const IS its connector id, spelled
//! reverse-DNS (`io.rapidbyte.duckdb`), so the strict-equality
//! handshake verification (D-039-2) and D-039-1's last-segment binary
//! discovery (`io.rapidbyte.duckdb` → binary `rdlt-connector-duckdb`
//! on PATH) both derive from one const. This crate is
//! DESTINATION-ONLY, so `--role=source` is an ARG error (clap's exit
//! 2), pinned beside the nonsense role.
//!
//! Plus the cross-process cell (D-042-2's operator story, measured
//! live on every run): a SECOND spawned connector pointed at a
//! database file a FIRST live connector holds read-write is refused at
//! its handshake, the refusal classified FATAL on the wire — an
//! embedder sees a typed terminal error carrying duckdb's own lock
//! diagnosis, never an infinite retry.

use std::process::Stdio;

use rdlt_runtime::{
    Classification, ClientError, ConnectorProvider, ConnectorRequirement,
    LocalBinaryConnectorProvider, ProviderError, Role,
};
use serde_json::json;

use super::support::spawn::built_bin;

/// The destination half answers the config-free `Spec` RPC through the
/// provider (spawn → handshake line → dial → Spec; the provider owns
/// the whole lifecycle including socket cleanup) and reports the
/// reverse-DNS id — the one `NAME` const, exact.
#[tokio::test]
async fn the_duckdb_bin_answers_the_spec_rpc() {
    let bin = built_bin();
    let provider = LocalBinaryConnectorProvider::new();
    let requirement = ConnectorRequirement::new("io.rapidbyte.duckdb").with_path(&bin);

    let spec = provider
        .spec_for_role(&requirement, Role::Destination)
        .await
        .unwrap_or_else(|error| panic!("the destination half answers the Spec RPC: {error}"));
    assert_eq!(spec.name, "io.rapidbyte.duckdb");
    // One workspace version everywhere, so this crate's own version
    // IS the connector's.
    assert_eq!(spec.version, env!("CARGO_PKG_VERSION"));
    assert!(
        spec.config_schema.is_some(),
        "the bin publishes a config schema"
    );
}

/// The pinned arg contract: no args → exit 2, an unrecognized role →
/// exit 2, and — this crate being destination-only — `--role=source`
/// is equally an unrecognized VALUE, refused by clap's arg gate before
/// any serve machinery.
#[test]
fn bad_args_exit_2() {
    let bin = built_bin();

    let no_args = std::process::Command::new(&bin)
        .stderr(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(no_args.status.code(), Some(2), "no args");

    let bad_role = std::process::Command::new(&bin)
        .arg("--role=nonsense")
        .stderr(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(bad_role.status.code(), Some(2), "--role=nonsense");

    let source_role = std::process::Command::new(&bin)
        .arg("--role=source")
        .stderr(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(
        source_role.status.code(),
        Some(2),
        "--role=source on a destination-only crate"
    );
}

/// `--version` succeeds and its output contains the crate version;
/// `--help` exits 0. The TEXT around the version is clap's, unasserted.
#[test]
fn version_and_help_behave() {
    let bin = built_bin();

    let version = std::process::Command::new(&bin)
        .arg("--version")
        .output()
        .expect("the bin runs");
    assert_eq!(version.status.code(), Some(0));
    let stdout = String::from_utf8(version.stdout).expect("version output is UTF-8");
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "`--version` output {stdout:?} must contain the crate version"
    );

    let help = std::process::Command::new(&bin)
        .arg("--help")
        .stdout(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(help.status.code(), Some(0), "--help");
}

/// THE CROSS-PROCESS CELL (D-042-2, live): connector 1 handshakes and
/// holds the database file read-write for its whole life; connector 2,
/// spawned against the SAME file, is refused at ITS handshake with a
/// FATAL classification on the wire and duckdb's own lock diagnosis in
/// the message. This is the live re-measurement of the `classify`
/// unit pin's spelling — the `Could not set lock on file` template
/// comes from the service on every run, never from a fixture — and the
/// proof the refusal is terminal: a fatal handshake refusal reaches an
/// embedder as a typed error, not a retry loop.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_spawned_connector_on_a_held_file_is_refused_fatal() {
    let bin = built_bin();
    let dir = tempfile::tempdir().expect("dir");
    let config = json!({ "path": dir.path().join("held.duckdb") });
    let provider = LocalBinaryConnectorProvider::new();
    let requirement = ConnectorRequirement::new("io.rapidbyte.duckdb").with_path(&bin);

    let first = provider
        .destination(&requirement, &config)
        .await
        .expect("the first connector opens the file and holds it");

    let error = provider
        .destination(&requirement, &config)
        .await
        .expect_err("the second connector must be refused, not admitted");
    match error {
        ProviderError::Client(ClientError::Handshake {
            classification,
            message,
            ..
        }) => {
            assert_eq!(
                classification,
                Classification::Fatal,
                "the refusal's classification travels the wire as FATAL"
            );
            assert!(
                message.contains("Could not set lock on file"),
                "the refusal carries duckdb's own lock diagnosis: {message}"
            );
        }
        other => panic!("expected a handshake refusal, got {other:?}"),
    }
    drop(first);
}

//! The `rdlt-connector-iceberg` BIN, spawned for real (042): the
//! config-free `Spec` RPC answers with the reverse-DNS identity, plus
//! the bin's pinned arg behavior (exit codes and the version string;
//! clap's usage TEXT is deliberately unasserted — clap owns it).
//!
//! These are the pins for THE IDENTITY RULE (039 T6) reaching iceberg:
//! the connector's `NAME` const IS its connector id, spelled
//! reverse-DNS (`io.rapidbyte.iceberg`), so the strict-equality
//! handshake verification (D-039-2) and D-039-1's last-segment binary
//! discovery (`io.rapidbyte.iceberg` → binary `rdlt-connector-iceberg`
//! on PATH) both derive from one const. This crate is
//! DESTINATION-ONLY, so `--role=source` is an ARG error (clap's exit
//! 2), pinned beside the nonsense role. All three cells are offline —
//! `Spec` answers before any catalog is dialed, so no fixture and no
//! skip surface.

use std::process::Stdio;

use rdlt_runtime::{ConnectorRequirement, LocalBinaryConnectorProvider, Role};

use super::support::spawn::built_bin;

/// The destination half answers the config-free `Spec` RPC through the
/// provider (spawn → handshake line → dial → Spec; the provider owns
/// the whole lifecycle including socket cleanup) and reports the
/// reverse-DNS id — the one `NAME` const, exact.
#[tokio::test]
async fn the_iceberg_bin_answers_the_spec_rpc() {
    let bin = built_bin();
    let provider = LocalBinaryConnectorProvider::new();
    let requirement = ConnectorRequirement::new("io.rapidbyte.iceberg").with_path(&bin);

    let spec = provider
        .spec_for_role(&requirement, Role::Destination)
        .await
        .unwrap_or_else(|error| panic!("the destination half answers the Spec RPC: {error}"));
    assert_eq!(spec.name, "io.rapidbyte.iceberg");
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

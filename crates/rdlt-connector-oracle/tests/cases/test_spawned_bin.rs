//! The `rdlt-connector-oracle` BIN, spawned for real (042): the
//! config-free `Spec` RPC answers for the SOURCE role with the
//! reverse-DNS identity, plus the bin's pinned arg behavior (exit
//! codes and the version string; clap's usage TEXT is deliberately
//! unasserted — clap owns it).
//!
//! These are the pins for THE IDENTITY RULE (039 T6) reaching oracle:
//! the connector's `NAME` const IS its connector id, spelled
//! reverse-DNS (`io.rapidbyte.oracle`), so the strict-equality
//! handshake verification (D-039-2) and D-039-1's last-segment binary
//! discovery (`io.rapidbyte.oracle` → binary `rdlt-connector-oracle`
//! on PATH) both derive from one const. SOURCE-ONLY: the crate has no
//! destination half, so `--role=destination` is an unrecognized VALUE
//! and exits 2 at clap's arg gate — pinned below beside the other arg
//! contracts.
//!
//! THE PRE-SPAWN CLIENT PROBE is this suite's oracle-specific pin,
//! asserted from BOTH sides: the driver dlopens an Oracle Client at
//! RUNTIME, and the bin probes for one between clap's arg gate and
//! the handshake line. A machine WITHOUT a client must see the typed
//! stderr refusal with stdout EMPTY and the serve-error exit code —
//! never an opaque death after a half-printed handshake — and a
//! machine WITH one must see the ordinary handshake. Each arm has a
//! subject on exactly one kind of machine, so its counterpart
//! announces the skip (024's skip-not-fail rule); the arg-contract
//! cells sit BEFORE the probe in the bin and run everywhere.

use std::process::Stdio;

use rdlt_connector_oracle::source::client_available;
use rdlt_runtime::{ConnectorRequirement, LocalBinaryConnectorProvider, Role};

use super::support::spawn::built_bin;

/// The refusal, byte-for-byte: the bin's one stderr line when no
/// client is loadable, naming the library it failed to dlopen and the
/// install hint. Frozen — the operator-facing spelling of the whole
/// probe.
const REFUSAL: &str = "rdlt-connector-oracle: no Oracle Client library is loadable — this \
     connector wraps ODPI-C, which dlopens libclntsh at RUNTIME (the build needed none). \
     Install Oracle Instant Client and put its directory on LD_LIBRARY_PATH.\n";

/// The source half answers the config-free `Spec` RPC through the
/// provider (spawn → handshake line → dial → Spec; the provider owns
/// the whole lifecycle including socket cleanup) and reports the
/// reverse-DNS id — the one `NAME` const, exact. Needs a loadable
/// client: the bin's probe sits before the handshake, so on a
/// clientless machine this arm's subject is the OTHER cell's.
#[tokio::test]
async fn with_a_client_the_bin_answers_the_spec_rpc_for_the_source_role() {
    if !client_available() {
        eprintln!(
            "SKIP: no Oracle Client library — the bin refuses before the handshake \
             (the refusal cell covers this machine); the Spec RPC arm not run"
        );
        return;
    }
    let bin = built_bin();
    let provider = LocalBinaryConnectorProvider::new();
    let requirement = ConnectorRequirement::new("io.rapidbyte.oracle").with_path(&bin);

    let spec = provider
        .spec_for_role(&requirement, Role::Source)
        .await
        .unwrap_or_else(|error| panic!("the source half answers the Spec RPC: {error}"));
    assert_eq!(spec.name, "io.rapidbyte.oracle");
    // One workspace version everywhere, so this crate's own version IS
    // the connector's.
    assert_eq!(spec.version, env!("CARGO_PKG_VERSION"));
    assert!(
        spec.config_schema.is_some(),
        "the source half publishes a config schema"
    );
}

/// THE REFUSAL ARM: on a machine without a client, `--role=source` is
/// one typed stderr line (frozen, full-string), an EMPTY stdout — no
/// handshake byte ever printed — and the serve-error exit code, 1.
/// The probe's whole point is that the provider reads this as a clean
/// pre-handshake refusal rather than an opaque spawn death; a machine
/// WITH a client has no subject for it and says so.
#[test]
fn without_a_client_the_bin_refuses_before_the_handshake() {
    if client_available() {
        eprintln!(
            "SKIP: an Oracle Client library is loadable — the refusal arm has no \
             subject on this machine (the Spec RPC cell covers it); not run"
        );
        return;
    }
    let output = std::process::Command::new(built_bin())
        .arg("--role=source")
        .output()
        .expect("the bin runs");

    assert_eq!(
        output.status.code(),
        Some(1),
        "a missing client is the serve-error exit code"
    );
    assert!(
        output.stdout.is_empty(),
        "the refusal precedes ANY stdout byte — a partial handshake would be an opaque \
         spawn death to the provider; got {:?}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8(output.stderr).expect("the refusal is UTF-8");
    assert_eq!(stderr, REFUSAL, "the refusal spelling is frozen");
}

/// The pinned arg contract: no args → exit 2, an unrecognized role →
/// exit 2, and — the source-only pin — `--role=destination` → exit 2,
/// because this crate serves no destination and the refusal happens at
/// clap's arg gate, before any serve machinery AND before the client
/// probe (which is why these cells run on every machine).
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

    let destination = std::process::Command::new(&bin)
        .arg("--role=destination")
        .stderr(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(
        destination.status.code(),
        Some(2),
        "--role=destination — this connector is source-only"
    );
}

/// `--version` succeeds and its output contains the crate version;
/// `--help` exits 0. The TEXT around the version is clap's,
/// unasserted. Both are clap's own exits, before the client probe.
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

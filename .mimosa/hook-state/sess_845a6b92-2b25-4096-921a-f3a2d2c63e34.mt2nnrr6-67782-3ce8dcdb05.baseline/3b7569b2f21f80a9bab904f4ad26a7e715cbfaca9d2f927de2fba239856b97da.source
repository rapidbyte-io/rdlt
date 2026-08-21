//! The spawned connector bin's shared contract pins (round-4 fix —
//! five connector suites carried verbatim copies of the argument and
//! version arms, and near-copies of the Spec-RPC identity arm; a
//! contract change had to land five times or one crate's smoke drifted
//! from the others'). This crate is the natural home: every spawn
//! suite already depends on it for the certification cells, and the
//! Spec arm needs the provider this crate already carries.
//!
//! Two helpers, deliberately split rather than bundled: the argument
//! contract is std-only and runs on ANY machine, while the Spec RPC
//! needs a servable bin — oracle's suite gates the latter on its
//! client probe and must keep running the former clientless.

use std::path::Path;
use std::process::Stdio;

use rdlt_runtime::{ConnectorRequirement, LocalBinaryConnectorProvider, Role};

/// The pinned ARGUMENT contract every served connector bin speaks: no
/// args → clap's exit 2; `--role=nonsense` → exit 2; and each role in
/// `unserved_roles` (`--role=<r>` for a role the crate does not serve)
/// equally exit 2, refused as an unrecognized VALUE by clap's arg gate
/// before any serve machinery. `--version` exits 0 and its output
/// contains `version` (the text around it is clap's, unasserted);
/// `--help` exits 0.
pub fn assert_bin_arg_contract(bin: &Path, unserved_roles: &[&str], version: &str) {
    let run = |args: &[&str]| {
        std::process::Command::new(bin)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .expect("the bin runs")
    };

    assert_eq!(run(&[]).status.code(), Some(2), "no args");
    assert_eq!(
        run(&["--role=nonsense"]).status.code(),
        Some(2),
        "--role=nonsense"
    );
    for role in unserved_roles {
        assert_eq!(
            run(&[&format!("--role={role}")]).status.code(),
            Some(2),
            "--role={role} — this crate does not serve that role"
        );
    }

    let printed = std::process::Command::new(bin)
        .arg("--version")
        .stderr(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(printed.status.code(), Some(0), "--version");
    let stdout = String::from_utf8(printed.stdout).expect("version output is UTF-8");
    assert!(
        stdout.contains(version),
        "`--version` output {stdout:?} must contain the crate version"
    );
    assert_eq!(run(&["--help"]).status.code(), Some(0), "--help");
}

/// The config-free Spec RPC identity for one role, through the same
/// provider the runtime spawns with: the reported name IS the
/// reverse-DNS `id` (the 039 identity rule), the version is the
/// crate's, and a config schema is present.
pub async fn assert_spec_identity(bin: &Path, role: Role, id: &str, version: &str) {
    let provider = LocalBinaryConnectorProvider::new();
    let requirement = ConnectorRequirement::new(id).with_path(bin);
    let spec = provider
        .spec_for_role(&requirement, role)
        .await
        .unwrap_or_else(|error| panic!("the {role:?} half answers the Spec RPC: {error}"));
    assert_eq!(spec.name, id, "the NAME const is the connector id");
    assert_eq!(spec.version, version, "one workspace version everywhere");
    assert!(
        spec.config_schema.is_some(),
        "a served half publishes a config schema"
    );
}

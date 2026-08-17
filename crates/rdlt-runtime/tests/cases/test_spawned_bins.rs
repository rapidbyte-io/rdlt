//! The T6 smoke: the REAL `rdlt-connector-reference` binary — the
//! contract's own connector, dual-role — spawned and driven through the
//! provider, plus the bin's pinned arg behavior (exit codes and the
//! version string; clap's usage TEXT is deliberately unasserted — clap
//! owns it). The seven first-party connectors are not spawn subjects
//! here: an engine gate that leaned on them would go dark when they
//! move to their own repository (044), while the reference connector
//! lives beside the engine forever.
//!
//! These are also the pins for THE IDENTITY RULE (039 T6): a
//! connector's `NAME` const IS its connector id, spelled reverse-DNS
//! (`io.rapidbyte.reference`), so the strict-equality handshake
//! verification (D-039-2) and D-039-1's last-segment binary discovery
//! both derive from one const. The handshakes below succeeding against
//! the real bin with a reverse-DNS requirement id is that rule holding
//! end-to-end.
//!
//! Bin location goes through the testkit's ONE spawn scaffold
//! (`rdlt_testkit::spawn::built_connector_bin`, the 042 dedup): the
//! `CARGO_TARGET_DIR` resolution, the `RDLT_BUILD_CONNECTOR_BINS`
//! build guard and the loud missing-bin refusal live there once for
//! every spawn suite.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;

use rdlt_connector_client::wire::{DEFAULT_DEADLINE, connector_client, dial};
use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::SpecRequest;
use rdlt_runtime::{
    ClientError, ConnectorProvider, ConnectorRequirement, LocalBinaryConnectorProvider,
    ProviderError, Role,
};
use tokio::io::{AsyncBufReadExt, BufReader};

/// The built reference bin, through the shared scaffold. `pub(crate)`
/// because the T8 headline e2e (`test_e2e_spawned`) spawns the same
/// bin — one helper, one location rule.
pub(crate) fn built_bin() -> PathBuf {
    rdlt_testkit::spawn::built_connector_bin(env!("CARGO_MANIFEST_DIR"), "rdlt-connector-reference")
}

/// The workspace root: two levels above this crate's own manifest —
/// where `built_cli`'s cargo invocation must run.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt-runtime sits two levels below the workspace root")
        .to_path_buf()
}

/// Where debug binaries land: the directory the scaffold-built
/// reference bin sits in — the discovery tests' search path and the
/// CLI's location share the connector bin's own resolution rather than
/// carrying a second copy of the target-dir rule.
fn target_debug_dir() -> PathBuf {
    built_bin()
        .parent()
        .expect("a built bin has a parent directory")
        .to_path_buf()
}

/// A destination-only SCRIPT FAKE: its arg gate accepts only
/// `--role=destination` and exits 2 for anything else — the shape a
/// single-role connector's clap gate presents to a spawn. The
/// dual-role reference bin cannot exhibit a role refusal, so the
/// refusal legs below ride this fake (the test_local_provider
/// pattern) while every served leg rides the real bin.
fn write_destination_only_fake(dir: &Path) -> PathBuf {
    let path = dir.join("rdlt-connector-destonly");
    std::fs::write(
        &path,
        "#!/bin/sh\ncase \"$1\" in\n  --role=destination) exec sleep 30 ;;\n  *) exit 2 ;;\nesac\n",
    )
    .expect("the fake script writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("the fake script becomes executable");
    path
}

/// The reference bin serves its SOURCE half: spawn through the provider
/// with a path override and a real config, complete the wire handshake
/// (strict identity: the requirement id `io.rapidbyte.reference` must
/// equal the reported `NAME` byte-for-byte), and get `streams()`
/// answered over the wire — the one stream named by the file's stem.
#[tokio::test]
async fn the_reference_bin_serves_a_source_handshake_and_streams() {
    let bin = built_bin();
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("events.jsonl");
    std::fs::write(&file, "{\"id\":1}\n{\"id\":2}\n").expect("the fixture file writes");
    let config = serde_json::json!({ "path": file });

    let provider = LocalBinaryConnectorProvider::new();
    let managed = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.reference").with_path(&bin),
            &config,
        )
        .await
        .expect("spawn + handshake against the real source bin succeeds");

    assert_eq!(managed.identity(), "io.rapidbyte.reference");
    // One workspace version everywhere, so this crate's own version IS
    // the reference connector's.
    assert_eq!(managed.resolved_version(), env!("CARGO_PKG_VERSION"));

    let streams = rdlt_connector::source::Source::streams(&managed)
        .await
        .expect("streams() answers over the wire");
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].name.as_str(), "events");
}

/// The reference bin serves its DESTINATION half — same shape, plus the
/// exact-version pin (D-039-2) exercised against the real binary.
#[tokio::test]
async fn the_reference_bin_serves_a_destination_handshake() {
    let bin = built_bin();
    let dir = tempfile::tempdir().expect("tempdir");
    let config = serde_json::json!({ "path": dir.path().join("out") });

    let provider = LocalBinaryConnectorProvider::new();
    let managed = provider
        .destination(
            &ConnectorRequirement::new("io.rapidbyte.reference")
                .with_path(&bin)
                .with_version(env!("CARGO_PKG_VERSION")),
            &config,
        )
        .await
        .expect("spawn + handshake against the real destination bin succeeds");

    assert_eq!(managed.identity(), "io.rapidbyte.reference");
    assert_eq!(managed.resolved_version(), env!("CARGO_PKG_VERSION"));
}

/// The reference bin answers the config-free `Spec` RPC — its static
/// identity, BEFORE any handshake, so no config document is involved.
/// The reported name is the reverse-DNS id, straight off the raw wire:
/// this test dials the advertised socket itself, below the provider's
/// adapters.
#[tokio::test]
async fn the_reference_bin_answers_the_spec_rpc_before_any_handshake() {
    let bin = built_bin();
    let mut child = tokio::process::Command::new(&bin)
        .arg("--role=destination")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the reference bin spawns");

    let stdout = child
        .stdout
        .take()
        .expect("stdout was piped at spawn, so the child carries it");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(10), reader.read_line(&mut line))
        .await
        .expect("the handshake line arrives within its budget")
        .expect("the handshake line reads");
    let parsed = Line::parse(line.trim_end_matches(['\n', '\r']))
        .expect("the bin's first stdout line is a valid handshake line");

    let channel = dial(
        &parsed.socket_path,
        MAX_FRAME_BYTES as u64,
        DEFAULT_DEADLINE,
    )
    .await
    .expect("the advertised socket dials");
    let reply = connector_client(channel)
        .spec(SpecRequest {})
        .await
        .expect("Spec answers before any handshake")
        .into_inner();
    let spec: serde_json::Value =
        serde_json::from_slice(&reply.spec_json).expect("spec_json decodes");
    assert_eq!(spec["name"], "io.rapidbyte.reference");
    assert_eq!(spec["version"], env!("CARGO_PKG_VERSION"));

    // The consumer contract every production path honors
    // (LifecycleGuard, the certifier's guard and probe): whoever read
    // the handshake line unlinks the socket FILE on cleanup — the
    // SIGKILLed child (`kill_on_drop`) cannot do it itself, and an
    // orphaned socket keeps its private serve directory non-empty
    // forever, defeating the sdk's rmdir-only startup sweep. This test
    // is the one raw spawn outside the guards, so it cleans up by hand.
    let _ = std::fs::remove_file(&parsed.socket_path);
}

/// THE ID UX pin (039 T7): the reverse-DNS spelling IS the id. A
/// dotless `id: reference` reaches the same binary — discovery's
/// convention takes the last `.`-segment, so both spellings resolve to
/// `rdlt-connector-reference` — but the handshake then REFUSES it as
/// an identity mismatch naming both spellings, actionably and typed.
/// No magic normalization anywhere: the fix is spelling the real id.
#[tokio::test]
async fn a_dotless_id_reaches_the_binary_but_is_refused_as_an_identity_mismatch() {
    let bin = built_bin();
    let provider = LocalBinaryConnectorProvider::new();
    let error = provider
        .source(
            &ConnectorRequirement::new("reference").with_path(&bin),
            &serde_json::json!({ "path": "/tmp/absent.jsonl" }),
        )
        .await
        .expect_err("a dotless id must not pass the strict-identity handshake");
    match &error {
        ProviderError::Client(ClientError::IdentityMismatch { expected, reported }) => {
            assert_eq!(expected, "reference");
            assert_eq!(reported, "io.rapidbyte.reference");
        }
        other => panic!("expected the typed IdentityMismatch, got: {other:?}"),
    }
    assert_eq!(
        error.to_string(),
        "connector identity mismatch: required `reference`, the connector reported \
         `io.rapidbyte.reference`",
        "the rendered refusal names BOTH spellings — the operator's fix is in the message"
    );
}

/// The config-free `Spec` probe behind `rdlt schema <id>`: the
/// dual-role reference bin answers on the FIRST (source) probe with a
/// real JSON Schema document — no credentials, no config, no
/// handshake. (The destination-only shape — a clean source-role
/// refusal retried as destination — is test_local_provider's
/// `the_spec_probe_retries_only_a_clean_role_refusal`, on script
/// fakes: no single-role first-party bin lives in this repo.)
#[tokio::test]
async fn the_spec_probe_answers_source_first_for_the_dual_role_bin() {
    let provider = LocalBinaryConnectorProvider::new();

    let bin = built_bin();
    let spec = provider
        .spec(&ConnectorRequirement::new("io.rapidbyte.reference").with_path(&bin))
        .await
        .expect("the dual-role reference bin answers the source probe");
    assert_eq!(spec.name, "io.rapidbyte.reference");
    assert_eq!(spec.version, env!("CARGO_PKG_VERSION"));
    let schema = spec
        .config_schema
        .expect("the reference source publishes a config schema");
    assert!(
        schema.is_object() && schema.get("properties").is_some(),
        "the schema is a JSON Schema document: {schema}"
    );
}

/// The `Spec` probe verifies identity like the run path (D-039-2): the
/// last-segment convention resolves `com.example.reference` to the
/// REAL `rdlt-connector-reference`, and without this gate `rdlt schema
/// com.example.reference` would print the wrong connector's schema as
/// if it were the asked-for one. Refused typed, the rendered message
/// naming both spellings — the explicit-path probes above stay
/// uncheckable by design (a path names a binary, not an id) and keep
/// passing.
#[tokio::test]
async fn the_spec_probe_refuses_a_discovered_binary_with_the_wrong_identity() {
    // Discovery, not a path override: the built bin's directory IS the
    // search path, so `com.example.reference` resolves the real bin.
    let _ = built_bin();
    let provider = LocalBinaryConnectorProvider::new().with_search_path(target_debug_dir());
    let error = provider
        .spec(&ConnectorRequirement::new("com.example.reference"))
        .await
        .expect_err("a foreign id must not pass the Spec probe's identity gate");
    match &error {
        ProviderError::Client(ClientError::IdentityMismatch { expected, reported }) => {
            assert_eq!(expected, "com.example.reference");
            assert_eq!(reported, "io.rapidbyte.reference");
        }
        other => panic!("expected the typed IdentityMismatch, got: {other:?}"),
    }
    assert_eq!(
        error.to_string(),
        "connector identity mismatch: required `com.example.reference`, the connector reported \
         `io.rapidbyte.reference`",
        "the rendered refusal names BOTH spellings — the operator's fix is in the message"
    );
}

/// The rdlt CLI itself, for the `schema --role` door (040 T9): built
/// under the same env guard as the connector bins (the Makefile's
/// spawn-bins line sets it), located by the same target-dir rule, and
/// failing loudly with instructions when absent — never building
/// behind the runner's back.
fn built_cli() -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        if std::env::var_os("RDLT_BUILD_CONNECTOR_BINS").is_none() {
            // Same opt-in rebuild rule as the connector bin, and the
            // same residue: an already-present CLI is used as-is.
            eprintln!(
                "note: RDLT_BUILD_CONNECTOR_BINS is unset — using the rdlt CLI already \
                 on disk WITHOUT rebuilding; it may predate the current source."
            );
            return;
        }
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        // `test --no-run`, not `build`: the workspace gate compiles the
        // CLI through test-unified features (its own suite spawns the
        // bin via CARGO_BIN_EXE), and a plain `build` resolves a SECOND
        // feature variant of the whole facade chain — minutes of
        // duplicate compilation the gate's cache can never share.
        let status = std::process::Command::new(&cargo)
            .current_dir(workspace_root())
            .args(["test", "-p", "rdlt-cli", "--no-run"])
            .status()
            .unwrap_or_else(|error| {
                panic!("cargo test -p rdlt-cli --no-run did not spawn: {error}")
            });
        assert!(status.success(), "cargo test -p rdlt-cli --no-run failed");
    });
    let path = target_debug_dir().join("rdlt");
    assert!(
        path.is_file(),
        "the rdlt CLI `{}` is not built — run the Makefile's spawn-bins line \
         (it sets RDLT_BUILD_CONNECTOR_BINS=1) or `cargo build -p rdlt-cli` first",
        path.display()
    );
    path
}

/// `spec_for_role` asks exactly the named half — no probing, no silent
/// retry as the other role. Against the dual-role reference bin the two
/// halves answer DIFFERENT schemas (one file in, one directory out),
/// and the role-less `spec()` probe answers the SOURCE one (039's
/// source-first behavior, now by delegation). Against a
/// destination-only arg gate — the script fake, since no single-role
/// first-party bin lives in this repo — Source is a spawn-tier refusal
/// (exit 2, no handshake line ever arrives), never a silent retry.
#[tokio::test]
async fn spec_for_role_asks_exactly_the_named_half() {
    let provider = LocalBinaryConnectorProvider::new();

    let bin = built_bin();
    let requirement = ConnectorRequirement::new("io.rapidbyte.reference").with_path(&bin);
    let source = provider
        .spec_for_role(&requirement, Role::Source)
        .await
        .expect("the reference bin answers its source half");
    let destination = provider
        .spec_for_role(&requirement, Role::Destination)
        .await
        .expect("the reference bin answers its destination half");
    assert_eq!(source.name, "io.rapidbyte.reference");
    assert_eq!(destination.name, "io.rapidbyte.reference");
    assert_ne!(
        source.config_schema, destination.config_schema,
        "the two halves publish different config schemas"
    );
    let probed = provider
        .spec(&requirement)
        .await
        .expect("the role-less probe still answers");
    assert_eq!(
        probed.config_schema, source.config_schema,
        "no role = 039's source-first probe, byte-for-byte the source schema"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let fake = write_destination_only_fake(dir.path());
    let requirement = ConnectorRequirement::new("io.rapidbyte.destonly").with_path(&fake);
    let error = provider
        .spec_for_role(&requirement, Role::Source)
        .await
        .expect_err("the destination-only arg gate refuses the source role");
    match error {
        ProviderError::ExitedBeforeHandshake { status, .. } => {
            assert_eq!(status.code(), Some(2), "the arg-gate refusal is exit 2")
        }
        other => panic!(
            "the refusal is the spawn tier's own (no handshake line), never a silent \
             retry as the other half: {other:?}"
        ),
    }
}

/// `spec_for_role` keeps the id-resolution identity gate (D-039-2,
/// the 040 T7 rule): a DISCOVERED binary whose reported name differs
/// from the requirement id is refused — the last-segment convention
/// would otherwise resolve `com.example.reference` to the real
/// `rdlt-connector-reference` and answer with the wrong connector's
/// schema.
#[tokio::test]
async fn spec_for_role_refuses_a_discovered_binary_with_the_wrong_identity() {
    let _ = built_bin();
    let provider = LocalBinaryConnectorProvider::new().with_search_path(target_debug_dir());
    let error = provider
        .spec_for_role(
            &ConnectorRequirement::new("com.example.reference"),
            Role::Source,
        )
        .await
        .expect_err("a foreign id must not pass the role probe's identity gate");
    match &error {
        ProviderError::Client(ClientError::IdentityMismatch { expected, reported }) => {
            assert_eq!(expected, "com.example.reference");
            assert_eq!(reported, "io.rapidbyte.reference");
        }
        other => panic!("expected the typed IdentityMismatch, got: {other:?}"),
    }
    assert_eq!(
        error.to_string(),
        "connector identity mismatch: required `com.example.reference`, the connector reported \
         `io.rapidbyte.reference`",
        "the rendered refusal names BOTH spellings — the operator's fix is in the message"
    );
}

/// The CLI door on `spec_for_role` (040 T9): `rdlt schema <bin>
/// --role destination` prints the DESTINATION schema, different from
/// the flagless output — which itself stays 039's source-first probe,
/// byte-identical to `--role source`. ONE tier since the 043 D1 swap:
/// every spelling spawns, so there is no compiled output to compare
/// against — the two halves answering differently over the same bin
/// is the whole claim.
#[test]
fn the_cli_schema_role_flag_selects_the_destination_half() {
    let cli = built_cli();
    let reference_bin = built_bin();
    let run = |args: &[&str]| {
        let out = std::process::Command::new(&cli)
            .args(args)
            .output()
            .expect("the rdlt CLI runs");
        assert_eq!(out.status.code(), Some(0), "{args:?}: {out:?}");
        out.stdout
    };
    let bin = reference_bin.to_string_lossy();

    let flagless = run(&["schema", &bin]);
    let source = run(&["schema", &bin, "--role", "source"]);
    let destination = run(&["schema", &bin, "--role", "destination"]);

    assert_ne!(
        destination, flagless,
        "the destination schema differs from the source-first flagless output"
    );
    assert_eq!(
        flagless, source,
        "no flag stays 039's source-first probe, byte-identical to --role source"
    );
}

/// The pinned arg contract: no args → exit 2, an unrecognized role →
/// exit 2. (A single-role bin refusing the role it does not carry at
/// the same gate is each ported connector's own spawn-suite pin — the
/// dual-role reference bin carries both halves by design.)
#[test]
fn bad_args_exit_2() {
    let bin = built_bin();
    let no_args = std::process::Command::new(&bin)
        .stderr(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(no_args.status.code(), Some(2), "{}: no args", bin.display());

    let bad_role = std::process::Command::new(&bin)
        .arg("--role=nonsense")
        .stderr(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(
        bad_role.status.code(),
        Some(2),
        "{}: --role=nonsense",
        bin.display()
    );
}

/// `--version` succeeds and its output contains the crate version (one
/// workspace version, so this crate's own is the bin's); `--help`
/// exits 0. The TEXT around the version is clap's, unasserted.
#[test]
fn version_and_help_behave() {
    let bin = built_bin();
    let version = std::process::Command::new(&bin)
        .arg("--version")
        .output()
        .expect("the bin runs");
    assert_eq!(version.status.code(), Some(0), "{}", bin.display());
    let stdout = String::from_utf8(version.stdout).expect("version output is UTF-8");
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "{}: `--version` output {stdout:?} must contain the crate version",
        bin.display()
    );

    let help = std::process::Command::new(&bin)
        .arg("--help")
        .stdout(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(help.status.code(), Some(0), "{}: --help", bin.display());
}

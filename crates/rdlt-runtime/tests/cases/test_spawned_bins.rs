//! The T6 smoke: the REAL connector binaries — `rdlt-connector-file`
//! (both roles) and `rdlt-connector-snowflake` (destination-only) —
//! spawned and driven through the provider, plus the bins' pinned arg
//! behavior (exit codes and the version string; clap's usage TEXT is
//! deliberately unasserted — clap owns it).
//!
//! These are also the pins for THE IDENTITY RULE (039 T6): a
//! connector's `NAME` const IS its connector id, spelled reverse-DNS
//! (`io.rapidbyte.file`), so the strict-equality handshake verification
//! (D-039-2) and D-039-1's last-segment binary discovery both derive
//! from one const. The handshakes below succeeding against the real
//! bins with reverse-DNS requirement ids is that rule holding
//! end-to-end.
//!
//! Bin location follows the rdlt-bench precedent (`CARGO_TARGET_DIR`
//! honored, else the repo's own `target/`); the build itself is guarded
//! by `RDLT_BUILD_CONNECTOR_BINS` — the Makefile line sets it, and a
//! bare `cargo nextest run --features spawn-bins` without it still
//! fails LOUDLY when the bins are missing rather than building behind
//! the runner's back.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;

use rdlt_connector_client::{connector_client, dial};
use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::handshake::Line;
use rdlt_connector_protocol::proto::SpecRequest;
use rdlt_runtime::{
    ClientError, ConnectorProvider, ConnectorRequirement, LocalBinaryConnectorProvider,
    ProviderError, Role,
};
use tokio::io::{AsyncBufReadExt, BufReader};

/// The workspace root: two levels above this crate's own manifest.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt-runtime sits two levels below the workspace root")
        .to_path_buf()
}

/// Where debug binaries land — `CARGO_TARGET_DIR` honored the way
/// rdlt-bench honors it for the CLI (absolute used as-is, relative
/// resolved against the repo root, exactly as cargo treats it).
fn target_debug_dir() -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(target) => {
            let target = PathBuf::from(target);
            if target.is_absolute() {
                target.join("debug")
            } else {
                workspace_root().join(target).join("debug")
            }
        }
        None => workspace_root().join("target/debug"),
    }
}

/// The path to a built connector bin, building BOTH bins first (once
/// per test process) when `RDLT_BUILD_CONNECTOR_BINS` is set — the
/// Makefile line sets it. Without the env var a missing bin fails with
/// instructions, never silently: a smoke suite that quietly built or
/// quietly skipped would be the 024 class wearing a new hat.
///
/// `pub(crate)` because the T8 headline e2e (`test_e2e_file`) locates
/// the same bins the same way — one helper, one location rule.
pub(crate) fn built_bin(name: &str) -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        if std::env::var_os("RDLT_BUILD_CONNECTOR_BINS").is_none() {
            // Opt-in rebuild, deliberately: the gate line sets the var,
            // and a developer running this suite in a loop should not
            // pay a cargo invocation per run. The residue is that a
            // STALE bin passes green here, so say so out loud — silence
            // is what would make an hours-old binary look like evidence
            // about the current tree.
            eprintln!(
                "note: RDLT_BUILD_CONNECTOR_BINS is unset — spawning the connector \
                 binaries already on disk WITHOUT rebuilding. Whatever this suite \
                 proves is about those binaries, not necessarily the current source. \
                 The Makefile's spawn-bins lines set the var."
            );
            return;
        }
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        for package in ["rdlt-connector-file", "rdlt-connector-snowflake"] {
            let status = std::process::Command::new(&cargo)
                .current_dir(workspace_root())
                .args([
                    "build",
                    "-p",
                    package,
                    "--features",
                    "bin-serve",
                    "--bin",
                    package,
                ])
                .status()
                .unwrap_or_else(|error| panic!("cargo build -p {package} did not spawn: {error}"));
            assert!(
                status.success(),
                "cargo build -p {package} --features bin-serve failed"
            );
        }
    });
    let path = target_debug_dir().join(name);
    assert!(
        path.is_file(),
        "connector binary `{}` is not built — run the Makefile's spawn-bins \
         line (it sets RDLT_BUILD_CONNECTOR_BINS=1) or `cargo build -p {name} \
         --features bin-serve` first",
        path.display()
    );
    path
}

/// The file bin serves its SOURCE half: spawn through the provider with
/// a path override and a real config, complete the wire handshake
/// (strict identity: the requirement id `io.rapidbyte.file` must equal
/// the reported `NAME` byte-for-byte), and get `streams()` answered
/// over the wire.
#[tokio::test]
async fn the_file_bin_serves_a_source_handshake_and_streams() {
    let bin = built_bin("rdlt-connector-file");
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("rows.jsonl"), "{\"id\":1}\n{\"id\":2}\n")
        .expect("the fixture file writes");
    let config = serde_json::json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": format!("{}/*.jsonl", dir.path().display()),
        }]
    });

    let provider = LocalBinaryConnectorProvider::new();
    let managed = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.file").with_path(&bin),
            &config,
        )
        .await
        .expect("spawn + handshake against the real source bin succeeds");

    assert_eq!(managed.identity(), "io.rapidbyte.file");
    // One workspace version everywhere, so this crate's own version IS
    // the file connector's.
    assert_eq!(managed.resolved_version(), env!("CARGO_PKG_VERSION"));

    let streams = rdlt_connector::Source::streams(&managed)
        .await
        .expect("streams() answers over the wire");
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].name.as_str(), "events");
}

/// The file bin serves its DESTINATION half — same shape, plus the
/// exact-version pin (D-039-2) exercised against the real binary.
#[tokio::test]
async fn the_file_bin_serves_a_destination_handshake() {
    let bin = built_bin("rdlt-connector-file");
    let dir = tempfile::tempdir().expect("tempdir");
    let config = serde_json::json!({ "path": dir.path().to_string_lossy() });

    let provider = LocalBinaryConnectorProvider::new();
    let managed = provider
        .destination(
            &ConnectorRequirement::new("io.rapidbyte.file")
                .with_path(&bin)
                .with_version(env!("CARGO_PKG_VERSION")),
            &config,
        )
        .await
        .expect("spawn + handshake against the real destination bin succeeds");

    assert_eq!(managed.identity(), "io.rapidbyte.file");
    assert_eq!(managed.resolved_version(), env!("CARGO_PKG_VERSION"));
}

/// The snowflake bin spawns and answers the config-free `Spec` RPC —
/// its static identity, BEFORE any handshake, so no credentials and no
/// account are involved (its conformance is 040). The reported name is
/// the reverse-DNS id, same rule as file.
#[tokio::test]
async fn the_snowflake_bin_answers_the_spec_rpc_without_credentials() {
    let bin = built_bin("rdlt-connector-snowflake");
    let mut child = tokio::process::Command::new(&bin)
        .arg("--role=destination")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the snowflake bin spawns");

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

    let channel = dial(&parsed.socket_path, MAX_FRAME_BYTES as u64)
        .await
        .expect("the advertised socket dials");
    let reply = connector_client(channel)
        .spec(SpecRequest {})
        .await
        .expect("Spec answers before any handshake")
        .into_inner();
    let spec: serde_json::Value =
        serde_json::from_slice(&reply.spec_json).expect("spec_json decodes");
    assert_eq!(spec["name"], "io.rapidbyte.snowflake");
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
/// dotless `id: file` reaches the same binary — discovery's convention
/// takes the last `.`-segment, so both spellings resolve to
/// `rdlt-connector-file` — but the handshake then REFUSES it as an
/// identity mismatch naming both spellings, actionably and typed.
/// No magic normalization anywhere: the fix is spelling the real id.
#[tokio::test]
async fn a_dotless_id_reaches_the_binary_but_is_refused_as_an_identity_mismatch() {
    let bin = built_bin("rdlt-connector-file");
    let provider = LocalBinaryConnectorProvider::new();
    let error = provider
        .source(
            &ConnectorRequirement::new("file").with_path(&bin),
            &serde_json::json!({
                "streams": [{"name": "events", "format": "jsonl", "path": "/tmp/*.jsonl"}]
            }),
        )
        .await
        .expect_err("a dotless id must not pass the strict-identity handshake");
    match &error {
        ProviderError::Client(ClientError::IdMismatch { expected, reported }) => {
            assert_eq!(expected, "file");
            assert_eq!(reported, "io.rapidbyte.file");
        }
        other => panic!("expected the typed IdMismatch, got: {other:?}"),
    }
    assert_eq!(
        error.to_string(),
        "connector identity mismatch: required `file`, the connector reported \
         `io.rapidbyte.file`",
        "the rendered refusal names BOTH spellings — the operator's fix is in the message"
    );
}

/// The config-free `Spec` probe behind `rdlt schema <id>`: the
/// dual-role file bin answers on the FIRST (source) probe with its
/// source schema; the destination-only snowflake bin refuses the
/// source role at its arg gate and answers on the destination RETRY —
/// both without credentials, config or a handshake.
#[tokio::test]
async fn the_spec_probe_answers_for_both_bin_shapes() {
    let provider = LocalBinaryConnectorProvider::new();

    let file_bin = built_bin("rdlt-connector-file");
    let spec = provider
        .spec(&ConnectorRequirement::new("io.rapidbyte.file").with_path(&file_bin))
        .await
        .expect("the dual-role file bin answers the source probe");
    assert_eq!(spec.name, "io.rapidbyte.file");
    assert_eq!(spec.version, env!("CARGO_PKG_VERSION"));
    let schema = spec
        .config_schema
        .expect("the file source publishes a config schema");
    assert!(
        schema.is_object() && schema.get("properties").is_some(),
        "the schema is a JSON Schema document: {schema}"
    );

    let snowflake_bin = built_bin("rdlt-connector-snowflake");
    let spec = provider
        .spec(&ConnectorRequirement::new("io.rapidbyte.snowflake").with_path(&snowflake_bin))
        .await
        .expect("the destination-only snowflake bin answers on the destination retry");
    assert_eq!(spec.name, "io.rapidbyte.snowflake");
    assert!(
        spec.config_schema.is_some(),
        "the snowflake destination publishes a config schema"
    );
}

/// The `Spec` probe verifies identity like the run path (D-039-2): the
/// last-segment convention resolves `com.example.file` to the REAL
/// `rdlt-connector-file`, and without this gate `rdlt schema
/// com.example.file` would print the wrong connector's schema as if it
/// were the asked-for one. Refused typed, the rendered message naming
/// both spellings — the explicit-path probes above stay uncheckable by
/// design (a path names a binary, not an id) and keep passing.
#[tokio::test]
async fn the_spec_probe_refuses_a_discovered_binary_with_the_wrong_identity() {
    // Discovery, not a path override: the built bins' directory IS the
    // search path, so `com.example.file` resolves the real file bin.
    let _ = built_bin("rdlt-connector-file");
    let provider = LocalBinaryConnectorProvider::new().with_search_path(target_debug_dir());
    let error = provider
        .spec(&ConnectorRequirement::new("com.example.file"))
        .await
        .expect_err("a foreign id must not pass the Spec probe's identity gate");
    match &error {
        ProviderError::Client(ClientError::IdMismatch { expected, reported }) => {
            assert_eq!(expected, "com.example.file");
            assert_eq!(reported, "io.rapidbyte.file");
        }
        other => panic!("expected the typed IdMismatch, got: {other:?}"),
    }
    assert_eq!(
        error.to_string(),
        "connector identity mismatch: required `com.example.file`, the connector reported \
         `io.rapidbyte.file`",
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
            // Same opt-in rebuild rule as the connector bins above, and
            // the same residue: an already-present CLI is used as-is.
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

/// `spec_for_role` asks exactly the named half — no probing, no
/// silent retry as the other role. Against the dual-role file bin the
/// two halves answer DIFFERENT schemas, and the role-less `spec()`
/// probe answers the SOURCE one (039's source-first behavior, now by
/// delegation). Against the destination-only snowflake bin, Source is
/// a spawn-tier refusal (its arg gate rejects the role, so no
/// handshake line ever arrives) while Destination answers.
#[tokio::test]
async fn spec_for_role_asks_exactly_the_named_half() {
    let provider = LocalBinaryConnectorProvider::new();

    let file_bin = built_bin("rdlt-connector-file");
    let requirement = ConnectorRequirement::new("io.rapidbyte.file").with_path(&file_bin);
    let source = provider
        .spec_for_role(&requirement, Role::Source)
        .await
        .expect("the file bin answers its source half");
    let destination = provider
        .spec_for_role(&requirement, Role::Destination)
        .await
        .expect("the file bin answers its destination half");
    assert_eq!(source.name, "io.rapidbyte.file");
    assert_eq!(destination.name, "io.rapidbyte.file");
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

    let snowflake_bin = built_bin("rdlt-connector-snowflake");
    let requirement = ConnectorRequirement::new("io.rapidbyte.snowflake").with_path(&snowflake_bin);
    let error = provider
        .spec_for_role(&requirement, Role::Source)
        .await
        .expect_err("the destination-only bin's arg gate refuses the source role");
    assert!(
        matches!(error, ProviderError::HandshakeLine { .. }),
        "the refusal is the spawn tier's own (no handshake line), never a silent \
         retry as the other half: {error:?}"
    );
    let destination = provider
        .spec_for_role(&requirement, Role::Destination)
        .await
        .expect("the snowflake bin answers its destination half");
    assert_eq!(destination.name, "io.rapidbyte.snowflake");
    assert!(
        destination.config_schema.is_some(),
        "the snowflake destination publishes a config schema"
    );
}

/// `spec_for_role` keeps the id-resolution identity gate (D-039-2,
/// the 040 T7 rule): a DISCOVERED binary whose reported name differs
/// from the requirement id is refused — the last-segment convention
/// would otherwise resolve `com.example.file` to the real
/// `rdlt-connector-file` and answer with the wrong connector's schema.
#[tokio::test]
async fn spec_for_role_refuses_a_discovered_binary_with_the_wrong_identity() {
    let _ = built_bin("rdlt-connector-file");
    let provider = LocalBinaryConnectorProvider::new().with_search_path(target_debug_dir());
    let error = provider
        .spec_for_role(&ConnectorRequirement::new("com.example.file"), Role::Source)
        .await
        .expect_err("a foreign id must not pass the role probe's identity gate");
    match &error {
        ProviderError::Client(ClientError::IdMismatch { expected, reported }) => {
            assert_eq!(expected, "com.example.file");
            assert_eq!(reported, "io.rapidbyte.file");
        }
        other => panic!("expected the typed IdMismatch, got: {other:?}"),
    }
    assert_eq!(
        error.to_string(),
        "connector identity mismatch: required `com.example.file`, the connector reported \
         `io.rapidbyte.file`",
        "the rendered refusal names BOTH spellings — the operator's fix is in the message"
    );
}

/// The CLI door on `spec_for_role` (040 T9): `rdlt schema <file-bin>
/// --role destination` prints the DESTINATION schema, byte-identical
/// to the compiled `file-dest` spelling's output (one crate, one
/// schema, two tiers) and different from the flagless output — which
/// itself stays 039's source-first probe, byte-identical to
/// `--role source`.
#[test]
fn the_cli_schema_role_flag_selects_the_destination_half() {
    let cli = built_cli();
    let file_bin = built_bin("rdlt-connector-file");
    let run = |args: &[&str]| {
        let out = std::process::Command::new(&cli)
            .args(args)
            .output()
            .expect("the rdlt CLI runs");
        assert_eq!(out.status.code(), Some(0), "{args:?}: {out:?}");
        out.stdout
    };
    let bin = file_bin.to_string_lossy();

    let flagless = run(&["schema", &bin]);
    let source = run(&["schema", &bin, "--role", "source"]);
    let destination = run(&["schema", &bin, "--role", "destination"]);
    let compiled_destination = run(&["schema", "file-dest"]);

    assert_eq!(
        destination, compiled_destination,
        "--role destination prints the file destination's schema, byte-identical \
         to the compiled `file-dest` spelling's"
    );
    assert_ne!(
        destination, flagless,
        "the destination schema differs from the source-first flagless output"
    );
    assert_eq!(
        flagless, source,
        "no flag stays 039's source-first probe, byte-identical to --role source"
    );
}

/// The pinned arg contract, both bins: no args → exit 2, an
/// unrecognized role → exit 2 — and snowflake, destination-only by
/// design, refuses `--role=source` at the SAME arg gate.
#[test]
fn bad_args_exit_2() {
    for bin in [
        built_bin("rdlt-connector-file"),
        built_bin("rdlt-connector-snowflake"),
    ] {
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

    let source_role = std::process::Command::new(built_bin("rdlt-connector-snowflake"))
        .arg("--role=source")
        .stderr(Stdio::null())
        .output()
        .expect("the bin runs");
    assert_eq!(
        source_role.status.code(),
        Some(2),
        "snowflake carries no source half — the role enum refuses it as any other non-value"
    );
}

/// `--version` succeeds and its output contains the crate version (one
/// workspace version, so this crate's own is both bins'); `--help`
/// exits 0. The TEXT around the version is clap's, unasserted.
#[test]
fn version_and_help_behave() {
    for bin in [
        built_bin("rdlt-connector-file"),
        built_bin("rdlt-connector-snowflake"),
    ] {
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
}

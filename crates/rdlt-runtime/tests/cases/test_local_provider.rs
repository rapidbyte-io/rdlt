//! The local provider against SCRIPT FAKES — shell scripts standing in
//! for connector binaries, so every pre-gRPC seam (discovery, the
//! override, the one-line read, the timeout, the guard) is measured
//! without a served connector existing yet (T6 builds the real ones;
//! T8 drives them end to end).
//!
//! The success-shaped fakes print a valid handshake line naming a
//! socket NOTHING listens on, so a run that consumed the line fails
//! next at the dial — `ProviderError::Client(ClientError::Dial)`
//! carrying that socket path is the proof the line was read, parsed,
//! and followed.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rdlt_connector::ConnectorSpec;
use rdlt_connector_protocol::proto::connector_server::{Connector, ConnectorServer};
use rdlt_connector_protocol::proto::{self, SpecReply};
use rdlt_runtime::{
    ClientError, ConnectorProvider, ConnectorRequirement, LifecycleGuard,
    LocalBinaryConnectorProvider, ProviderError,
};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

/// Write an executable shell script `name` into `dir`.
fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(&path, body).expect("the fake script writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("the fake script becomes executable");
    path
}

/// A fake connector: refuses unless spawned with the expected
/// `--role=` argument (pinning the provider's spawn contract), prints
/// one valid handshake line naming `socket`, then stays alive so the
/// kill has something real to do. `exec` matters: the shell REPLACES
/// itself with the sleep, so the pid the provider kills is the process
/// actually holding the pipes — without it the sleep would outlive its
/// shell as an orphan and nextest would flag the run leaky.
fn line_fake_body(role: &str, socket: &Path) -> String {
    format!(
        "#!/bin/sh\n[ \"$1\" = \"--role={role}\" ] || exit 3\necho 'rdlt-connector|1|0|0|{}'\nexec sleep 30\n",
        socket.display()
    )
}

/// Unwrap the `Client(Dial)` arm and return the socket path it names.
fn dial_path(error: ProviderError) -> PathBuf {
    match error {
        ProviderError::Client(ClientError::Dial { path, .. }) => path,
        other => panic!("expected Client(Dial), got {other:?}"),
    }
}

/// A connector control-plane fake that answers only the config-free
/// `Spec` RPC. The shell fake below owns role handling and advertises
/// this server only for its destination half.
#[derive(Debug)]
struct SpecServer;

#[tonic::async_trait]
impl Connector for SpecServer {
    async fn handshake(
        &self,
        _request: Request<proto::HandshakeRequest>,
    ) -> Result<Response<proto::HandshakeReply>, Status> {
        Err(Status::unimplemented("the fake answers Spec alone"))
    }

    async fn check(
        &self,
        _request: Request<proto::CheckRequest>,
    ) -> Result<Response<proto::CheckReply>, Status> {
        Err(Status::unimplemented("the fake answers Spec alone"))
    }

    async fn spec(
        &self,
        _request: Request<proto::SpecRequest>,
    ) -> Result<Response<SpecReply>, Status> {
        let mut spec = ConnectorSpec::new("io.rapidbyte.fake", "1.0.0");
        spec.config_schema = Some(serde_json::json!({"type": "object"}));
        Ok(Response::new(SpecReply {
            spec_json: serde_json::to_vec(&spec).expect("the fake spec serializes"),
        }))
    }
}

/// Serve the Spec-only fake at `socket` until the returned task is
/// aborted or the listener is closed.
fn serve_spec(socket: &Path) -> tokio::task::JoinHandle<()> {
    let listener = tokio::net::UnixListener::bind(socket).expect("the fake socket binds");
    let incoming = UnixListenerStream::new(listener);
    let serving = tonic::transport::Server::builder()
        .add_service(ConnectorServer::new(SpecServer))
        .serve_with_incoming(incoming);
    tokio::spawn(async move {
        let _ = serving.await;
    })
}

/// A role-dispatching fake advertises the in-process Spec server for
/// destination and runs `source_action` for the source probe.
fn spec_probe_body(socket: &Path, source_action: &str) -> String {
    format!(
        "#!/bin/sh\ncase \"$1\" in\n  --role=source) {source_action} ;;\n  --role=destination) echo 'rdlt-connector|1|0|0|{}'; exec sleep 30 ;;\n  *) exit 3 ;;\nesac\n",
        socket.display()
    )
}

/// The role-less Spec probe retries only a process that cleanly
/// refuses the source role. A hung source is its own failure even when
/// the same binary could answer as a destination.
#[tokio::test]
async fn the_spec_probe_retries_only_a_clean_role_refusal() {
    let hanging = tempfile::tempdir().expect("tempdir");
    let hanging_socket = hanging.path().join("spec.sock");
    let hanging_server = serve_spec(&hanging_socket);
    let hanging_bin = write_script(
        hanging.path(),
        "hanging-fake",
        &spec_probe_body(&hanging_socket, "exec sleep 30"),
    );
    let provider =
        LocalBinaryConnectorProvider::new().with_line_timeout(Duration::from_millis(300));
    let error = provider
        .spec(&ConnectorRequirement::new("io.rapidbyte.fake").with_path(&hanging_bin))
        .await
        .expect_err("a hung source probe must propagate its timeout");
    assert!(matches!(error, ProviderError::Timeout { .. }), "{error:?}");
    hanging_server.abort();

    let refusing = tempfile::tempdir().expect("tempdir");
    let refusing_socket = refusing.path().join("spec.sock");
    let refusing_server = serve_spec(&refusing_socket);
    let refusing_bin = write_script(
        refusing.path(),
        "refusing-fake",
        &spec_probe_body(&refusing_socket, "exit 2"),
    );
    let spec = provider
        .spec(&ConnectorRequirement::new("io.rapidbyte.fake").with_path(&refusing_bin))
        .await
        .expect("a clean source-role refusal retries as destination");
    assert_eq!(spec.name, "io.rapidbyte.fake");
    assert!(spec.config_schema.is_some());
    refusing_server.abort();
}

/// Discovery resolves `io.rapidbyte.fake` to `rdlt-connector-fake` on
/// the search path, spawns it with `--role=source`, and consumes its
/// handshake line — proven by the dial failing at exactly the socket
/// the fake advertised.
#[tokio::test]
async fn discovery_resolves_the_convention_and_consumes_the_line() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("never-bound.sock");
    write_script(
        dir.path(),
        "rdlt-connector-fake",
        &line_fake_body("source", &socket),
    );

    let provider = LocalBinaryConnectorProvider::new().with_search_path(dir.path());
    let error = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.fake"),
            &serde_json::json!({}),
        )
        .await
        .expect_err("nothing listens on the advertised socket — the dial must refuse");
    assert_eq!(dial_path(error), socket);
}

/// The destination half spawns with `--role=destination` — the fake
/// exits without a line for any other argument, so reaching the dial
/// proves the role was passed.
#[tokio::test]
async fn a_destination_spawn_carries_its_role() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("never-bound.sock");
    write_script(
        dir.path(),
        "rdlt-connector-fake",
        &line_fake_body("destination", &socket),
    );

    let provider = LocalBinaryConnectorProvider::new().with_search_path(dir.path());
    let error = provider
        .destination(
            &ConnectorRequirement::new("io.rapidbyte.fake"),
            &serde_json::json!({}),
        )
        .await
        .expect_err("nothing listens on the advertised socket — the dial must refuse");
    assert_eq!(dial_path(error), socket);
}

/// An explicit `path` on the requirement bypasses discovery entirely:
/// with a convention-named fake sitting ON the search path, the
/// override's line is the one consumed — the two fakes advertise
/// distinct sockets and the dial fails at the OVERRIDE's.
#[tokio::test]
async fn the_path_override_bypasses_discovery() {
    let on_path = tempfile::tempdir().expect("tempdir");
    let elsewhere = tempfile::tempdir().expect("tempdir");
    let path_socket = on_path.path().join("path-fake.sock");
    let override_socket = elsewhere.path().join("override-fake.sock");
    write_script(
        on_path.path(),
        "rdlt-connector-fake",
        &line_fake_body("source", &path_socket),
    );
    let override_binary = write_script(
        elsewhere.path(),
        "some-other-name",
        &line_fake_body("source", &override_socket),
    );

    let provider = LocalBinaryConnectorProvider::new().with_search_path(on_path.path());
    let error = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.fake").with_path(&override_binary),
            &serde_json::json!({}),
        )
        .await
        .expect_err("nothing listens on the advertised socket — the dial must refuse");
    assert_eq!(
        dial_path(error),
        override_socket,
        "the OVERRIDE's line must be the one consumed"
    );
}

/// The frozen NotFound spelling, full-string: it names the convention
/// AND the override.
#[tokio::test]
async fn a_missing_binary_refuses_with_the_frozen_spelling() {
    let empty = tempfile::tempdir().expect("tempdir");
    let provider = LocalBinaryConnectorProvider::new().with_search_path(empty.path());
    let error = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.nosuch"),
            &serde_json::json!({}),
        )
        .await
        .expect_err("an empty search path finds nothing");
    assert!(matches!(error, ProviderError::NotFound { .. }), "{error:?}");
    assert_eq!(
        error.to_string(),
        "connector `io.rapidbyte.nosuch`: no binary `rdlt-connector-nosuch` on PATH \
         and no explicit path was given — install it (e.g. cargo install \
         rdlt-connector-nosuch) or set path: in the connector requirement"
    );
}

/// A binary that never writes a line refuses as `Timeout` once the
/// provider's line budget (a 300 ms test knob) elapses.
#[tokio::test]
async fn a_silent_binary_times_out() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_script(
        dir.path(),
        "rdlt-connector-fake",
        "#!/bin/sh\nexec sleep 30\n",
    );

    let provider = LocalBinaryConnectorProvider::new()
        .with_search_path(dir.path())
        .with_line_timeout(Duration::from_millis(300));
    let error = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.fake"),
            &serde_json::json!({}),
        )
        .await
        .expect_err("a silent binary must time out");
    match error {
        ProviderError::Timeout { binary } => assert_eq!(binary, "rdlt-connector-fake"),
        other => panic!("expected Timeout, got {other:?}"),
    }
}

/// A binary that floods stdout WITHOUT a newline refuses typed as
/// `HandshakeLineOverflow` the moment the cap fills — before the
/// provider's line timeout, and without buffering the flood (the fake
/// writes 256 KiB against the 64 KiB cap; an uncapped read would have
/// held it all until the 10 s default timeout).
#[tokio::test]
async fn a_newline_less_flood_refuses_at_the_cap() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_script(
        dir.path(),
        "rdlt-connector-fake",
        // 256 KiB of 'x', no newline, then stay alive so the kill has
        // something real to do (see line_fake_body's exec note).
        "#!/bin/sh\nhead -c 262144 /dev/zero | tr '\\0' 'x'\nexec sleep 30\n",
    );

    let provider = LocalBinaryConnectorProvider::new().with_search_path(dir.path());
    let error = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.fake"),
            &serde_json::json!({}),
        )
        .await
        .expect_err("a newline-less flood must refuse");
    match &error {
        ProviderError::HandshakeLineOverflow { binary, limit } => {
            assert_eq!(binary, "rdlt-connector-fake");
            assert_eq!(*limit, 64 * 1024);
        }
        other => panic!("expected HandshakeLineOverflow, got {other:?}"),
    }
    assert_eq!(
        error.to_string(),
        "connector `rdlt-connector-fake` wrote 65536 bytes of stdout \
         without completing a handshake line"
    );
}

/// A binary that CLOSES stdout without exiting refuses typed as
/// `HandshakeLine` (the empty line) after the short EOF exit-grace —
/// never as a full-line-deadline `Timeout`: EOF means no handshake can
/// ever arrive, so there is nothing left to wait for. The 10 s default
/// line budget stays in force to prove the refusal beat it.
#[tokio::test]
async fn a_closed_stdout_with_a_live_child_refuses_without_the_line_deadline() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_script(
        dir.path(),
        "rdlt-connector-fake",
        "#!/bin/sh\nexec >&- \nexec sleep 30\n",
    );

    let provider = LocalBinaryConnectorProvider::new().with_search_path(dir.path());
    let started = std::time::Instant::now();
    let error = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.fake"),
            &serde_json::json!({}),
        )
        .await
        .expect_err("a closed stdout must refuse");
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "the refusal must come from the EOF grace, not the 10 s line \
         deadline (took {:?})",
        started.elapsed()
    );
    match &error {
        ProviderError::HandshakeLine { binary, .. } => assert_eq!(binary, "rdlt-connector-fake"),
        other => panic!("expected HandshakeLine, got {other:?}"),
    }
}

/// A first line that is not a handshake line refuses typed as
/// `HandshakeLine`, carrying the parse refusal as its cause.
#[tokio::test]
async fn a_garbage_line_refuses_typed() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_script(
        dir.path(),
        "rdlt-connector-fake",
        "#!/bin/sh\necho 'this is not a handshake line'\nexec sleep 30\n",
    );

    let provider = LocalBinaryConnectorProvider::new().with_search_path(dir.path());
    let error = provider
        .source(
            &ConnectorRequirement::new("io.rapidbyte.fake"),
            &serde_json::json!({}),
        )
        .await
        .expect_err("garbage must refuse");
    match &error {
        ProviderError::HandshakeLine { binary, .. } => assert_eq!(binary, "rdlt-connector-fake"),
        other => panic!("expected HandshakeLine, got {other:?}"),
    }
    assert!(
        error.to_string().contains("invalid handshake line"),
        "the rendering names the failure: {error}"
    );
}

/// Is `pid` gone? A zombie counts as dead: the guard's `start_kill`
/// deliberately does not wait (no blocking in Drop) — tokio's reaper
/// collects the exit — so the honest liveness read is /proc state, with
/// `Z` (killed, awaiting reap) as dead as absent.
fn process_dead(pid: u32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Err(_) => true,
        Ok(stat) => match stat.rsplit_once(')') {
            // The state char is the first field after the comm's
            // closing paren.
            Some((_, rest)) => matches!(rest.trim_start().chars().next(), None | Some('Z' | 'X')),
            None => true,
        },
    }
}

/// Dropping the guard kills the child and unlinks the socket file —
/// red-proven: with the `Drop` body emptied, this test fails on BOTH
/// asserts (the sleep survives the bound, the socket file remains).
///
/// The child is spawned WITHOUT `kill_on_drop`, so the kill observed
/// here can only be the guard's own `start_kill`, not the `Child`
/// destructor's belt.
#[tokio::test]
async fn the_guard_drop_kills_the_child_and_unlinks_the_socket() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("guarded.sock");
    std::fs::write(&socket, b"").expect("a stand-in socket file writes");

    let child = tokio::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("sleep spawns");
    let guard = LifecycleGuard::new(child, &socket);
    let pid = guard.pid().expect("a just-spawned child has a pid");
    assert!(!process_dead(pid), "the child is alive while guarded");
    assert_eq!(guard.socket_path(), socket);

    drop(guard);

    let mut dead = false;
    for _ in 0..100 {
        if process_dead(pid) {
            dead = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(dead, "the guard's drop must kill the child (pid {pid})");
    assert!(
        !socket.exists(),
        "the guard's drop must unlink the socket file"
    );
}

//! Plumbing shared by every `serve()` service: standing up the Unix
//! domain socket the handshake line advertises, and the [`proto::ErrorFrame`]
//! encoding every RPC error path converges on.
//!
//! NOT HERE: gRPC client flow-control window sizing. The research spike
//! (`specs/038-connector-protocol/research.md` #2) measured that h2's
//! flow-control window bounds a `Read` stream's in-flight bytes
//! correctly (D6: rides h2 for v1, no explicit credit message) — but
//! also that tonic's CLIENT `Endpoint` defaults to a ~2 MiB window
//! rather than HTTP/2's raw 64 KiB default when the caller never calls
//! `initial_stream_window_size`/`initial_connection_window_size`
//! explicitly. That knob belongs to the adapter that DIALS a connector
//! (039) — a `serve()` listener built here has no client `Endpoint` to
//! configure, only the accept side of the same connection.

use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::proto;
use thiserror::Error;
use tokio::net::UnixListener;

/// Promoted to the wire crate in 039 (its doc rides with it — both
/// sides of the wire import the ONE constant now, so the ceiling can
/// never skew between a server and the client that dials it);
/// re-imported here so serve call sites keep reading
/// `common::MAX_FRAME_BYTES` unchanged.
pub(super) use rdlt_connector_protocol::MAX_FRAME_BYTES;

/// Failures standing up or running a `serve()` listener itself — never a
/// connector's own classified failure, which rides [`proto::ErrorFrame`]
/// over the wire instead of ending the process.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServeError {
    /// Creating the private per-process socket directory failed — before
    /// any bind was even attempted.
    #[error("creating the private socket directory at {path}: {source}")]
    SocketDir {
        /// The directory whose creation failed.
        path: PathBuf,
        /// The underlying IO failure.
        #[source]
        source: io::Error,
    },
    /// Binding the Unix domain socket failed (a path collision with a
    /// live listener, a parent directory that does not exist, a
    /// permissions problem).
    #[error("binding the connector socket at {path}: {source}")]
    Bind {
        /// Where the bind was attempted.
        path: PathBuf,
        /// The underlying IO failure.
        #[source]
        source: io::Error,
    },
    /// Restricting the freshly bound socket to its owner failed.
    #[error("setting socket permissions at {path}: {source}")]
    Permissions {
        /// Where the bind was attempted.
        path: PathBuf,
        /// The underlying IO failure.
        #[source]
        source: io::Error,
    },
    /// Writing the handshake line to stdout failed — the one thing a
    /// spawning host is waiting to read.
    #[error("writing the handshake line to stdout: {0}")]
    Stdout(#[source] io::Error),
    /// The gRPC server exited with a transport-level error.
    #[error("connector server: {0}")]
    Serve(#[source] tonic::transport::Error),
    /// The server task itself panicked or was cancelled.
    #[error("connector server task did not complete: {0}")]
    Join(#[source] tokio::task::JoinError),
}

/// A fresh socket path inside a PRIVATE per-process directory under the
/// system temp directory — what [`crate::serve::source::source`] and
/// [`crate::serve::destination::destination`] bind to when the caller
/// (a spawned connector process) has no path of its own to offer.
///
/// A private directory, not a bare pid-derived name in the shared
/// world-writable temp dir: a predictable name there could be
/// pre-created by another local user (the bind fails — DoS) or planted
/// as a dangling symlink, which `bind(2)` follows during path
/// resolution, landing the socket at an attacker-chosen location. The
/// directory is created mode `0700` with an unpredictable name (a
/// random 64-bit token seeded from OS entropy via
/// [`std::collections::hash_map::RandomState`]) BEFORE the socket
/// binds inside it, so both the bind and `bind_uds`'s chmod-to-`0600`
/// run in space nothing else can reach. Mirrors the pyjsonl
/// connector's mkdtemp shape (commit 939a1461).
///
/// A `serve()` process dies by SIGKILL (the provider's lifecycle), so
/// nothing here can reclaim its OWN directory on exit — instead each
/// startup sweeps dead predecessors' directories
/// ([`sweep_dead_serve_dirs`]) before minting its own.
pub fn temp_socket_path() -> Result<PathBuf, ServeError> {
    let base = std::env::temp_dir();
    sweep_dead_serve_dirs(&base);
    Ok(create_private_dir(&base)?.join("connector.sock"))
}

/// Reclaim dead predecessors' socket directories — rmdir-only over the
/// `rdlt-serve-*` entries of `base`, every failure ignored.
///
/// rmdir-only is the WHOLE liveness check, and it is sufficient and
/// race-free: every consumer of the handshake line (the runtime's
/// `LifecycleGuard`, the certifier's `SpawnedConnector` and its probe)
/// unlinks the socket FILE on every cleanup path, so a dead
/// predecessor's directory is EMPTY and rmdir succeeds exactly then; a
/// LIVE sibling's directory still holds its socket, so rmdir fails
/// harmlessly; rmdir never follows symlinks; and other users'
/// directories fail on permissions. Reclaim is best-effort hygiene,
/// never a gate — which is why errors (including reading `base` at
/// all) are ignored wholesale.
///
/// Known and accepted (the pyjsonl fix accepted the same): between a
/// concurrent sibling's mkdir and its bind, its directory is briefly
/// empty and a sweep here could rmdir it — that sibling's bind then
/// fails loudly with a `ServeError::Bind` (never silently), and the
/// window is a few syscalls wide.
fn sweep_dead_serve_dirs(base: &Path) {
    let Ok(entries) = std::fs::read_dir(base) else {
        return;
    };
    for entry in entries.flatten() {
        if entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(b"rdlt-serve-")
        {
            let _ = std::fs::remove_dir(entry.path());
        }
    }
}

/// Create `base/rdlt-serve-<random>` mode `0700`, retrying on a name
/// collision with a fresh token — the std-only shape of `mkdtemp`
/// (the sdk's runtime tree deliberately carries no tempfile
/// dependency, and delete-on-drop would be useless anyway: `serve()`
/// dies by SIGKILL, never by unwinding).
fn create_private_dir(base: &Path) -> Result<PathBuf, ServeError> {
    use std::hash::{BuildHasher, Hasher};
    use std::os::unix::fs::DirBuilderExt;

    let mut last_error: Option<(PathBuf, io::Error)> = None;
    for _ in 0..16 {
        // Each `RandomState::new()` is keyed from OS entropy, so the
        // finished hash of an empty input is a fresh unpredictable
        // 64-bit token — std's only randomness source, and enough that
        // another local user cannot pre-guess the name.
        let token = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        let path = base.join(format!("rdlt-serve-{token:016x}"));
        let mut builder = std::fs::DirBuilder::new();
        // Mode at mkdir time — atomic, no chmod-after-create window. A
        // umask can only CLEAR bits from 0o700, never widen them, so
        // the normalization below fixes an over-strict umask without
        // any exposure in between.
        builder.mode(0o700);
        match builder.create(&path) {
            Ok(()) => {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).map_err(
                    |source| ServeError::SocketDir {
                        path: path.clone(),
                        source,
                    },
                )?;
                return Ok(path);
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                // A 1-in-2^64 token collision (or an attacker squatting
                // one guessed name) — mint a fresh token and try again.
                last_error = Some((path, source));
            }
            Err(source) => return Err(ServeError::SocketDir { path, source }),
        }
    }
    let (path, source) = last_error.expect("the retry loop ran at least once");
    Err(ServeError::SocketDir { path, source })
}

/// Bind a Unix domain socket at `path`, then restrict it to the owner
/// (mode `0600`).
///
/// A spawned connector process inherits its operator's trust like any
/// child process (the proto file's "Trust model" note) — the socket
/// still shouldn't be group/world-writable by construction, since
/// nothing else establishes that.
///
/// `UnixListener::bind` refuses `AddrInUse` against an existing path
/// unconditionally — that errno alone cannot tell a real collision (a
/// LIVE listener already owns this path) from a stale file a same-PID
/// predecessor left behind (an unclean kill; PIDs recycle). Unlinking
/// unconditionally, as an earlier version of this function did, would
/// silently steal a live listener's path out from under it. So on
/// `AddrInUse` the path is probed instead: a connect attempt that
/// FAILS — refused, or any other connect error — means nothing usable
/// is listening (stale — unlink and retry); a connect that SUCCEEDS
/// means something is (a real collision, reported as
/// `ServeError::Bind` rather than clobbered).
///
/// The stale-probe → unlink → rebind sequence has a TOCTOU window:
/// between the failed probe-connect and the unlink, another process
/// could bind this same path — and the unlink would then steal its
/// live socket. Safe in practice, not by the syscalls: production
/// paths live in a private per-process directory with an unpredictable
/// name ([`temp_socket_path`]) and the process model is
/// single-spawn-per-connector, so no second binder ever targets the
/// same path inside that window.
pub fn bind_uds(path: &Path) -> Result<UnixListener, ServeError> {
    match UnixListener::bind(path) {
        Ok(listener) => finish_bind(path, listener),
        Err(source) if source.kind() == io::ErrorKind::AddrInUse => {
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                return Err(ServeError::Bind {
                    path: path.to_path_buf(),
                    source,
                });
            }
            // TOCTOU: between the failed probe-connect above and this
            // unlink, another process *could* bind this same path and have
            // its live socket removed — safe in practice because
            // production paths sit in a private 0700 per-process directory
            // (so single-spawner-per-path), and the provider owns the
            // spawn lifecycle.
            std::fs::remove_file(path).map_err(|source| ServeError::Bind {
                path: path.to_path_buf(),
                source,
            })?;
            let listener = UnixListener::bind(path).map_err(|source| ServeError::Bind {
                path: path.to_path_buf(),
                source,
            })?;
            finish_bind(path, listener)
        }
        Err(source) => Err(ServeError::Bind {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn finish_bind(path: &Path, listener: UnixListener) -> Result<UnixListener, ServeError> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|source| {
        ServeError::Permissions {
            path: path.to_path_buf(),
            source,
        }
    })?;
    Ok(listener)
}

/// Build one [`proto::ErrorFrame`] — every RPC error path (a handshake
/// refusal, a classified `SourceError`/`DestinationError`) converges
/// here rather than constructing the message by hand at each site.
pub(crate) fn error_frame(
    classification: proto::Classification,
    message: impl Into<String>,
    retry_after: Option<Duration>,
) -> proto::ErrorFrame {
    proto::ErrorFrame {
        classification: classification as i32,
        message: message.into(),
        retry_after_ms: retry_after.map(|duration| duration.as_millis() as u64),
    }
}

/// The frozen pre-handshake refusal: every RPC but `Handshake` itself
/// and the config-free `Spec` RPC (which answers before the handshake —
/// it carries no session state, so it never reaches a `shell()`
/// accessor for this refusal to fire from), on EITHER service, answers
/// this as a raw `Status::failed_precondition` until a handshake
/// populates the shell. Quoted verbatim in the protocol crate's README
/// — hoisted here so the spelling lives once, used by both servers'
/// `shell()` accessors.
pub(super) const HANDSHAKE_NOT_COMPLETED: &str = "handshake has not completed";

/// Every role the protocol currently defines — used only to tell "asked
/// for the OTHER recognized role" (a real mismatch, worded around what
/// this connector actually is) apart from "asked for a role that isn't
/// a role at all" (a typo or a version skew, worded around the request
/// itself instead).
pub(crate) const KNOWN_ROLES: [&str; 2] = ["source", "destination"];

/// Answer the config-free `Spec` RPC: the connector's static identity
/// (`C::NAME`/`C::VERSION`/`C::config_schema()`) serialized as
/// `ConnectorSpec` JSON — both servers' `spec` handlers converge here.
/// Answerable BEFORE any handshake by construction: it reads statics
/// alone and never touches a handshake-populated shell, so there is no
/// session state for the pre-handshake refusal to guard (039: a
/// provider asks a spawned connector what it IS — the schema command's
/// path — before deciding what config to hand it).
pub(crate) fn spec_reply(
    name: &'static str,
    version: &'static str,
    config_schema: Option<serde_json::Value>,
) -> tonic::Response<proto::SpecReply> {
    let mut spec = rdlt_connector::ConnectorSpec::new(name, version);
    spec.config_schema = config_schema;
    tonic::Response::new(proto::SpecReply {
        spec_json: serde_json::to_vec(&spec)
            .expect("a ConnectorSpec serializes to JSON infallibly"),
    })
}

/// Refuse a handshake with a FATAL [`proto::ErrorFrame`] carrying
/// `message` — every handshake refusal, from either service, converges
/// here.
pub(crate) fn refuse_handshake(
    message: impl Into<String>,
) -> tonic::Response<proto::HandshakeReply> {
    tonic::Response::new(proto::HandshakeReply {
        outcome: Some(proto::handshake_reply::Outcome::Error(error_frame(
            proto::Classification::Fatal,
            message,
            None,
        ))),
    })
}

/// What [`handshake`] needs from a `serve()` shell to run the
/// choreography once for either role — implemented for
/// `crate::source::Shell<C>` and `crate::destination::Shell<C>` in
/// their own modules (this trait lives here, not in either, so the ONE
/// function below can drive both without either shell type depending on
/// the other's module).
pub(crate) trait HandshakeShell: Sized {
    /// The document gate's own error type — its `Display` is what a
    /// refused handshake carries verbatim (the connector's own wording,
    /// never text this crate invents).
    type Error: std::fmt::Display;

    /// Validate-then-build from an already-parsed config document — the
    /// handshake-config entry point every shell's `from_value` already
    /// is.
    fn from_config(value: serde_json::Value) -> Result<Self, Self::Error>;
    /// `C::NAME` — the connector's stable identifier.
    fn connector_id(&self) -> &'static str;
    /// `C::VERSION`.
    fn connector_version(&self) -> &'static str;
    /// The shell's `ConnectorSpec`, pre-serialized.
    fn spec_json(&self) -> Vec<u8>;
    /// Destination capabilities, pre-serialized — empty for a source
    /// (the proto field's own doc: "DestinationCapabilities; empty for
    /// sources").
    fn capabilities_json(&self) -> Vec<u8>;
}

/// The handshake choreography shared by `serve::source` and
/// `serve::destination`: role check, protocol-version check, config
/// parse, shell construction, spec/capabilities serialization, and the
/// `OnceLock` race — this used to be ~60 near-identical lines written
/// out in each service; the ONE thing that genuinely differs between
/// them (the refusal's OTHER-role name) is derived from `this_role` +
/// [`KNOWN_ROLES`] rather than spelled out per call site, so the frozen
/// refusal spellings now live exactly once.
pub(crate) fn handshake<S: HandshakeShell>(
    slot: &OnceLock<Arc<S>>,
    this_role: &'static str,
    request: proto::HandshakeRequest,
) -> tonic::Response<proto::HandshakeReply> {
    if slot.get().is_some() {
        return refuse_handshake("handshake already completed");
    }

    if request.expected_role != this_role {
        if !KNOWN_ROLES.contains(&request.expected_role.as_str()) {
            return refuse_handshake(format!(
                "the handshake asked for role `{}`, which this connector does not recognize",
                request.expected_role
            ));
        }
        let other_role = KNOWN_ROLES
            .iter()
            .find(|&&role| role != this_role)
            .expect("KNOWN_ROLES names exactly two roles");
        return refuse_handshake(format!(
            "this connector is a {this_role}; the handshake asked for a {other_role}"
        ));
    }

    // A range check in spirit — v0 supports exactly `PROTOCOL_VERSION`,
    // so `proto_min == proto_max == PROTOCOL_VERSION` and the range
    // collapses to one value. Written as `!=` (not `< min || > max`)
    // because `PROTOCOL_VERSION` is `u32`'s minimum and clippy correctly
    // flags the lower bound as vacuous; widen this back into an explicit
    // range comparison the day a second supported version exists.
    if request.protocol_version != PROTOCOL_VERSION {
        return refuse_handshake(format!(
            "protocol version {} is outside this connector's supported range [{PROTOCOL_VERSION}, {PROTOCOL_VERSION}]",
            request.protocol_version
        ));
    }

    let config: serde_json::Value = match serde_json::from_slice(&request.config_json) {
        Ok(config) => config,
        Err(error) => return refuse_handshake(format!("invalid config_json: {error}")),
    };

    let shell = match S::from_config(config) {
        Ok(shell) => shell,
        Err(error) => return refuse_handshake(error.to_string()),
    };

    let ok = proto::HandshakeOk {
        connector_id: shell.connector_id().to_string(),
        connector_version: shell.connector_version().to_string(),
        spec_json: shell.spec_json(),
        capabilities_json: shell.capabilities_json(),
        // v0 hole, not an oversight: nothing populates this yet because
        // with one format version per state kind there is nothing to
        // negotiate. 039's client threads the empty map through to
        // embedders unread; negotiation semantics are owned by the
        // feature that adds a second format version — recorded so the
        // gap keeps an owner rather than looking finished.
        state_format_versions: Default::default(),
    };

    if slot.set(Arc::new(shell)).is_err() {
        // Lost a race against a concurrent handshake on the same
        // session — the same refusal either way.
        return refuse_handshake("handshake already completed");
    }

    tonic::Response::new(proto::HandshakeReply {
        outcome: Some(proto::handshake_reply::Outcome::Ok(ok)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The private socket directory is owner-only from birth and its
    /// name is minted fresh per call — the two properties that close
    /// the predictable-path race (pre-create DoS, dangling-symlink
    /// redirect) at the source.
    #[test]
    fn the_private_socket_dir_is_owner_only_and_unpredictable() {
        let base = tempfile::tempdir().expect("tempdir");

        let first = create_private_dir(base.path()).expect("first private dir");
        let second = create_private_dir(base.path()).expect("second private dir");

        assert_ne!(first, second, "each call mints a fresh name");
        for dir in [&first, &second] {
            let name = dir
                .file_name()
                .and_then(|name| name.to_str())
                .expect("utf8 name");
            assert!(
                name.starts_with("rdlt-serve-"),
                "the sweep's prefix contract: got `{name}`"
            );
            let mode = std::fs::metadata(dir)
                .expect("dir metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700, "the directory is private to its owner");
        }
    }

    /// The startup sweep reclaims exactly the dead: an EMPTY
    /// `rdlt-serve-*` sibling (its socket unlinked by the consumer's
    /// cleanup) is removed; a sibling still holding its socket file
    /// survives, and so does anything outside the prefix.
    #[test]
    fn the_sweep_reclaims_empty_serve_dirs_and_nothing_else() {
        let base = tempfile::tempdir().expect("tempdir");
        let dead = base.path().join("rdlt-serve-dead");
        let live = base.path().join("rdlt-serve-live");
        let foreign = base.path().join("someone-elses-dir");
        for dir in [&dead, &live, &foreign] {
            std::fs::create_dir(dir).expect("fixture dir");
        }
        std::fs::write(live.join("connector.sock"), b"").expect("live marker");

        sweep_dead_serve_dirs(base.path());

        assert!(!dead.exists(), "the empty (dead) sibling is reclaimed");
        assert!(live.exists(), "a non-empty (live) sibling survives rmdir");
        assert!(
            foreign.exists(),
            "names outside rdlt-serve-* are never touched"
        );
    }

    /// A stale socket file at the same path does not block a fresh
    /// bind — the AddrInUse-on-existing-path behavior `bind_uds` exists
    /// to route around. `#[tokio::test]`, not `#[test]`: binding a
    /// `UnixListener` registers with tokio's reactor, which only exists
    /// inside a running runtime.
    #[tokio::test]
    async fn a_stale_socket_file_does_not_block_a_fresh_bind() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("stale.sock");
        std::fs::write(&path, b"not a socket").expect("write stale file");

        let listener = bind_uds(&path).expect("bind despite the stale file");
        drop(listener);

        let mode = std::fs::metadata(&path)
            .expect("socket metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the socket is owner-only");
    }

    /// A path a LIVE listener already owns is a real collision, not a
    /// stale file — the probe-connect must refuse to unlink it out from
    /// under the first listener.
    #[tokio::test]
    async fn a_live_listener_at_the_same_path_is_a_collision_not_a_steal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("live.sock");

        let first = bind_uds(&path).expect("first bind");

        let error = bind_uds(&path).expect_err("a second bind must refuse, not steal the path");
        assert!(
            matches!(error, ServeError::Bind { .. }),
            "expected a Bind refusal, got {error:?}"
        );

        // The first listener is still the one at `path` — proven by
        // successfully connecting to it, not just by the second bind's
        // refusal.
        drop(tokio::net::UnixStream::connect(&path).await.expect(
            "the first listener is still live and accepting after the second bind refused",
        ));
        drop(first);
    }
}

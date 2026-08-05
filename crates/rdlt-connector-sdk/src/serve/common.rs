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

/// Failures standing up or running a `serve()` listener itself — never a
/// connector's own classified failure, which rides [`proto::ErrorFrame`]
/// over the wire instead of ending the process.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServeError {
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

/// A fresh, process-unique socket path in the system temp directory —
/// what [`crate::serve::source::source`] binds to when the caller (a
/// spawned connector process) has no path of its own to offer.
pub fn temp_socket_path() -> PathBuf {
    std::env::temp_dir().join(format!("rdlt-{}.sock", std::process::id()))
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
/// `AddrInUse` the path is probed instead: a connect attempt that is
/// REFUSED means nothing is listening (stale — unlink and retry);
/// a connect that SUCCEEDS means something is (a real collision,
/// reported as `ServeError::Bind` rather than clobbered).
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

/// Every role the protocol currently defines — used only to tell "asked
/// for the OTHER recognized role" (a real mismatch, worded around what
/// this connector actually is) apart from "asked for a role that isn't
/// a role at all" (a typo or a version skew, worded around the request
/// itself instead).
pub(crate) const KNOWN_ROLES: [&str; 2] = ["source", "destination"];

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
        // nothing on either side negotiates a resume format version to
        // put in it. 039's adapter is where that negotiation is
        // designed; recorded here so the gap has an owner rather than
        // looking finished.
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

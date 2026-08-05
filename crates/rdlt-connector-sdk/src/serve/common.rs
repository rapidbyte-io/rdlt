//! Plumbing shared by every `serve()` service: standing up the Unix
//! domain socket the handshake line advertises, and the [`ErrorFrame`]
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
use std::time::Duration;

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

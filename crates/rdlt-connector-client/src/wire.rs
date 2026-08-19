//! The transport seat: dialing a served connector's Unix socket, the
//! capped service clients every RPC goes through, and the deadline
//! that bounds every wire await.

use std::path::Path;
use std::time::Duration;

use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::proto::connector_client::ConnectorClient;
use rdlt_connector_protocol::proto::destination_service_client::DestinationServiceClient;
use rdlt_connector_protocol::proto::source_service_client::SourceServiceClient;
use rdlt_connector_protocol::proto::{self, check_reply};
use tonic::transport::{Channel, Endpoint};

use crate::error;

/// The default deadline for any single wire await — the dial, the
/// handshake, one read frame's quiet interval, one reply.
///
/// The same ten seconds is pinned by the certifier's kill window and
/// the runtime's spawn line-timeout, so a dead or silent connector
/// fails typed within it — change one and the law fragments. Per-await,
/// never per-stream: every frame or reply that arrives restarts the
/// clock, so a slow-but-flowing stream of any total duration never
/// trips it while a stalled one always does.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(10);

/// Which wire await exceeded the deadline — carried by
/// [`error::Error::Timeout`] so an embedder can tell a connector that
/// never came up (dial, handshake) from one that went silent
/// mid-session (a read frame, a reply that never arrives).
///
/// `#[non_exhaustive]`: a future transport can add awaits of its own —
/// match with a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operation {
    /// Establishing the transport to the advertised socket — the
    /// connector accepted the connection but never completed the
    /// HTTP/2 setup.
    Dial,
    /// The handshake reply.
    Handshake,
    /// The next frame of a server-streamed read.
    ReadFrame,
    /// An RPC reply — a unary reply, or the next reply on an open
    /// destination session.
    Reply,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Operation::Dial => "transport setup",
            Operation::Handshake => "handshake reply",
            Operation::ReadFrame => "read frame",
            Operation::Reply => "reply",
        })
    }
}

/// Bound one wire await by the session's RPC deadline, elapsing into
/// the typed [`error::Error::Timeout`]. Every await in this crate that
/// waits on the connector goes through here — the deadline bounds the
/// QUIET interval of that one await, never a whole stream: each frame
/// or reply that arrives starts the next await's clock afresh, so a
/// slow-but-flowing connector never trips it while a silent one always
/// does.
pub(crate) async fn bounded<F: std::future::Future>(
    deadline: Duration,
    operation: Operation,
    future: F,
) -> Result<F::Output, error::Error> {
    tokio::time::timeout(deadline, future)
        .await
        .map_err(|_elapsed| error::Error::Timeout {
            operation,
            deadline,
        })
}

/// h2's workable window floor. The RFC default is 64 KiB; a window
/// below it stalls a stream on frames the peer legally sends, so a
/// tiny budget is floored here rather than handed to h2 as-is.
const MIN_WINDOW_BYTES: u64 = 64 * 1024;

/// Dial the Unix domain socket a served connector's handshake line
/// advertised, returning the one [`Channel`] every service client for
/// that connector shares.
///
/// The host's byte budget IS the pacing authority: both h2 windows are
/// set from `budget_bytes`, so a server can never hold more bytes
/// in flight than the host's own channel budget — left unset,
/// tonic's ~2 MiB default window would pace the wire instead of the
/// budget. The clamp floors tiny budgets at h2's workable minimum
/// (`MIN_WINDOW_BYTES`, 64 KiB) and caps at [`MAX_FRAME_BYTES`], the
/// wire's hard per-message ceiling.
///
/// The URI handed to `Endpoint` is a placeholder — every connection
/// goes to the UDS through the connector closure, the tonic-over-UDS
/// idiom: `tower::service_fn` supplies the connector,
/// `hyper_util::rt::TokioIo` adapts `tokio::net::UnixStream` to
/// hyper's IO traits.
///
/// `rpc_deadline` bounds the WHOLE dial — a connector that accepts the
/// socket connection but never completes the HTTP/2 setup elapses into
/// the typed [`error::Error::Timeout`] rather than hanging the host
/// (`Endpoint::connect_timeout` alone covers only the io connect, so
/// the outer deadline is what makes the bound whole). The same
/// deadline arms the channel's HTTP/2 keep-alive, so a transport whose
/// peer dies BETWEEN awaits errors out within roughly two deadlines
/// instead of lingering until the next RPC.
pub async fn dial(
    socket_path: &Path,
    budget_bytes: u64,
    rpc_deadline: Duration,
) -> Result<Channel, error::Error> {
    let window = budget_bytes.clamp(MIN_WINDOW_BYTES, MAX_FRAME_BYTES as u64) as u32;
    let path = socket_path.to_path_buf();
    let endpoint = Endpoint::try_from("http://[::1]:1")
        .expect("a static placeholder endpoint parses")
        .initial_stream_window_size(window)
        .initial_connection_window_size(window)
        .connect_timeout(rpc_deadline)
        .http2_keep_alive_interval(rpc_deadline)
        .keep_alive_timeout(rpc_deadline)
        .keep_alive_while_idle(true);
    let connecting =
        endpoint.connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let path = path.clone();
            async move {
                let io = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(io))
            }
        }));
    bounded(rpc_deadline, Operation::Dial, connecting)
        .await?
        .map_err(|source| error::Error::Dial {
            path: socket_path.to_path_buf(),
            source,
        })
}

/// The decode cap on `Connector`-service replies: the LEGAL maximum is
/// computable, so decoding admits no more. A `HandshakeOk` — the
/// largest reply the service defines — is two document-ceiling payloads
/// (`spec_json` + `capabilities_json`, 8 MiB each), two identifiers
/// (≤ 1 KiB each), and a ≤64-entry state-format map of ≤1 KiB keys
/// (~66 KiB): ≈ 16.1 MiB with envelope; `SpecReply` (one document) and
/// `CheckReply` are smaller by construction. 18 MiB refuses nothing an
/// honest server can send, while a hostile frame sized to the 64 MiB
/// wire cap — whose map/repeated fields prost would materialize at a
/// multiple of the wire bytes BEFORE any content gate runs — now
/// refuses at decode, cutting that amplification ~4×. The bulk
/// services stay at [`MAX_FRAME_BYTES`]: their legal replies genuinely
/// fill the frame.
const MAX_CONNECTOR_REPLY_BYTES: usize = 18 * 1024 * 1024;

/// A `Connector` service client with the decode cap installed — every
/// construction site in this crate goes through one of these three
/// helpers so a decode ceiling can never be forgotten at one of them:
/// a client left at tonic's 4 MiB default dies on the first over-4 MiB
/// frame a server legally sends. This one caps at
/// [`MAX_CONNECTOR_REPLY_BYTES`] — the service's own legal maximum —
/// rather than the frame cap.
pub fn connector_client(channel: Channel) -> ConnectorClient<Channel> {
    ConnectorClient::new(channel).max_decoding_message_size(MAX_CONNECTOR_REPLY_BYTES)
}

/// A `SourceService` client with the decode cap installed — see
/// [`connector_client`].
pub fn source_client(channel: Channel) -> SourceServiceClient<Channel> {
    SourceServiceClient::new(channel).max_decoding_message_size(MAX_FRAME_BYTES)
}

/// A `DestinationService` client with the decode cap installed — see
/// [`connector_client`].
pub fn destination_client(channel: Channel) -> DestinationServiceClient<Channel> {
    DestinationServiceClient::new(channel).max_decoding_message_size(MAX_FRAME_BYTES)
}

/// The connector-level `Check` RPC — one implementation for both
/// adapter halves.
pub(crate) async fn check<E: crate::error::FromWire>(
    channel: &Channel,
    deadline: Duration,
) -> Result<(), E> {
    let mut client = connector_client(channel.clone());
    let reply = bounded(
        deadline,
        Operation::Reply,
        client.check(proto::CheckRequest {}),
    )
    .await
    .map_err(E::fatal_error)?
    .map_err(E::transport)?
    .into_inner();
    match reply.outcome {
        Some(check_reply::Outcome::Ok(_)) => Ok(()),
        Some(check_reply::Outcome::Error(frame)) => Err(E::from_frame(&frame)),
        None => Err(E::protocol(
            "the check reply carried no outcome".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A path nothing listens on refuses as `Dial`, carrying the path.
    #[tokio::test]
    async fn a_dead_socket_path_refuses_as_dial() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nothing-listens-here.sock");

        let error = dial(&path, 8 * 1024 * 1024, DEFAULT_DEADLINE)
            .await
            .expect_err("nothing listens — dial must refuse");
        match &error {
            error::Error::Dial { path: reported, .. } => assert_eq!(reported, &path),
            other => panic!("expected Dial, got {other:?}"),
        }
        assert!(
            error.to_string().contains("nothing-listens-here.sock"),
            "the rendered error names the socket: {error}"
        );
    }
}

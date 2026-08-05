//! Dialing a served connector over its Unix domain socket, and the
//! client-constructor helpers every generated service client in this
//! crate is built through.

use std::path::Path;

use rdlt_connector_protocol::MAX_FRAME_BYTES;
use rdlt_connector_protocol::proto::connector_client::ConnectorClient;
use rdlt_connector_protocol::proto::destination_service_client::DestinationServiceClient;
use rdlt_connector_protocol::proto::source_service_client::SourceServiceClient;
use tonic::transport::{Channel, Endpoint};

use crate::error::ClientError;

/// h2's workable window floor. The RFC default is 64 KiB; a window
/// below it stalls a stream on frames the peer legally sends, so a
/// tiny engine budget is floored here rather than handed to h2 as-is.
const MIN_WINDOW_BYTES: u64 = 64 * 1024;

/// Dial the Unix domain socket a served connector's handshake line
/// advertised, returning the one [`Channel`] every service client for
/// that connector shares.
///
/// The engine budget IS the pacing authority (ADR 0001 D6: `Read`
/// rides h2 flow control, no explicit credit message): both h2 windows
/// are set from `engine_budget_bytes`, so a server can never hold more
/// bytes in flight than the engine's own channel budget — the clamp
/// floors tiny budgets at h2's workable minimum (`MIN_WINDOW_BYTES`,
/// 64 KiB) and caps at [`MAX_FRAME_BYTES`], the wire's hard per-message
/// ceiling. Left unset, tonic's ~2 MiB default window would pace the
/// wire instead of the budget (the research spike measured exactly
/// that skew).
///
/// The URI handed to `Endpoint` is a placeholder — every connection
/// goes to the UDS through the connector closure, the tonic-over-UDS
/// idiom: `tower::service_fn` supplies the connector,
/// `hyper_util::rt::TokioIo` adapts `tokio::net::UnixStream` to
/// hyper's IO traits.
pub async fn dial(socket_path: &Path, engine_budget_bytes: u64) -> Result<Channel, ClientError> {
    let window = engine_budget_bytes.clamp(MIN_WINDOW_BYTES, MAX_FRAME_BYTES as u64) as u32;
    let path = socket_path.to_path_buf();
    Endpoint::try_from("http://[::1]:1")
        .expect("a static placeholder endpoint parses")
        .initial_stream_window_size(window)
        .initial_connection_window_size(window)
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let path = path.clone();
            async move {
                let io = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(io))
            }
        }))
        .await
        .map_err(|source| ClientError::Dial {
            path: socket_path.to_path_buf(),
            source,
        })
}

/// A `Connector` service client with the decode cap installed — every
/// construction site in this crate goes through one of these three
/// helpers so [`MAX_FRAME_BYTES`] can never be forgotten at one of
/// them: a client left at tonic's 4 MiB default dies on the first
/// over-4 MiB frame a server legally sends.
pub fn connector_client(channel: Channel) -> ConnectorClient<Channel> {
    ConnectorClient::new(channel).max_decoding_message_size(MAX_FRAME_BYTES)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A path nothing listens on refuses as `Dial`, carrying the path.
    #[tokio::test]
    async fn a_dead_socket_path_refuses_as_dial() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nothing-listens-here.sock");

        let error = dial(&path, 8 * 1024 * 1024)
            .await
            .expect_err("nothing listens — dial must refuse");
        match &error {
            ClientError::Dial { path: reported, .. } => assert_eq!(reported, &path),
            other => panic!("expected Dial, got {other:?}"),
        }
        assert!(
            error.to_string().contains("nothing-listens-here.sock"),
            "the rendered error names the socket: {error}"
        );
    }
}

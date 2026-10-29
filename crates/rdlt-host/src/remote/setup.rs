//! A connection's setup: the HTTP/2 channel its control connection carries, with the protocol's
//! transport settings and pings, and clients over it with no handshake.

use std::sync::{Arc, Mutex};

use hyper_util::rt::TokioIo;
use rdlt_connector::ConnectorError;
use rdlt_wire::transport::Transport;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;

use super::checked::Checked;
use super::severed::Severed;
use super::{Carrier, Client, Options, exceeded, lost_because};

/// A client of the protocol over `io`, with no handshake yet, its data calls `Data` calls of the
/// connection: for clients that speak the protocol themselves, as a certification suite does.
///
/// It runs as a connection does, with `options`' limits and HTTP/2 pings.
///
/// # Errors
///
/// A transient error when the transport fails, coded `deadline_exceeded` when the connection
/// is not set up within the connect deadline.
pub async fn client<IO>(io: IO, options: Options) -> Result<Client, ConnectorError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let channel = raw_channel(io, &options).await?;
    let carrier = Carrier::Called(channel.channel());
    Ok(Client::new(&channel, &options.limits, carrier))
}

/// A client of the protocol through `opener`, with no handshake yet: its control connection the
/// first stream the opener opens, each data call a stream of its own, as [`client`] is otherwise.
///
/// # Errors
///
/// As [`client`], and a transient error coded `connector_lost` where the control connection
/// cannot be opened.
pub async fn client_opened(
    opener: Arc<dyn crate::Opener>,
    options: Options,
) -> Result<Client, ConnectorError> {
    let deadline = options.deadlines.connect;
    let io = tokio::time::timeout(deadline, opener.open(crate::Opening::Control))
        .await
        .map_err(|_| exceeded("opening the control connection", deadline))?
        .map_err(|error| lost_because(format!("opening the control connection failed: {error}")))?;
    let channel = raw_channel(io, &options).await?;
    Ok(Client::new(
        &channel,
        &options.limits,
        Carrier::Opened(opener),
    ))
}

/// A channel over `io` with no handshake, set up within the connect deadline.
async fn raw_channel<IO>(io: IO, options: &Options) -> Result<Checked, ConnectorError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (cut, spent) = (CancellationToken::new(), CancellationToken::new());
    let deadline = options.deadlines.connect;
    let setup = channel(io, options, cut, spent);
    tokio::time::timeout(deadline, setup)
        .await
        .map_err(|_| exceeded("setting up the connection", deadline))?
}

/// A channel over `io`, which fails once `cut` is cancelled, and whose one connection, once
/// closed, cancels `spent`.
pub(super) async fn channel<IO>(
    io: IO,
    options: &Options,
    cut: CancellationToken,
    spent: CancellationToken,
) -> Result<Checked, ConnectorError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let slot = Arc::new(Mutex::new(Some(Severed::new(io, cut))));
    let connector = tower::service_fn(move |_| {
        let io = slot.lock().map(|mut slot| slot.take()).ok().flatten();
        // The channel reconnects once its one connection has closed: the connection is spent.
        if io.is_none() {
            spent.cancel();
        }
        async move {
            io.map(TokioIo::new).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "the connection is spent")
            })
        }
    });
    // HTTP/2's pings end a connection the network dropped silently. They wait twice the
    // heartbeat's patience: after a stall they check their deadline before reading the pong
    // waiting, where the heartbeat reads its echoes first, so the heartbeat judges a connector.
    let patience = options
        .heartbeat
        .saturating_mul(options.missed.get().saturating_mul(2));
    let transport = Transport::default();
    let channel = Endpoint::from_static("http://connector")
        .initial_stream_window_size(transport.stream_window)
        .initial_connection_window_size(transport.connection_window)
        .max_frame_size(transport.max_frame)
        .http2_max_header_list_size(rdlt_wire::limits::HEADER_LIST_BYTES)
        .http2_keep_alive_interval(options.heartbeat)
        .keep_alive_timeout(patience)
        .keep_alive_while_idle(true)
        .connect_with_connector(connector)
        .await
        .map_err(|error| lost_because(format!("connecting failed: {error}")))?;
    Ok(Checked::new(channel, options.limits))
}

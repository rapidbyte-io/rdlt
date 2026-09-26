//! A connection to a served connector: the handshake that connects it, a heartbeat that notices
//! when it stops answering, and a deadline for each call.

mod destination;
mod read;
mod source;
mod write;

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use rdlt_connector::wire::{error as status_error, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind, Role};
use rdlt_wire::v1::connector_client::ConnectorClient;
use rdlt_wire::{Limits, PROTOCOL_MAJOR, PROTOCOL_MINOR};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

pub use destination::RemoteDestination;
pub use source::RemoteSource;

/// The code of the error a call fails with once the connector stops answering heartbeats.
pub const CONNECTOR_LOST: &str = "connector_lost";

/// The code of the error a call fails with once it takes longer than its deadline.
pub const DEADLINE_EXCEEDED: &str = "deadline_exceeded";

/// How long each kind of call may take (§12.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deadlines {
    /// The handshake.
    pub connect: Duration,
    /// A check.
    pub check: Duration,
    /// A discovery.
    pub discover: Duration,
    /// Planning a stream's partitions.
    pub plan: Duration,
    /// Opening a session.
    pub open: Duration,
    /// Applying a schema change.
    pub apply_schema: Duration,
    /// Each answer to a write: credit, or a flush's stats.
    pub write_ack: Duration,
    /// A commit.
    pub commit: Duration,
    /// Closing a session.
    pub close: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        let minutes = |minutes: u64| Duration::from_secs(60 * minutes);
        Self {
            connect: minutes(1),
            check: minutes(5),
            discover: minutes(10),
            plan: minutes(10),
            open: minutes(5),
            apply_schema: minutes(30),
            write_ack: minutes(10),
            commit: minutes(30),
            close: minutes(5),
        }
    }
}

/// How a connection runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Options {
    /// How often a heartbeat is sent.
    pub heartbeat: Duration,
    /// How many heartbeats may go unanswered before the connector counts as lost.
    pub missed: u32,
    /// Each call's deadline.
    pub deadlines: Deadlines,
    /// The limits this end enforces on what it receives.
    pub limits: Limits,
    /// The bytes a read may send ahead of what the engine has taken.
    pub read_window: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_secs(5),
            missed: 6,
            deadlines: Deadlines::default(),
            limits: Limits::default(),
            read_window: rdlt_wire::limits::CREDIT_WINDOW,
        }
    }
}

/// A handshaken connection to a served connector.
#[derive(Debug)]
pub struct Connection {
    client: ConnectorClient<Channel>,
    spec: v1::ConnectorSpec,
    options: Options,
    lost: CancellationToken,
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Stops the heartbeat.
        self.lost.cancel();
    }
}

impl Connection {
    /// Connects over `io` to a connector served on its other end, as `role`, with `config`.
    ///
    /// # Errors
    ///
    /// The connector's error when the handshake fails, or a transient error when the transport
    /// does.
    pub async fn connect<IO>(
        io: IO,
        role: Role,
        config: &serde_json::Value,
        options: Options,
    ) -> Result<Arc<Self>, ConnectorError>
    where
        IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let slot = Arc::new(Mutex::new(Some(io)));
        let connector = tower::service_fn(move |_| {
            let io = slot.lock().map(|mut slot| slot.take()).ok().flatten();
            async move {
                io.map(TokioIo::new).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotConnected, "the connection is spent")
                })
            }
        });
        let channel = Endpoint::from_static("http://connector")
            .connect_with_connector(connector)
            .await
            .map_err(|error| lost(format!("connecting failed: {error}")))?;
        let bytes = options.limits.message_bytes();
        let mut client = ConnectorClient::new(channel)
            .max_decoding_message_size(bytes)
            .max_encoding_message_size(bytes);
        let request = v1::HandshakeRequest {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            features: Vec::new(),
            role: match role {
                Role::Source => v1::Role::Source,
                Role::Destination => v1::Role::Destination,
            } as i32,
            config_json: config.to_string(),
            traceparent: String::new(),
            limits: Some(options.limits.into()),
        };
        let deadline = options.deadlines.connect;
        let response = within(deadline, "the handshake", client.handshake(request)).await?;
        let lost = CancellationToken::new();
        let connection = Arc::new(Self {
            client,
            spec: response.spec.unwrap_or_default(),
            options,
            lost: lost.clone(),
        });
        tokio::spawn(heartbeat(connection.client.clone(), options, lost));
        Ok(connection)
    }

    /// The connector's spec, as its handshake answered.
    pub fn spec(&self) -> &v1::ConnectorSpec {
        &self.spec
    }

    /// Runs `call`, a call of the protocol named `what`, within `deadline`, failing once the
    /// connector is lost.
    async fn call<T>(
        &self,
        deadline: Duration,
        what: &str,
        call: impl Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    ) -> Result<T, ConnectorError> {
        tokio::select! {
            biased;
            () = self.lost.cancelled() => Err(lost_error()),
            answer = within(deadline, what, call) => answer,
        }
    }
}

/// The error of a call after the connector was lost.
fn lost_error() -> ConnectorError {
    lost("the connector stopped answering heartbeats".to_owned())
}

fn lost(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Transient, message).with_code(CONNECTOR_LOST)
}

/// Runs `call` within `deadline`: its answer, or the error it or the deadline failed with.
async fn within<T>(
    deadline: Duration,
    what: &str,
    call: impl Future<Output = Result<tonic::Response<T>, tonic::Status>>,
) -> Result<T, ConnectorError> {
    match tokio::time::timeout(deadline, call).await {
        Ok(Ok(response)) => Ok(response.into_inner()),
        Ok(Err(status)) => Err(status_error(&status)),
        Err(_) => Err(ConnectorError::new(
            ConnectorErrorKind::Transient,
            format!("{what} took longer than its deadline of {deadline:?}"),
        )
        .with_code(DEADLINE_EXCEEDED)),
    }
}

/// Sends a heartbeat every interval, and cancels `lost` once `missed` sent are unanswered when the
/// next is due, or the heartbeat stream fails.
async fn heartbeat(
    mut client: ConnectorClient<Channel>,
    options: Options,
    lost: CancellationToken,
) {
    let (beats, receiver) = mpsc::channel(4);
    let echoes = tokio::select! {
        biased;
        () = lost.cancelled() => return,
        echoes = client.heartbeat(ReceiverStream::new(receiver)) => echoes,
    };
    let Ok(echoes) = echoes else {
        lost.cancel();
        return;
    };
    let mut echoes = echoes.into_inner();
    let mut ticks = tokio::time::interval(options.heartbeat);
    let (mut sent, mut answered) = (0_u64, 0_u64);
    loop {
        tokio::select! {
            biased;
            () = lost.cancelled() => return,
            echo = echoes.message() => match echo {
                Ok(Some(echo)) => answered = answered.max(echo.seq),
                Ok(None) | Err(_) => break,
            },
            _ = ticks.tick() => {
                if sent - answered >= u64::from(options.missed) {
                    break;
                }
                sent += 1;
                if beats.send(v1::Ping { seq: sent }).await.is_err() {
                    break;
                }
            }
        }
    }
    lost.cancel();
}

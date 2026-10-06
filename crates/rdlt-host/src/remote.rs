//! A connection to a served connector: the handshake that connects it, a heartbeat that notices
//! when it stops answering, and a deadline for each call.

mod agreed;
mod checked;
mod clients;
mod destination;
mod read;
pub(crate) mod severed;
mod source;
mod write;

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use rdlt_connector::wire::{Invalid, error as status_error, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind, Role};
use rdlt_wire::bounded::Charged;
use rdlt_wire::{Limits, PROTOCOL_MAJOR, PROTOCOL_MINOR};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;

pub use clients::Client;
use clients::Clients;
pub use destination::RemoteDestination;
pub use read::MAX_FREE_FRAMES;
pub use source::RemoteSource;

/// The code of the error a call fails with once the connector stops answering heartbeats.
pub const CONNECTOR_LOST: &str = "connector_lost";

pub use rdlt_connector::DEADLINE_EXCEEDED;

/// How long each kind of call may take.
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
    /// A write's wait for credit to send a frame, or a flush's wait for its stats, however
    /// many answers come meanwhile.
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
    /// How often a heartbeat is sent; above zero, as [`Connection::connect`] requires.
    pub heartbeat: Duration,
    /// How many heartbeats may go unanswered before the connector counts as lost.
    pub missed: NonZeroU32,
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
            missed: NonZeroU32::new(6).unwrap_or(NonZeroU32::MIN),
            deadlines: Deadlines::default(),
            limits: Limits::default(),
            read_window: rdlt_wire::limits::CREDIT_WINDOW,
        }
    }
}

/// A handshaken connection to a served connector.
#[derive(Debug)]
pub struct Connection {
    client: Clients,
    spec: v1::ConnectorSpec,
    /// The limits the connector enforces on what it receives.
    peer: Limits,
    options: Options,
    /// Cancelled once the connector is lost: every call on the connection fails.
    lost: CancellationToken,
    /// Cancelled once the connection takes no new calls: the connector was lost, or is stopping
    /// and finishes the calls in flight.
    spent: CancellationToken,
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Stops the heartbeat.
        self.lost.cancel();
    }
}

impl Connection {
    /// Connects over `io` to a connector served on its other end, as `role`, with `config`,
    /// whatever connector it is: [`Connection::handshake`] checks who it is first.
    ///
    /// # Errors
    ///
    /// As [`Connection::handshake`] and [`Handshaken::configure`] fail.
    pub async fn connect<IO>(
        io: IO,
        role: Role,
        config: &serde_json::Value,
        options: Options,
    ) -> Result<Arc<Self>, ConnectorError>
    where
        IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::handshake(io, role, options)
            .await?
            .configure(config)
            .await
    }

    /// Handshakes over `io` with a connector served on its other end, as `role`: its answer says
    /// who the connector is, which the host checks before configuring it.
    ///
    /// # Errors
    ///
    /// A `Config` error coded `options_invalid` for a zero heartbeat interval, the connector's
    /// error when the handshake fails, or a transient error when the transport does.
    pub async fn handshake<IO>(
        io: IO,
        role: Role,
        options: Options,
    ) -> Result<Handshaken, ConnectorError>
    where
        IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        if options.heartbeat.is_zero() {
            return Err(ConnectorError::config("the heartbeat interval is zero")
                .with_code("options_invalid"));
        }
        let (lost, spent) = {
            let lost = CancellationToken::new();
            let spent = lost.child_token();
            (lost, spent)
        };
        let channel = channel(io, &options, lost.clone(), spent.clone()).await?;
        let client = Clients::new(&channel, &options.limits);
        let request = v1::HandshakeRequest {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            // A host offers no feature: certification's read-back is the only one.
            features: Vec::new(),
            role: match role {
                Role::Source => v1::Role::Source,
                Role::Destination => v1::Role::Destination,
            } as i32,
            traceparent: String::new(),
            limits: Some(options.limits.into()),
        };
        let deadline = options.deadlines.connect;
        let mut handshaking = client.handshake.clone();
        let handshaken = handshaking.handshake(request);
        let response = within(deadline, "the handshake", handshaken).await?;
        agreed::agreed(&response, &[])?;
        // A connector may not make this host send frames smaller than the protocol's least.
        let peer = response.limits.map(Limits::from).unwrap_or_default();
        peer.admit_peer()
            .map_err(|shortfall| rdlt_connector::wire::shortfall_error(&shortfall))?;
        Ok(Handshaken {
            client,
            spec: response.spec.unwrap_or_default(),
            peer,
            options,
            lost,
            spent,
        })
    }

    /// The contract's spec of the connector, in `role`, from what its handshake answered.
    ///
    /// # Errors
    ///
    /// An internal error when the connector's id or configuration schema is malformed.
    pub fn connector_spec(
        &self,
        role: Role,
    ) -> Result<rdlt_connector::ConnectorSpec, ConnectorError> {
        contract_spec(&self.spec, role)
    }

    /// The connector's spec, as its handshake answered.
    pub fn spec(&self) -> &v1::ConnectorSpec {
        &self.spec
    }

    /// Whether the connection takes no new calls: the connector was lost, or is stopping.
    pub(crate) fn is_spent(&self) -> bool {
        self.spent.is_cancelled()
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

/// A connection whose handshake agreed a role, before the connector is configured: its spec says
/// who the connector is, which the host checks before the connector sees any configuration.
///
/// Dropped unconfigured, it cuts the connection: its client is the connection's last.
#[derive(Debug)]
pub struct Handshaken {
    client: Clients,
    spec: v1::ConnectorSpec,
    peer: Limits,
    options: Options,
    lost: CancellationToken,
    spent: CancellationToken,
}

impl Handshaken {
    /// The connector's spec, as its handshake answered: who it is, without what its
    /// configuration decides.
    pub fn spec(&self) -> &v1::ConnectorSpec {
        &self.spec
    }

    /// Configures the connector with `config`, for the role the handshake agreed.
    ///
    /// # Errors
    ///
    /// The connector's error when its configuration fails, an internal error coded
    /// `invalid_message` when it answers as another connector than it handshook as, or a transient
    /// error when the transport fails.
    pub async fn configure(
        self,
        config: &serde_json::Value,
    ) -> Result<Arc<Connection>, ConnectorError> {
        self.configure_json(config.to_string()).await
    }

    /// Configures the connector with the JSON document `config_json`, as
    /// [`configure`](Self::configure) does with a value.
    ///
    /// # Errors
    ///
    /// As [`configure`](Self::configure) fails.
    pub async fn configure_json(
        self,
        config_json: String,
    ) -> Result<Arc<Connection>, ConnectorError> {
        let request = v1::ConfigureRequest { config_json };
        let deadline = self.options.deadlines.connect;
        // The configuration's answer carries a destination's identifier rules, which at their
        // limits outgrow any other control message: it is decoded as the handshake's is.
        let mut client = self.client.handshake.clone();
        let configured = tokio::select! {
            biased;
            () = self.lost.cancelled() => return Err(lost_error()),
            answer = within(deadline, "the configuration", client.configure(request)) => answer?,
        };
        let spec = configured.spec.unwrap_or_default();
        agreed::spec_within(&spec)?;
        if (spec.id.as_str(), spec.version.as_str())
            != (self.spec.id.as_str(), self.spec.version.as_str())
        {
            return Err(source::invalid(&Invalid::Rejected {
                what: "configured spec",
                source: format!(
                    "the connector handshook as `{}` {} and configured as `{}` {}",
                    self.spec.id, self.spec.version, spec.id, spec.version
                )
                .into(),
            }));
        }
        tokio::spawn(heartbeat(
            self.client.control.clone(),
            (self.options.heartbeat, self.options.missed),
            self.lost.clone(),
            self.spent.clone(),
        ));
        Ok(Arc::new(Connection {
            client: self.client.clone(),
            spec,
            peer: self.peer,
            options: self.options,
            lost: self.lost.clone(),
            spent: self.spent.clone(),
        }))
    }
}

/// A client of the protocol over `io`, with no handshake yet: for clients that speak the protocol
/// themselves, as a certification suite does.
///
/// It runs as a connection does, with `options`' limits and HTTP/2 pings.
///
/// # Errors
///
/// A transient error when the transport fails.
pub async fn client<IO>(io: IO, options: Options) -> Result<Client, ConnectorError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (cut, spent) = (CancellationToken::new(), CancellationToken::new());
    let channel = channel(io, &options, cut, spent).await?;
    Ok(clients::sized(
        &channel,
        &options.limits,
        rdlt_wire::limits::Class::Data,
    ))
}

/// A channel over `io`, which fails once `cut` is cancelled, and whose one connection, once
/// closed, cancels `spent`.
async fn channel<IO>(
    io: IO,
    options: &Options,
    cut: CancellationToken,
    spent: CancellationToken,
) -> Result<checked::Checked, ConnectorError>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let slot = Arc::new(Mutex::new(Some(severed::Severed::new(io, cut))));
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
    // HTTP/2's own pings notice a connection the network dropped silently, beside the
    // protocol's heartbeat, which notices a connector that stopped answering.
    let patience = options.heartbeat.saturating_mul(options.missed.get());
    let channel = Endpoint::from_static("http://connector")
        .initial_connection_window_size(rdlt_wire::limits::CONNECTION_WINDOW)
        .http2_max_header_list_size(rdlt_wire::limits::HEADER_LIST_BYTES)
        .http2_keep_alive_interval(options.heartbeat)
        .keep_alive_timeout(patience)
        .keep_alive_while_idle(true)
        .connect_with_connector(connector)
        .await
        .map_err(|error| lost_because(format!("connecting failed: {error}")))?;
    Ok(checked::Checked::new(channel, options.limits))
}

/// The contract's spec of the connector the handshake's `spec` describes, in `role`.
pub(crate) fn contract_spec(
    spec: &v1::ConnectorSpec,
    role: Role,
) -> Result<rdlt_connector::ConnectorSpec, ConnectorError> {
    use rdlt_connector::wire::Invalid;
    let id = rdlt_connector::ConnectorId::parse(&spec.id)
        .map_err(|error| source::invalid(&Invalid::rejected("connector id", error)))?;
    let config_schema = serde_json::from_str(&spec.config_schema_json)
        .map_err(|error| source::invalid(&Invalid::rejected("configuration schema", error)))?;
    Ok(rdlt_connector::ConnectorSpec {
        id,
        version: spec.version.clone(),
        role,
        config_schema,
    })
}

/// The error of a call after the connector was lost.
fn lost_error() -> ConnectorError {
    lost_because("the connector stopped answering heartbeats".to_owned())
}

fn lost_because(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Transient, message).with_code(CONNECTOR_LOST)
}

/// `response`, with the charge of what its body passed on last, which whoever reads its stream
/// releases once each message is decoded.
fn charged<T>(response: tonic::Response<T>) -> tonic::Response<(T, Charged)> {
    let charged = response.extensions().get::<Charged>().cloned();
    response.map(|messages| (messages, charged.unwrap_or_default()))
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

/// Sends a heartbeat `every` interval, and cancels `lost` once `missed` sent are unanswered when the
/// next is due, or the heartbeat stream fails.
///
/// A connector that ends the stream is stopping: it finishes the calls in flight, and `retired` is
/// cancelled.
async fn heartbeat(
    mut client: Client,
    (every, missed): (Duration, NonZeroU32),
    lost: CancellationToken,
    retired: CancellationToken,
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
    let mut ticks = tokio::time::interval(every);
    // After this end stalls, the next heartbeat waits its interval: the connector gets its time.
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let (mut sent, mut answered) = (0_u64, 0_u64);
    loop {
        tokio::select! {
            biased;
            () = lost.cancelled() => return,
            echo = echoes.message() => match echo {
                // An echo of a heartbeat never sent answers nothing.
                Ok(Some(echo)) if echo.seq <= sent => answered = answered.max(echo.seq),
                Ok(Some(_)) => {}
                Ok(None) => {
                    retired.cancel();
                    return;
                }
                Err(_) => break,
            },
            _ = ticks.tick() => {
                if sent - answered >= u64::from(missed.get()) {
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

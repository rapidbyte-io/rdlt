//! Connectors that break the protocol to the engine's host, one fault to each port of a host of
//! their own; beside a seed's runs, the engine's host makes the call one's fault breaks, and
//! must meet it as the protocol says, or lose the connector, within a bound, leaving nothing
//! running.
//!
//! Where the network is disrupted a call may also be lost or cut, so an attack is judged in full
//! only on a healed network; that it ends within its bound is judged always.

mod attack;
#[cfg(test)]
mod tests;

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::Role;
use rdlt_connector::limits::ListenLimits;
use rdlt_connector::wire::{INVALID_MESSAGE, MALFORMED_FRAME, PROTOCOL_VERSION, TRANSPORT, v1};
use rdlt_host::{CONNECTOR_LOST, DEADLINE_EXCEEDED, MAX_FREE_FRAMES};
use rdlt_testkit::fake::{self, Fake, Fault};
use rdlt_wire::limits::{LIMIT_BELOW_MINIMUM, LIMIT_EXCEEDED};
use tokio::runtime::Handle;
use tokio_rustls::TlsAcceptor;

use super::Net;

/// The port of the catalogue's first broken connector; each next one listens on the next.
const FIRST_PORT: u16 = 7500;

/// The id every broken connector answers its handshake as.
pub(super) const ID: &str = "test.fake";

/// The call an attack makes on a broken connector once it is dialed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Call {
    /// The connection's setup, its handshake and configuration, as a source.
    Connect,
    /// A check, once the heartbeat has had twice its patience.
    Heartbeat,
    /// A read of one partition.
    Read,
    /// A read of one partition, a barrier raised before it starts.
    Barrier,
    /// A discovery.
    Discover,
    /// A session's opening, as a destination.
    Open,
    /// Four writes of a table and their flush, as a destination.
    Write,
}

impl Call {
    /// The role the connection is set up as.
    pub(super) fn role(self) -> Role {
        match self {
            Self::Open | Self::Write => Role::Destination,
            _ => Role::Source,
        }
    }
}

/// How the engine's host must meet a broken connector on a healed network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Expect {
    /// The call fails with this code.
    Code(&'static str),
    /// The call fails as an answer too large to decode.
    TooLarge,
    /// The read succeeds, having answered its barrier once.
    Answered,
}

/// A broken connector: what its fault does, the fault, the call that meets it, and how the
/// host must meet it.
#[derive(Clone, Copy, Debug)]
pub(super) struct Broken {
    pub(super) what: &'static str,
    pub(super) fault: Fault,
    pub(super) call: Call,
    pub(super) expect: Expect,
    /// Whether the host takes its answers within limits small enough that they pass them.
    pub(super) small: bool,
}

impl Broken {
    const fn new(what: &'static str, fault: Fault, call: Call, expect: Expect) -> Self {
        Self {
            what,
            fault,
            call,
            expect,
            small: false,
        }
    }

    /// This connector, taken within small limits.
    const fn small(self) -> Self {
        Self {
            small: true,
            ..self
        }
    }
}

/// Every broken connector, each at the port its index names, met as `rdlt-host`'s own tests
/// meet it.
#[expect(
    clippy::too_many_lines,
    reason = "a table, one row to a broken connector"
)]
pub(super) fn catalogue() -> [Broken; 20] {
    use Call::{Barrier, Connect, Discover, Heartbeat, Open, Read, Write};
    let invalid = Expect::Code(INVALID_MESSAGE);
    let lost = Expect::Code(CONNECTOR_LOST);
    let trickle = Duration::from_millis(100);
    [
        Broken::new(
            "configures as another connector",
            Fault::Impostor,
            Connect,
            invalid,
        ),
        Broken::new(
            "says no dictionary limit",
            Fault::NoDictionaryLimit,
            Connect,
            Expect::Code(LIMIT_BELOW_MINIMUM),
        ),
        Broken::new(
            "answers at the next major version",
            Fault::Handshakes(ahead),
            Connect,
            Expect::Code(PROTOCOL_VERSION),
        ),
        Broken::new(
            "accepts a feature never offered",
            Fault::Handshakes(unoffered),
            Connect,
            invalid,
        ),
        Broken::new("echoes pings never sent", Fault::EchoAhead, Heartbeat, lost),
        Broken::new("answers no ping", Fault::Silent, Heartbeat, lost),
        Broken::new(
            "never opens its heartbeat",
            Fault::Unopened,
            Heartbeat,
            lost,
        ),
        Broken::new(
            "refuses its heartbeat",
            Fault::RefusesHeartbeat,
            Heartbeat,
            lost,
        ),
        Broken::new("never reads its pings", Fault::Unread, Heartbeat, lost),
        Broken::new(
            "sends a schema that is no IPC message",
            Fault::GarbageSchema,
            Read,
            Expect::Code(MALFORMED_FRAME),
        ),
        Broken::new(
            "answers a barrier never asked for",
            Fault::AnswersAhead,
            Read,
            invalid,
        ),
        Broken::new(
            "sends a schema epoch twice",
            Fault::StaleEpoch,
            Read,
            Expect::Code(MALFORMED_FRAME),
        ),
        Broken::new(
            "sends schemas past its bound",
            Fault::Schemas(6),
            Read,
            invalid,
        )
        .small(),
        Broken::new(
            "sends lags past their bound",
            Fault::Lags(MAX_FREE_FRAMES + 1),
            Read,
            invalid,
        ),
        Broken::new(
            "knows no barrier in a read's start",
            Fault::Unstarted,
            Barrier,
            Expect::Answered,
        ),
        Broken::new(
            "fails with details that do not decode",
            Fault::GarbledDetails,
            Discover,
            Expect::Code(TRANSPORT),
        ),
        Broken::new(
            "answers a catalog too large to decode",
            Fault::Bloats(32 * 1024),
            Discover,
            Expect::TooLarge,
        )
        .small(),
        Broken::new(
            "opens with a state key past its limit",
            Fault::Keys(long_key),
            Open,
            Expect::Code(LIMIT_EXCEEDED),
        ),
        Broken::new(
            "grants a byte of credit at a time",
            Fault::Trickles(trickle, 1),
            Write,
            Expect::Code(DEADLINE_EXCEEDED),
        ),
        Broken::new(
            "grants credit of no bytes",
            Fault::Trickles(trickle, 0),
            Write,
            invalid,
        ),
    ]
}

/// A handshake answered at the next major version.
fn ahead(answer: &mut v1::HandshakeResponse) {
    answer.protocol_major += 1;
}

/// A handshake answered accepting a feature the host never offered.
fn unoffered(answer: &mut v1::HandshakeResponse) {
    answer.accepted_features = vec!["unoffered".to_owned()];
}

/// A state record's key a byte beyond the control string limit.
fn long_key() -> String {
    "k".repeat(64 * 1024 + 1)
}

/// The port the broken connector at `index` of the catalogue listens on.
pub(super) fn port(index: usize) -> u16 {
    FIRST_PORT + u16::try_from(index).expect("the catalogue fits the ports")
}

/// Serves every broken connector of the catalogue on its port of this host, over mutual TLS,
/// until the simulation ends.
pub(super) async fn host(net: Arc<Net>) -> turmoil::Result {
    net.served.lock().push(Handle::current());
    let (tls, _) = super::mutual(&net)?;
    let acceptor = TlsAcceptor::from(tls);
    let mut listening = Vec::new();
    for (index, broken) in catalogue().into_iter().enumerate() {
        let listener =
            turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, port(index))).await?;
        listening.push(accept(listener, acceptor.clone(), broken.fault));
    }
    futures_util::future::join_all(listening).await;
    Ok(())
}

/// Accepts connections on `listener`, and serves each the connector `fault` breaks.
async fn accept(listener: turmoil::net::TcpListener, acceptor: TlsAcceptor, fault: Fault) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            // The fake has no send limit of its own, and a host that vanished behind a cut
            // takes none of what is sent to it, so each connection lives at most a served
            // connector's send limit, longer than any attack's bound.
            let serving = async {
                if let Some(tls) = super::handshake(&acceptor, stream).await {
                    fake::serve(Fake(fault), tls).await;
                }
            };
            tokio::time::timeout(ListenLimits::default().send, serving)
                .await
                .ok();
        });
    }
}

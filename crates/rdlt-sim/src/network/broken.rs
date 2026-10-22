//! Connectors that break the protocol to the engine's host, one fault to each port of a host of
//! their own; beside a seed's runs, the engine's host makes the call one's fault breaks, and
//! must meet it as the protocol says, or lose the connector, within a bound, leaving nothing
//! running.
//!
//! Where the network is disrupted a call may also be lost or cut, so an attack is judged in full
//! only on a healed network; that it ends within its bound is judged always.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::net::Ipv4Addr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::wire::{MALFORMED_FRAME, TRANSPORT, v1};
use rdlt_connector::{
    ConnectorError, ConnectorId, Destination as _, LoadId, OpenContext, Partition, PipelineId,
    ReadRequest, Role, SchemaVersion, SegmentId, Source as _, SourceEvent, StreamName, TablePath,
    TableRef, partition_channel,
};
use rdlt_host::{
    CONNECTOR_LOST, Connection, ConnectorRef, DEADLINE_EXCEEDED, Deadlines, MAX_FREE_FRAMES,
    Options, RemoteDestination, RemoteSource, TLS, Wire,
};
use rdlt_testkit::fake::{self, Fake, Fault};
use rdlt_wire::Limits;
use rdlt_wire::limits::{LIMIT_BELOW_MINIMUM, LIMIT_EXCEEDED};
use tokio::runtime::Handle;
use tokio_rustls::TlsAcceptor;

use super::{BROKEN, Net, Networked, patience};
use crate::reached::Reach;
use crate::rng::SplitMix64;
use crate::world::World;

/// The port of the catalogue's first broken connector; each next one listens on the next.
const FIRST_PORT: u16 = 7500;

/// The id every broken connector answers its handshake as.
pub(super) const ID: &str = "test.fake";

/// The code of a message the protocol does not allow.
pub(super) const INVALID_MESSAGE: &str = "invalid_message";

/// The code of a handshake at a major version the host does not speak.
const PROTOCOL_VERSION: &str = "protocol_version";

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
            "sends log lines past their bound",
            Fault::Logs(MAX_FREE_FRAMES + 1),
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
            let tls = super::handshake(&acceptor, stream).await;
            if let Some(tls) = tls {
                fake::serve(Fake(fault), tls).await;
            }
        });
    }
}

/// Heartbeats an attack's connection may miss: more than its queue and a 16-byte window hold.
const MISSED: u32 = 16;

/// How long an attack may take beyond the waits its deadlines and heartbeat bound.
const LATE: Duration = Duration::from_secs(1);

/// The codes a call may fail with where the network was disrupted: the connector lost, its
/// transport failed, a deadline passed, or its TLS handshake failed.
const DISRUPTED: [&str; 4] = [CONNECTOR_LOST, TRANSPORT, DEADLINE_EXCEEDED, TLS];

/// How an attack ended.
#[derive(Debug)]
pub(super) enum Ended {
    /// Its call, or its connection's setup, was refused.
    Refused(ConnectorError),
    /// Its call was answered as a conforming connector's is.
    Answered,
    /// Its call was answered as no connector with its fault is: how.
    Wrong(String),
    /// The broken connector was never dialed: why.
    Undialed(String),
    /// It outlasted this bound.
    Outlasted(Duration),
}

/// What is wrong with an attack that ended as `ended`, where the host must meet its connector
/// as `expect` says; where not `strict`, the network was disrupted and it may have been lost.
pub(super) fn judge(expect: Expect, ended: &Ended, strict: bool) -> Option<String> {
    match ended {
        Ended::Outlasted(bound) => Some(format!("the call outlasted {bound:?}")),
        Ended::Wrong(how) => Some(how.clone()),
        Ended::Undialed(why) => {
            strict.then(|| format!("it was not dialed on a healed network: {why}"))
        }
        Ended::Answered => (expect != Expect::Answered)
            .then(|| format!("the call was answered, not met as {expect:?}")),
        Ended::Refused(error) => {
            let met = match expect {
                Expect::Code(code) => error.code() == Some(code),
                Expect::TooLarge => error.to_string().contains("too large"),
                Expect::Answered => false,
            };
            let lost = !strict && error.code().is_some_and(|code| DISRUPTED.contains(&code));
            (!met && !lost).then(|| format!("the call failed with {error}, not as {expect:?}"))
        }
    }
}

/// An attack on a broken connector: the placement it dials through, the world it records
/// findings in, and whether the network is healed, so the call must be met in full.
pub(super) struct Breaker<'a> {
    pub(super) placing: &'a Networked,
    pub(super) world: &'a World,
    pub(super) strict: bool,
}

impl Networked {
    /// Runs `work` while the engine's host makes the call a broken connector `rng` draws breaks,
    /// and records in `world` how it was met: in full where `strict`, the network healed.
    #[expect(dead_code, reason = "a run calls it beside its work, not yet wired")]
    pub(crate) async fn breaking<T>(
        &self,
        mut rng: SplitMix64,
        world: &World,
        strict: bool,
        work: impl Future<Output = T>,
    ) -> T {
        let count = u64::try_from(catalogue().len()).unwrap_or(1);
        let index = usize::try_from(rng.below(count)).unwrap_or(0);
        let breaker = Breaker {
            placing: self,
            world,
            strict,
        };
        let (done, ()) = tokio::join!(work, Box::pin(breaker.attack(index, rng)));
        done
    }
}

impl Breaker<'_> {
    /// Attacks the broken connector at `index`, its options drawn from `rng`, and judges how it
    /// ended.
    pub(super) async fn attack(&self, index: usize, mut rng: SplitMix64) {
        let broken = catalogue()[index];
        let connect = self.placing.remote_options.deadlines.connect;
        let options = attacking(&mut rng, connect, broken.small);
        let bound = bound(&options);
        let ended = tokio::time::timeout(bound, self.call(index, broken.call, &options))
            .await
            .unwrap_or(Ended::Outlasted(bound));
        match judge(broken.expect, &ended, self.strict) {
            Some(finding) => self.world.violation(format!(
                "a connector that {} ({:?}): {finding}",
                broken.what, broken.call
            )),
            None if self.strict => self.world.reach(Reach::Framing),
            None => {}
        }
    }

    /// Dials the broken connector at `index` and makes `call` on it with `options`.
    async fn call(&self, index: usize, call: Call, options: &Options) -> Ended {
        let id = ConnectorId::parse(ID).expect("the broken connectors' id is valid");
        let endpoint = format!("grpcs://{BROKEN}:{}", port(index));
        let reference = ConnectorRef::new(id).endpoint(endpoint);
        match self.placing.remote.wire(&reference).await {
            Ok(wire) => made(call, wire, options).await,
            Err(error) => Ended::Undialed(error.to_string()),
        }
    }
}

/// The options an attack's connection runs with, drawn from `rng`: heartbeats 20 to 100 ms
/// apart, [`MISSED`] of them missed, `connect` to set it up, writes waiting 300 ms to 3 s for
/// credit, and limits its answers pass where `small`.
fn attacking(rng: &mut SplitMix64, connect: Duration, small: bool) -> Options {
    let limits = if small {
        Limits {
            schema_columns: 4,
            catalog_bytes: 32 * 1024,
            state_bytes: 32 * 1024,
            ..Limits::default()
        }
    } else {
        Limits::default()
    };
    Options {
        heartbeat: Duration::from_millis(20 + rng.below(81)),
        missed: NonZeroU32::new(MISSED).unwrap_or(NonZeroU32::MIN),
        deadlines: Deadlines {
            connect,
            write_ack: Duration::from_millis(300 + rng.below(2701)),
            ..Deadlines::default()
        },
        limits,
        ..Options::default()
    }
}

/// How long an attack with `options` may take: its dial and its connection's setup, four of
/// its heartbeat's patience, a write's wait for credit, and [`LATE`].
fn bound(options: &Options) -> Duration {
    options.deadlines.connect.saturating_mul(2)
        + patience(options).saturating_mul(4)
        + options.deadlines.write_ack
        + LATE
}

/// Sets up a connection over `wire` as `call`'s role, with `options`, and makes `call` on it.
async fn made(call: Call, wire: Wire, options: &Options) -> Ended {
    let connected = Connection::connect(wire, call.role(), &serde_json::json!({}), *options).await;
    let connection = match connected {
        Ok(connection) => connection,
        Err(error) => return Ended::Refused(error),
    };
    match call {
        Call::Connect => Ended::Wrong("its connection was set up".to_owned()),
        Call::Heartbeat => {
            tokio::time::sleep(patience(options).saturating_mul(2)).await;
            ended(RemoteSource::new(connection).check().await)
        }
        Call::Read => read(connection, false).await,
        Call::Barrier => read(connection, true).await,
        Call::Discover => ended(RemoteSource::new(connection).discover().await.map(drop)),
        Call::Open => open(connection).await,
        Call::Write => write(connection).await,
    }
}

/// How a call whose result is all there is to judge ended.
fn ended(result: rdlt_connector::Result<()>) -> Ended {
    match result {
        Ok(()) => Ended::Answered,
        Err(error) => Ended::Refused(error),
    }
}

/// Reads one partition of `connection`'s source, barrier 1 raised before the read where
/// `barrier`.
async fn read(connection: Arc<Connection>, barrier: bool) -> Ended {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN));
    if barrier {
        feed.request_checkpoint(1);
    }
    let stream = StreamName::new("items").expect("a valid stream name");
    let request = ReadRequest::new(stream, Partition::single(), None);
    let source = RemoteSource::new(connection);
    let taking = async {
        let mut answered = Vec::new();
        while let Some(event) = feed.recv().await {
            if let SourceEvent::Checkpoint { answers, .. } = event {
                answered.extend(answers);
            }
        }
        answered
    };
    let (read, answered) = tokio::join!(source.read(request, sink), taking);
    match read {
        Err(error) => Ended::Refused(error),
        Ok(()) if barrier && answered == [1] => Ended::Answered,
        Ok(()) => Ended::Wrong(format!("its read ended, answering barriers {answered:?}")),
    }
}

/// The session an attack opens, of a pipeline no run loads.
fn context() -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse("sim-broken").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    }
}

/// Opens a session of `connection`'s destination.
async fn open(connection: Arc<Connection>) -> Ended {
    let destination = match RemoteDestination::new(connection) {
        Ok(destination) => destination,
        Err(error) => return Ended::Refused(error),
    };
    match destination.open(&context()).await {
        Ok(_) => Ended::Wrong("its session opened".to_owned()),
        Err(error) => Ended::Refused(error),
    }
}

/// Writes four batches of a table in a session of `connection`'s destination, then flushes
/// them.
async fn write(connection: Arc<Connection>) -> Ended {
    let destination = match RemoteDestination::new(connection) {
        Ok(destination) => destination,
        Err(error) => return Ended::Refused(error),
    };
    let written = async {
        let mut opened = destination.open(&context()).await?;
        let mut writer = opened.session.writer(&table()).await?;
        for segment in 1..=4 {
            writer.write(SegmentId(segment), ids()).await?;
        }
        writer.flush().await.map(drop)
    };
    match written.await {
        Ok(()) => Ended::Wrong("its writes were taken".to_owned()),
        Err(error) => Ended::Refused(error),
    }
}

/// The table an attack writes.
fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["items"]).expect("a valid table path"),
        name: Arc::from("items"),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// A batch of ten ids.
fn ids() -> RecordBatch {
    let ids: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from_iter_values(0..10));
    RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch")
}

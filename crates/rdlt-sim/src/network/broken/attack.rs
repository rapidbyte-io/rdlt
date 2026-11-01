//! An attack on a broken connector: the engine's host dials it, makes the call its fault breaks
//! and is judged by how it met it, within a bound.

use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::wire::TRANSPORT;
use rdlt_connector::{
    ConnectorError, ConnectorId, Destination as _, OpenContext, Partition, PipelineId, ReadRequest,
    SchemaVersion, SegmentId, Source as _, SourceEvent, StreamName, TablePath, TableRef,
    partition_channel,
};
use rdlt_host::{
    CONNECTOR_LOST, Connection, ConnectorRef, DEADLINE_EXCEEDED, Deadlines, Options,
    RemoteDestination, RemoteSource, TLS, Wire,
};
use rdlt_wire::Limits;

use super::super::{BROKEN, Networked, patience};
use super::{Call, Expect, ID, catalogue, port};
use crate::reached::Reach;
use crate::rng::SplitMix64;
use crate::world::World;

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
        let (done, _) = tokio::join!(work, Box::pin(breaker.attack(index, rng)));
        done
    }
}

impl Breaker<'_> {
    /// Attacks the broken connector at `index`, its options drawn from `rng`, and judges how it
    /// ended: whether the network's disruption kept it from being met.
    pub(super) async fn attack(&self, index: usize, mut rng: SplitMix64) -> bool {
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
        disrupted(broken.expect, &ended)
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
pub(super) fn attacking(rng: &mut SplitMix64, connect: Duration, small: bool) -> Options {
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

/// How long an attack with `options` may take: its dial, its handshake and its configuration,
/// each within the connect deadline, four of its heartbeat's patience, a write's wait for
/// credit, and [`LATE`].
pub(super) fn bound(options: &Options) -> Duration {
    options.deadlines.connect.saturating_mul(3)
        + patience(options).saturating_mul(4)
        + options.deadlines.write_ack
        + LATE
}

/// Whether the attack passes only because the network was disrupted: judged in full it is a
/// finding, judged as lost it is not.
fn disrupted(expect: Expect, ended: &Ended) -> bool {
    judge(expect, ended, true).is_some() && judge(expect, ended, false).is_none()
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

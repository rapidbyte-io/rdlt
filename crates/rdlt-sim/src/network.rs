//! A simulated network: the source and the destination listen on hosts of their own over mutual
//! TLS, and the engine places them there through the host's remote placement, all on turmoil's
//! hosts, each a paused tokio clock the network steps together.

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::serve::{Listener, Served, serve_listener};
use rdlt_connector::{BoxFuture, Destination, Source, destination_factory, source_factory};
use rdlt_host::{
    ConnectorRef, Deadlines, Identity, Network, Options, Placed, Provider as _, ProviderError,
    Remote, Stream,
};
use rdlt_testkit::tls::{Files, Pki};
use rdlt_wire::Limits;

use crate::destination::SimDestination;
use crate::env::SimEnv;
use crate::rng::SplitMix64;
use crate::seed::{Seed, report_failure};
use crate::source::SimSource;

/// The engine's host.
const ENGINE: &str = "engine";

/// The port each connector listens on.
const PORT: u16 = 7443;

/// The simulated time each step of the network advances every host's clock.
const TICK: Duration = Duration::from_millis(1);

/// The simulated time after which a simulation that has not ended fails.
const LIMIT: Duration = Duration::from_secs(10_000_000);

/// How many times placing a connector is tried before the simulation calls it unreachable.
const PLACEMENTS: u32 = 600;

/// How long placing a connector waits before trying again.
const REPLACE: Duration = Duration::from_millis(100);

/// A connector's side of a pipeline, each on a host of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Source,
    Destination,
}

impl Side {
    const BOTH: [Self; 2] = [Self::Source, Self::Destination];

    /// The name of the host the connector listens on.
    fn host(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
        }
    }
}

/// The network one simulation runs on: its certificates.
pub(crate) struct Net {
    pki: Pki,
    server: Files,
    client: Files,
}

impl Net {
    fn new() -> Self {
        let pki = Pki::new("ca");
        let server = pki.server("server", &[Side::Source.host(), Side::Destination.host()]);
        let client = pki.client(ENGINE);
        Self {
            pki,
            server,
            client,
        }
    }
}

/// Runs `scenario` as the engine's host of a simulated network, on which the source and the
/// destination listen, with a [`SimEnv`] seeded by `seed`, and turmoil's network seeded by it too.
///
/// # Panics
///
/// Re-raises any panic from `scenario` after printing the seed that reproduces it.
pub(crate) fn run_networked<F, Fut, T>(seed: Seed, scenario: F) -> T
where
    F: FnOnce(Arc<SimEnv>, Arc<Net>) -> Fut + 'static,
    Fut: Future<Output = T> + 'static,
    T: 'static,
{
    let mut rng = SplitMix64::new(seed.value() ^ LATENCY);
    let net = Arc::new(Net::new());
    let mut sim = turmoil::Builder::new()
        .tick_duration(TICK)
        .simulation_duration(LIMIT)
        .min_message_latency(Duration::ZERO)
        .max_message_latency(Duration::from_millis(1 + rng.below(20)))
        .rng_seed(seed.value())
        .build();
    for side in Side::BOTH {
        let net = Arc::clone(&net);
        sim.host(side.host(), move || listen(Arc::clone(&net), side));
    }
    let outcome = Rc::new(RefCell::new(None));
    let slot = Rc::clone(&outcome);
    sim.client(ENGINE, async move {
        let value = scenario(Arc::new(SimEnv::new(seed)), net).await;
        *slot.borrow_mut() = Some(value);
        Ok(())
    });
    let ran = panic::catch_unwind(AssertUnwindSafe(|| sim.run()));
    drop(sim);
    let error = match ran {
        Ok(Ok(())) => {
            return outcome
                .take()
                .expect("the engine's host ran its scenario to the end");
        }
        Ok(Err(error)) => error,
        Err(payload) => {
            report_failure(seed);
            panic::resume_unwind(payload)
        }
    };
    report_failure(seed);
    match error.downcast::<tokio::task::JoinError>() {
        Ok(joined) if joined.is_panic() => panic::resume_unwind(joined.into_panic()),
        Ok(joined) => panic!("{joined}"),
        Err(error) => panic!("{error}"),
    }
}

/// What the latency's draw mixes into the seed: "latency" in ASCII.
const LATENCY: u64 = 0x006c_6174_656e_6379;

/// Serves `side`'s connector on its host.
async fn listen(net: Arc<Net>, side: Side) -> turmoil::Result {
    let tls = rdlt_wire::tls::server_config(&identity(&net.server), &net.pki.ca())
        .map(Arc::new)
        .map_err(|error| error.to_string())?;
    let listener = turmoil::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, PORT)).await?;
    let served = Arc::new(match side {
        Side::Source => Served::new().with_source(source_factory::<SimSource>()),
        Side::Destination => {
            Served::new().with_destination(destination_factory::<SimDestination>())
        }
    });
    let never = std::future::pending();
    serve_listener(served, Sockets(listener), tls, Limits::default(), never).await;
    Ok(())
}

/// A turmoil host's listening socket.
struct Sockets(turmoil::net::TcpListener);

impl Listener for Sockets {
    type Stream = turmoil::net::TcpStream;

    async fn accept(&mut self) -> std::io::Result<(turmoil::net::TcpStream, SocketAddr)> {
        self.0.accept().await
    }
}

/// Turmoil's network, from the engine's host.
#[derive(Debug)]
struct Turmoil;

impl Network for Turmoil {
    fn connect<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> BoxFuture<'a, std::io::Result<Box<dyn Stream>>> {
        Box::pin(async move {
            let stream = turmoil::net::TcpStream::connect((host, port)).await?;
            Ok(Box::new(stream) as Box<dyn Stream>)
        })
    }
}

fn identity(files: &Files) -> Identity {
    Identity {
        cert: files.cert.clone(),
        key: files.key.clone(),
    }
}

/// Places the simulation's connectors on its network: the engine's side of it.
pub(crate) struct Placing {
    remote: Remote,
}

impl Placing {
    /// Places connectors on `net`, running their connections with `options`.
    pub(crate) fn new(net: &Net, options: Options) -> Self {
        let remote = Remote::new(identity(&net.client), net.pki.ca())
            .network(Turmoil)
            .options(options);
        Self { remote }
    }

    /// The source, placed with `config`, trying again until it is reachable.
    pub(crate) async fn source(&self, config: &serde_json::Value) -> Arc<dyn Source> {
        let id = source_factory::<SimSource>().spec().id.clone();
        let reference = reference(Side::Source, id);
        Arc::from(
            placed("source", async || {
                self.remote.source(&reference, config).await
            })
            .await,
        )
    }

    /// The destination, placed with `config`, trying again until it is reachable.
    pub(crate) async fn destination(&self, config: &serde_json::Value) -> Arc<dyn Destination> {
        let id = destination_factory::<SimDestination>().spec().id.clone();
        let reference = reference(Side::Destination, id);
        let place = async || self.remote.destination(&reference, config).await;
        Arc::from(placed("destination", place).await)
    }
}

/// The connector `place` places, trying again until it is reachable.
async fn placed<C>(what: &str, place: impl AsyncFn() -> Result<Placed<C>, ProviderError>) -> C {
    let mut last = None;
    for _ in 0..PLACEMENTS {
        match place().await {
            Ok(placed) => return placed.connector,
            Err(error) => last = Some(error.to_string()),
        }
        tokio::time::sleep(REPLACE).await;
    }
    panic!("the {what} was never placed: {last:?}");
}

/// The reference to connector `id`, listening on `side`'s host.
fn reference(side: Side, id: rdlt_connector::ConnectorId) -> ConnectorRef {
    ConnectorRef::new(id).endpoint(format!("grpcs://{}:{PORT}", side.host()))
}

/// The options the engine's connections run with, drawn from `rng`: heartbeats between 50 ms and
/// a second apart, two to five of them missed before a connector counts as lost, and between
/// 200 ms and 3 s to connect.
pub(crate) fn options(rng: &mut SplitMix64) -> Options {
    Options {
        heartbeat: Duration::from_millis(50 + rng.below(950)),
        missed: 2 + u32::try_from(rng.below(4)).unwrap_or(0),
        deadlines: Deadlines {
            connect: Duration::from_millis(200 + rng.below(2800)),
            ..Deadlines::default()
        },
        ..Options::default()
    }
}

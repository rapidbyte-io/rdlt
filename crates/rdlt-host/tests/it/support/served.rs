//! Connectors served over the other end of a socket, and an engine to load through them.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::{Role, destination_factory, source_factory};
use rdlt_connector_reference::{MemoryDestination, MemorySource};
use rdlt_engine::{
    CommitPolicy, ComputePool, Cores, Engine, EngineConfig, EngineConfigBuilder, Env, Job, Sleep,
    SystemEnv,
};
use rdlt_host::{Connection, Options, RemoteDestination, RemoteSource};
use rdlt_wire::Limits;
use rdlt_wire::v1::connector_client::ConnectorClient;
use tokio::io::DuplexStream;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};

/// The host's end of a socket whose other end serves `served`.
pub(crate) fn served(served: Served) -> UnixStream {
    served_within(served, Limits::default())
}

/// The host's end of a socket whose other end serves `served`, enforcing `limits`.
pub(crate) fn served_within(served: Served, limits: Limits) -> UnixStream {
    let (host, connector) = UnixStream::pair().expect("a socket pair");
    tokio::spawn(serve_connection(Arc::new(served), connector, limits));
    host
}

/// The host's end of an in-memory pipe whose other end serves `served`: no socket, so a runtime
/// whose clock is paused advances it only while every task waits.
pub(crate) fn piped(served: Served) -> DuplexStream {
    let (host, connector) = tokio::io::duplex(1 << 20);
    tokio::spawn(serve_connection(
        Arc::new(served),
        connector,
        Limits::default(),
    ));
    host
}

/// A raw client of the connector served on the other end of `io`, which has had no handshake.
pub(crate) async fn raw_client(io: UnixStream) -> ConnectorClient<Channel> {
    ConnectorClient::new(raw_channel(io).await)
}

/// A client of the connector served on the other end of `io`, which has had no handshake: its
/// generated calls and the data plane's, as certification speaks the protocol.
pub(crate) async fn plane_client(io: UnixStream) -> rdlt_host::remote::Client {
    rdlt_host::remote::client(io, Options::default())
        .await
        .expect("the channel connects")
}

/// A channel to the connector served on the other end of `io`.
pub(crate) async fn raw_channel(io: UnixStream) -> Channel {
    let slot = std::sync::Mutex::new(Some(io));
    let connector = tower::service_fn(move |_| {
        let io = slot.lock().expect("the lock is not poisoned").take();
        async move {
            io.map(hyper_util::rt::TokioIo::new)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotConnected))
        }
    });
    Endpoint::from_static("http://connector")
        .connect_with_connector(connector)
        .await
        .expect("the channel connects")
}

/// The memory source, served, with `config`.
pub(crate) async fn memory_source(config: serde_json::Value, options: &Options) -> RemoteSource {
    let io = served(Served::new().with_source(source_factory::<MemorySource>()));
    let connection = Connection::connect(io, Role::Source, &config, *options)
        .await
        .expect("the source handshakes");
    RemoteSource::new(connection)
}

/// The memory destination over `store`, served.
pub(crate) async fn memory_destination(store: &str, options: &Options) -> RemoteDestination {
    let io = served(Served::new().with_destination(destination_factory::<MemoryDestination>()));
    let config = serde_json::json!({ "store": store });
    let connection = Connection::connect(io, Role::Destination, &config, *options)
        .await
        .expect("the destination handshakes");
    RemoteDestination::new(connection).expect("the destination declares its capabilities")
}

/// An engine that commits every `rows` rows.
pub(crate) fn engine(rows: u64) -> Engine {
    let policy = CommitPolicy::new(None, Some(rows), None).expect("a row threshold is valid");
    let config = EngineConfig::builder()
        .commit(policy)
        .lanes(2)
        .build()
        .expect("the engine's configuration is valid");
    let env = system_env();
    Engine::new(config, Arc::new(env))
}

/// The system environment declaring four cores whatever runtime the test runs on, so default lanes
/// and the compute pool are alike on every machine.
pub(crate) fn system_env() -> SystemEnv {
    let cores = Cores::new(NonZeroUsize::new(4).expect("four is not zero"));
    SystemEnv::try_new(cores).expect("the compute pool starts")
}

/// An engine with `config` on the system environment, its compute jobs run on the calling thread:
/// a runtime whose clock is paused would otherwise advance it while a job runs on another thread.
pub(crate) fn paused_engine(config: EngineConfigBuilder) -> Engine {
    let config = config.build().expect("the engine's configuration is valid");
    Engine::new(config, Arc::new(Inline(system_env())))
}

/// The system environment, its compute jobs run inline.
struct Inline(SystemEnv);

impl Env for Inline {
    fn now(&self) -> SystemTime {
        self.0.now()
    }

    fn instant(&self) -> Instant {
        self.0.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        self.0.sleep(duration)
    }

    fn random(&self) -> u64 {
        self.0.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        self
    }

    fn cores(&self) -> NonZeroUsize {
        self.0.cores()
    }
}

impl ComputePool for Inline {
    fn execute(&self, job: Job) {
        job();
    }
}

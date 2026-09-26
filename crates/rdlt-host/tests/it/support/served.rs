//! Connectors served over the other end of a socket, and an engine to load through them.

use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::{Role, destination_factory, source_factory};
use rdlt_connector_reference::{MemoryDestination, MemorySource};
use rdlt_engine::{CommitPolicy, Engine, EngineConfig, RayonPool, SystemEnv};
use rdlt_host::{Connection, Options, RemoteDestination, RemoteSource};
use rdlt_wire::Limits;
use rdlt_wire::v1::connector_client::ConnectorClient;
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

/// A raw client of the connector served on the other end of `io`, which has had no handshake.
pub(crate) async fn raw_client(io: UnixStream) -> ConnectorClient<Channel> {
    let slot = std::sync::Mutex::new(Some(io));
    let connector = tower::service_fn(move |_| {
        let io = slot.lock().expect("the lock is not poisoned").take();
        async move {
            io.map(hyper_util::rt::TokioIo::new)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotConnected))
        }
    });
    let channel = Endpoint::from_static("http://connector")
        .connect_with_connector(connector)
        .await
        .expect("the channel connects");
    ConnectorClient::new(channel)
}

/// The memory source, served, with `config`.
pub(crate) async fn memory_source(config: serde_json::Value, options: Options) -> RemoteSource {
    let io = served(Served::new().with_source(source_factory::<MemorySource>()));
    let connection = Connection::connect(io, Role::Source, &config, options)
        .await
        .expect("the source handshakes");
    RemoteSource::new(connection)
}

/// The memory destination over `store`, served.
pub(crate) async fn memory_destination(store: &str, options: Options) -> RemoteDestination {
    let io = served(Served::new().with_destination(destination_factory::<MemoryDestination>()));
    let config = serde_json::json!({ "store": store });
    let connection = Connection::connect(io, Role::Destination, &config, options)
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
    let threads = NonZeroUsize::new(2).expect("two is not zero");
    let env = SystemEnv::new(RayonPool::new(threads).expect("the compute pool starts"));
    Engine::new(config, Arc::new(env))
}

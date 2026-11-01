//! Configurations that run beside each other on one connection, in a build with
//! certification's probes.

use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_wire::Limits;
use rdlt_wire::plane::Plane as _;
use rdlt_wire::tonic::{Request, Status};
use rdlt_wire::v1::connector_server::Connector as _;
use tokio_stream::StreamExt as _;
use tokio_util::sync::CancellationToken;

use crate::destination::{
    Destination, DestinationFactory, OpenContext, OpenedSession, PublishedReader, PublishedRows,
    Reading, TableRef,
};
use crate::error::{ConnectorError, Result};
use crate::id::{PartitionId, StreamName};
use crate::serve::Served;
use crate::serve::service::Service;
use crate::sink::PartitionSink;
use crate::source::{
    AcknowledgedReader, Acknowledging, PartitionPlan, ReadRequest, Source, SourceFactory,
};
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec};
use crate::state::StreamState;
use crate::wire::{error, v1};
use crate::{Capabilities, Catalog, ConnectorId, Cursor};

/// Bounds every wait of a test: a configuration that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

/// The configuration that waits once it has connected, and the thread it runs in.
const HELD: &str = "held";

/// A connector and its probe, each failing every call with the code of the configuration that
/// connected it.
struct Named(&'static str, Capabilities);

impl Named {
    fn refused(&self) -> ConnectorError {
        ConnectorError::config("named").with_code(self.0)
    }
}

impl Destination for Named {
    fn capabilities(&self) -> &Capabilities {
        &self.1
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(self.refused()) })
    }

    fn open<'a>(&'a self, _: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        unreachable!("nothing opens")
    }
}

impl PublishedReader for Named {
    fn published<'a>(&'a self, _: &'a TableRef, _: PublishedRows) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(self.refused()) })
    }
}

impl Source for Named {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(self.refused()) })
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        unreachable!("nothing discovers")
    }

    fn plan<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        unreachable!("nothing plans")
    }

    fn read(&self, _: ReadRequest, _: PartitionSink) -> BoxFuture<'_, Result<()>> {
        unreachable!("nothing reads")
    }

    fn committed<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        unreachable!("nothing commits")
    }
}

impl AcknowledgedReader for Named {
    fn acknowledged<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a PartitionId,
    ) -> BoxFuture<'a, Result<Option<Cursor>>> {
        Box::pin(async { Err(self.refused()) })
    }
}

/// A factory of [`Named`] connectors, named by their configuration.
///
/// The configuration named [`HELD`], in a thread of that name, tells `entered` once it has
/// connected and waits for `released`. `broken` names a configuration that fails, and `slow` one
/// that takes an hour.
struct Holding {
    spec: ConnectorSpec,
    entered: mpsc::Sender<()>,
    released: Mutex<mpsc::Receiver<()>>,
}

impl Holding {
    /// The spec, asked for once a configuration has connected, held where [`HELD`]'s asks.
    fn waited(&self) -> &ConnectorSpec {
        if std::thread::current().name() == Some(HELD) {
            self.entered
                .send(())
                .expect("the test waits for the held configuration");
            let released = self.released.lock();
            released
                .recv_timeout(WAIT)
                .expect("the test releases the held configuration");
        }
        &self.spec
    }

    async fn named(config: &serde_json::Value) -> Result<Arc<Named>> {
        let name = match config.as_str() {
            Some(HELD) => HELD,
            Some("broken") => return Err(ConnectorError::config("broken").with_code("broken")),
            Some("slow") => {
                tokio::time::sleep(Duration::from_mins(60)).await;
                "slow"
            }
            _ => "free",
        };
        Ok(Arc::new(Named(name, Capabilities::minimal())))
    }
}

impl DestinationFactory for Holding {
    fn spec(&self) -> &ConnectorSpec {
        self.waited()
    }

    fn connect(
        &self,
        _: serde_json::Value,
        _: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        unreachable!("a probed configuration connects reading")
    }

    fn reads_back(&self) -> bool {
        true
    }

    fn connect_reading(
        &self,
        config: serde_json::Value,
        _: ConnectContext,
    ) -> BoxFuture<'_, Result<Reading>> {
        Box::pin(async move {
            let named = Self::named(&config).await?;
            let destination: Arc<dyn Destination> = Arc::clone(&named) as Arc<dyn Destination>;
            Ok((destination, named as Arc<dyn PublishedReader>))
        })
    }
}

impl SourceFactory for Holding {
    fn spec(&self) -> &ConnectorSpec {
        self.waited()
    }

    fn connect(
        &self,
        _: serde_json::Value,
        _: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Source>>> {
        unreachable!("a probed configuration connects acknowledging")
    }

    fn acknowledges(&self) -> bool {
        true
    }

    fn connect_acknowledging(
        &self,
        config: serde_json::Value,
        _: ConnectContext,
    ) -> BoxFuture<'_, Result<Acknowledging>> {
        Box::pin(async move {
            let named = Self::named(&config).await?;
            let source: Arc<dyn Source> = Arc::clone(&named) as Arc<dyn Source>;
            Ok((source, named as Arc<dyn AcknowledgedReader>))
        })
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime builds")
}

/// A connection serving a [`Holding`] factory in `role`, its handshake offering the role's probe
/// and agreed, and the ends that hold and release [`HELD`]'s configuration.
fn connection(role: v1::Role) -> (Arc<Service>, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered, entering) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let factory = Holding {
        spec: ConnectorSpec {
            id: ConnectorId::parse("io.test.raced").expect("a valid id"),
            version: "0.0.1".to_owned(),
            config_schema: serde_json::Value::Null,
        },
        entered,
        released: Mutex::new(released),
    };
    let (served, feature) = match role {
        v1::Role::Source => (
            Served::new().with_source(Box::new(factory)),
            rdlt_wire::ACKNOWLEDGED,
        ),
        _ => (
            Served::new().with_destination(Box::new(factory)),
            rdlt_wire::PUBLISHED,
        ),
    };
    let stopping = CancellationToken::new();
    let service = Service::new(Arc::new(served), Limits::default(), None, 3, stopping);
    let handshake = v1::HandshakeRequest {
        role: role as i32,
        protocol_major: rdlt_wire::PROTOCOL_MAJOR,
        features: vec![feature.to_owned()],
        ..v1::HandshakeRequest::default()
    };
    service.agree(&handshake).expect("the handshake agrees");
    (Arc::new(service), entering, release)
}

fn configure(name: &str) -> v1::ConfigureRequest {
    v1::ConfigureRequest {
        config_json: format!("\"{name}\""),
    }
}

/// Configures `service` with `name`'s configuration, in a thread named `name`.
fn configuring(
    service: &Arc<Service>,
    name: &'static str,
) -> JoinHandle<Result<v1::ConnectorSpec>> {
    let service = Arc::clone(service);
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || runtime().block_on(service.connect(configure(name))))
        .expect("a thread starts")
}

/// The codes the connected connector's check and its probe fail with: each the name of the
/// configuration that connected it.
async fn answers(service: &Service, role: v1::Role) -> [Option<String>; 2] {
    let code = |status: &Status| error(status).code().map(str::to_owned);
    let checked = service
        .check(Request::new(v1::CheckRequest {}))
        .await
        .expect_err("the check fails with its name");
    let probed = if role == v1::Role::Source {
        let asked = v1::ReadAcknowledgedRequest {
            stream: Some(v1::StreamName {
                namespace: None,
                name: "rows".to_owned(),
            }),
            partition: "p0".to_owned(),
        };
        service
            .read_acknowledged(Request::new(asked))
            .await
            .expect_err("the probe fails with its name")
    } else {
        let table = v1::TableRef {
            path: Some(v1::TablePath {
                segments: vec!["rows".to_owned()],
            }),
            name: "rows".to_owned(),
            version: 1,
            ..v1::TableRef::default()
        };
        let request = v1::ReadPublishedRequest { table: Some(table) };
        let mut frames = service
            .read_published(request)
            .await
            .expect("the read-back is served");
        let first = tokio::time::timeout(WAIT, frames.next())
            .await
            .expect("the read-back answers");
        first
            .expect("the read-back answers once")
            .expect_err("the read-back fails with its name")
    };
    [code(&checked), code(&probed)]
}

/// Two configurations of `role` at once, [`HELD`]'s connecting first: the name of the configuration that
/// connected, the code the other was refused with, and the codes of [`answers`].
fn raced(role: v1::Role) -> (&'static str, Option<String>, [Option<String>; 2]) {
    let (service, entering, release) = connection(role);
    let held = configuring(&service, HELD);
    entering
        .recv_timeout(WAIT)
        .expect("the held configuration connects");
    let free = configuring(&service, "free");
    // Time for a configuration that does not wait for the held one to end before it.
    std::thread::sleep(Duration::from_millis(200));
    release.send(()).expect("the held configuration waits");
    let held = held.join().expect("the held configuration does not panic");
    let free = free.join().expect("the free configuration does not panic");
    let (won, lost) = match (held, free) {
        (Ok(_), Err(lost)) => (HELD, lost),
        (Err(lost), Ok(_)) => ("free", lost),
        (held, free) => panic!("one configuration connects: {held:?}, {free:?}"),
    };
    let answered = runtime().block_on(answers(&service, role));
    (won, lost.code().map(str::to_owned), answered)
}

#[test]
fn a_destination_configured_twice_at_once_connects_once_and_reads_back_what_it_connected() {
    let (won, lost, answered) = raced(v1::Role::Destination);
    assert_eq!(lost.as_deref(), Some("configure_repeated"));
    let won = Some(won.to_owned());
    assert_eq!(
        answered,
        [won.clone(), won],
        "the destination and its read-back"
    );
}

#[test]
fn a_source_configured_twice_at_once_connects_once_and_tells_where_what_it_connected_stands() {
    let (won, lost, answered) = raced(v1::Role::Source);
    assert_eq!(lost.as_deref(), Some("configure_repeated"));
    let won = Some(won.to_owned());
    assert_eq!(
        answered,
        [won.clone(), won],
        "the source and its acknowledger"
    );
}

#[tokio::test(start_paused = true)]
async fn a_configuration_that_fails_or_is_cancelled_publishes_nothing_and_the_next_connects() {
    for role in [v1::Role::Destination, v1::Role::Source] {
        let (service, _entering, _release) = connection(role);
        let broken = service
            .connect(configure("broken"))
            .await
            .expect_err("the broken configuration fails");
        assert_eq!(broken.code(), Some("broken"));
        let slow = tokio::time::timeout(Duration::from_secs(1), service.connect(configure("slow")));
        assert!(slow.await.is_err(), "the slow configuration is cancelled");
        tokio::time::timeout(WAIT, service.connect(configure("free")))
            .await
            .expect("the free configuration ends")
            .expect("the free configuration connects");
        let free = Some("free".to_owned());
        assert_eq!(
            answers(&service, role).await,
            [free.clone(), free],
            "{role:?}"
        );
    }
}

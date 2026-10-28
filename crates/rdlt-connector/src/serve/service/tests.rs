use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use rdlt_wire::Limits;
use rdlt_wire::tonic::Request;
use rdlt_wire::v1::connector_server::Connector as _;
use tokio_util::sync::CancellationToken;

use super::Service;
use crate::destination::{Destination, DestinationFactory, OpenContext, OpenedSession};
use crate::error::Result;
use crate::serve::Served;
use crate::serve::live::CountedSession;
use crate::spec::{BoxFuture, ConnectContext, ConnectorSpec};
use crate::wire::v1;
use crate::{Capabilities, ConnectorId, Epoch};

/// Bounds every wait of a test: an Open that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

/// Sessions counted in `live` from the start of their open, which takes longer for each of three
/// opens in turn, and `peak` the most at once.
#[derive(Clone, Default)]
struct Counts {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    opened: Arc<AtomicU64>,
}

struct Slow(Capabilities, Counts);

impl Destination for Slow {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn open<'a>(&'a self, _: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let counts = &self.1;
            // What a destination opens for a session it holds while it opens it.
            let session = CountedSession::new(&counts.live, Duration::ZERO);
            let live = counts.live.load(Ordering::SeqCst);
            counts.peak.fetch_max(live, Ordering::SeqCst);
            let nth = counts.opened.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10 * (nth % 3 + 1))).await;
            Ok(OpenedSession {
                session,
                epoch: Epoch(nth + 1),
                landed: None,
                state: Vec::new(),
            })
        })
    }
}

struct Factory(ConnectorSpec, Counts);

impl DestinationFactory for Factory {
    fn spec(&self) -> &ConnectorSpec {
        &self.0
    }

    fn connect(
        &self,
        _: serde_json::Value,
        _: ConnectContext,
    ) -> BoxFuture<'_, Result<Box<dyn Destination>>> {
        let slow = Slow(Capabilities::minimal(), self.1.clone());
        Box::pin(async move { Ok(Box::new(slow) as Box<dyn Destination>) })
    }
}

#[tokio::test(start_paused = true)]
async fn concurrent_opens_on_a_connection_never_hold_more_sessions_than_it_may() {
    let counts = Counts::default();
    let spec = ConnectorSpec {
        id: ConnectorId::parse("io.test.sessions").expect("a valid id"),
        version: "0.0.1".to_owned(),
        config_schema: serde_json::Value::Null,
    };
    let served = Served::new().with_destination(Box::new(Factory(spec, counts.clone())));
    let stopping = CancellationToken::new();
    let service = Arc::new(Service::new(
        Arc::new(served),
        Limits::default(),
        None,
        3,
        stopping,
    ));
    let handshake = v1::HandshakeRequest {
        role: v1::Role::Destination as i32,
        protocol_major: rdlt_wire::PROTOCOL_MAJOR,
        ..v1::HandshakeRequest::default()
    };
    service.agree(&handshake).expect("the handshake agrees");
    let configure = v1::ConfigureRequest {
        config_json: "{}".to_owned(),
    };
    service
        .connect(configure)
        .await
        .expect("the destination connects");
    let mut opens = tokio::task::JoinSet::new();
    for pipeline in 0..8 {
        let service = Arc::clone(&service);
        opens.spawn(async move {
            let request = v1::OpenRequest {
                pipeline: format!("pipeline-{pipeline}"),
            };
            let opened = service.open(Request::new(request)).await;
            opened.map(|opened| opened.into_inner().session)
        });
    }
    let mut ids = Vec::new();
    while let Some(opened) = tokio::time::timeout(WAIT, opens.join_next())
        .await
        .expect("every open ends")
    {
        ids.push(
            opened
                .expect("no open panics")
                .expect("every open succeeds"),
        );
    }
    ids.sort_unstable();
    assert_eq!(ids, (1..=8).collect::<Vec<u64>>());
    let peak = counts.peak.load(Ordering::SeqCst);
    assert!(
        peak <= 3,
        "{peak} sessions at once on a connection that holds three"
    );
    assert_eq!(counts.live.load(Ordering::SeqCst), 3);
}

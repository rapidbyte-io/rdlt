use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use rdlt_wire::Limits;
use rdlt_wire::bounded::{Bounded, Bounds};
use rdlt_wire::limits::Class;
use rdlt_wire::plane::{Incoming, Outgoing};
use rdlt_wire::tonic::body::Body;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use super::pump;
use crate::error::Result;
use crate::id::{PartitionId, StreamName};
use crate::serve::live::{Live, gone};
use crate::sink::PartitionSink;
use crate::source::{Partition, PartitionPlan, ReadRequest, Source};
use crate::spec::BoxFuture;
use crate::state::StreamState;
use crate::wire::v1;
use crate::{Catalog, Cursor};

/// Bounds every wait of a test: a read that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

/// How long a test lets a task that should end at once go on before it fails.
const HOUR: Duration = Duration::from_mins(60);

/// A source whose read waits for ever once begun, its sink kept or let go first, counted in
/// `live` while it reads.
struct Stuck {
    keeps_sink: bool,
    begun: Notify,
    live: Arc<AtomicUsize>,
}

impl Source for Stuck {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        unreachable!("the tests only read")
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        unreachable!("the tests only read")
    }

    fn plan<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        unreachable!("the tests only read")
    }

    fn read(&self, _: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let _live = Live::new(&self.live);
            // A source that let its sink go may still clean up before it returns.
            let _sink = self.keeps_sink.then_some(sink);
            self.begun.notify_one();
            std::future::pending::<()>().await;
            Ok(())
        })
    }

    fn committed<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        unreachable!("the tests only read")
    }
}

/// The host's end of a read's controls, and the controls as the pump reads them.
fn controls(limits: &Limits) -> (mpsc::Sender<v1::ReadControl>, Incoming<v1::ReadControl>) {
    let (control, sent) = mpsc::channel(1);
    let body = Body::new(Outgoing::request(
        ReceiverStream::new(sent),
        limits.largest(),
    ));
    let bounds = Bounds::of(
        limits,
        Class::of_request("Read"),
        rdlt_wire::scan::request("Read"),
    );
    (control, Incoming::request(Bounded::new(body, bounds, None)))
}

fn request() -> ReadRequest {
    let partition = Partition::new(PartitionId::parse("p0").expect("a valid partition id"));
    ReadRequest::new(
        StreamName::new("rows").expect("a valid name"),
        partition,
        None,
    )
}

fn stuck(keeps_sink: bool, live: &Arc<AtomicUsize>) -> Arc<Stuck> {
    Arc::new(Stuck {
        keeps_sink,
        begun: Notify::new(),
        live: Arc::clone(live),
    })
}

#[tokio::test(start_paused = true)]
async fn a_read_ends_once_its_host_goes_though_its_source_let_its_sink_go_and_cleans_up() {
    for keeps_sink in [true, false] {
        let limits = Limits::default();
        let live = Arc::new(AtomicUsize::new(0));
        let source = stuck(keeps_sink, &live);
        let (control, controls) = controls(&limits);
        let (frames, answers) = mpsc::channel(16);
        let pumping = tokio::spawn(pump(
            Arc::clone(&source) as _,
            request(),
            0,
            controls,
            frames,
            limits,
        ));
        tokio::time::timeout(WAIT, source.begun.notified())
            .await
            .expect("the source reads");
        // The pump finds the feed ended where the source let its sink go.
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(control);
        drop(answers);
        tokio::time::timeout(HOUR, pumping)
            .await
            .unwrap_or_else(|_| panic!("the read ends once its host goes, sink kept: {keeps_sink}"))
            .expect("the pump does not panic");
        tokio::time::timeout(WAIT, gone(&live))
            .await
            .expect("the source's read is dropped with the pump");
    }
}

#[tokio::test(start_paused = true)]
async fn a_read_ends_once_its_host_drops_its_answers_though_its_request_stays_open() {
    let limits = Limits::default();
    let live = Arc::new(AtomicUsize::new(0));
    let source = stuck(true, &live);
    let (control, controls) = controls(&limits);
    let (frames, answers) = mpsc::channel(16);
    let pumping = tokio::spawn(pump(
        Arc::clone(&source) as _,
        request(),
        0,
        controls,
        frames,
        limits,
    ));
    tokio::time::timeout(WAIT, source.begun.notified())
        .await
        .expect("the source reads");
    drop(answers);
    tokio::time::timeout(HOUR, pumping)
        .await
        .expect("the read ends once its answers are dropped")
        .expect("the pump does not panic");
    tokio::time::timeout(WAIT, gone(&live))
        .await
        .expect("the source's read is dropped with the pump");
    drop(control);
}

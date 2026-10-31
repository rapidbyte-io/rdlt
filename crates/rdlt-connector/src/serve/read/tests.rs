use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use rdlt_wire::Limits;
use rdlt_wire::bounded::{Bounded, Bounds};
use rdlt_wire::limits::{CREDIT_FLOOR, Class};
use rdlt_wire::plane::{Incoming, Outgoing};
use rdlt_wire::tonic::Code;
use rdlt_wire::tonic::body::Body;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use super::window::Window;
use super::{EVENTS, Outbox, finish, pump};
use crate::cost::push_charge;
use crate::error::Result;
use crate::id::{PartitionId, StreamName};
use crate::serve::live::{Live, gone};
use crate::sink::{Admission, PartitionSink, Permit, Push, SourceEvent};
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
            Arc::new(Window::new(limits)),
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
        Arc::new(Window::new(limits)),
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

#[tokio::test(start_paused = true)]
async fn a_read_s_panic_tells_its_host_neither_its_message_nor_its_task() {
    let read: tokio::task::JoinHandle<Result<()>> =
        tokio::spawn(async { panic!("the read held row 4417 of the ledger") });
    let ended = tokio::time::timeout(WAIT, read)
        .await
        .expect("the read ends");
    let mut outbox = Outbox::new(Limits::default());
    let Err(status) = finish(&mut outbox, ended) else {
        panic!("a read that panicked fails");
    };
    assert_eq!(status.code(), Code::Internal);
    for told in [
        status.message().to_owned(),
        crate::wire::error(&status).to_string(),
    ] {
        assert!(
            !told.contains("4417") && !told.contains("task"),
            "the host is told {told:?}"
        );
    }
}

/// A source that pushes `batch` for ever, counting the pushes its channel took.
struct Pushing {
    batch: RecordBatch,
    sent: Arc<AtomicUsize>,
}

impl Source for Pushing {
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

    fn read(&self, _: ReadRequest, mut sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            loop {
                let push = Push::Arrow(self.batch.clone());
                sink.send(SourceEvent::Push(push)).await?;
                self.sent.fetch_add(1, Ordering::SeqCst);
            }
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

/// A read's window, counting the bytes the pushes it admitted are charged while they hold room.
struct Counted {
    window: Window,
    held: Arc<AtomicU64>,
}

/// A push's room, which it holds until it leaves the channel.
struct Held {
    _room: Permit,
    charge: u64,
    total: Arc<AtomicU64>,
}

impl Drop for Held {
    fn drop(&mut self) {
        self.total.fetch_sub(self.charge, Ordering::SeqCst);
    }
}

impl Admission for Counted {
    fn admit(&self, event: &SourceEvent) -> BoxFuture<'_, Result<Option<Permit>>> {
        let charge = match event {
            SourceEvent::Push(push) => push_charge(push),
            _ => 0,
        };
        let admitting = self.window.admit(event);
        Box::pin(async move {
            let room = admitting.await?;
            Ok(room.map(|room| {
                self.held.fetch_add(charge, Ordering::SeqCst);
                let total = Arc::clone(&self.held);
                Box::new(Held {
                    _room: room,
                    charge,
                    total,
                }) as Permit
            }))
        })
    }

    fn charge(&self, bytes: u64) -> Result<Permit> {
        self.window.charge(bytes)
    }

    fn limits(&self) -> Limits {
        self.window.limits()
    }
}

/// A batch of `rows` strings of `bytes` each.
fn strings(bytes: usize, rows: usize) -> RecordBatch {
    let value = "s".repeat(bytes);
    let column: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        value.as_str(),
        rows,
    )));
    RecordBatch::try_from_iter([("s", column)]).expect("a batch")
}

#[tokio::test(start_paused = true)]
async fn a_read_whose_host_takes_nothing_holds_a_frame_and_one_push_in_its_channel() {
    // The frame limit of an engine's host at its default budget.
    let limits = Limits {
        frame_bytes: 29_345_742,
        ..Limits::default()
    };
    let frame = limits.frame_bytes;
    // Pushes of 64 MiB, each beyond the frame, and of 4 MiB, sixteen of which pass it.
    for batch in [strings(1_024, 65_536), strings(1_024, 4_096)] {
        let push = push_charge(&Push::Arrow(batch.clone()));
        let (sent, held) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicU64::new(0)));
        let source = Arc::new(Pushing {
            batch,
            sent: Arc::clone(&sent),
        });
        let admission = Arc::new(Counted {
            window: Window::new(limits),
            held: Arc::clone(&held),
        });
        let (control, controls) = controls(&limits);
        let (frames, answers) = mpsc::channel(EVENTS.get());
        let pumping = tokio::spawn(pump(source, request(), 0, controls, frames, admission));
        // The host grants the read its first credit, and takes nothing.
        let credit = v1::read_control::Control::Credit(v1::Credit {
            bytes: CREDIT_FLOOR,
        });
        let credit = v1::ReadControl {
            control: Some(credit),
        };
        control
            .send(credit)
            .await
            .expect("the read takes its credit");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let waiting = sent.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            sent.load(Ordering::SeqCst),
            waiting,
            "the source waits for room"
        );
        let channel = held.load(Ordering::SeqCst);
        assert!(channel > 0, "pushes of {push} bytes wait in the channel");
        assert!(
            channel <= frame + push && (push > frame || channel <= frame),
            "pushes of {push} bytes hold {channel} in the channel, beyond a frame of {frame}"
        );
        let pushes = channel / push;
        assert!(
            pushes < u64::try_from(EVENTS.get()).expect("a count"),
            "the bytes bound the channel, not its count: {pushes} pushes of {push} bytes"
        );
        drop((control, answers));
        tokio::time::timeout(HOUR, pumping)
            .await
            .expect("the read ends once its host goes")
            .expect("the pump does not panic");
    }
}

#[test]
fn a_read_s_start_asks_for_a_read_without_the_columns_it_excludes() {
    let start = v1::ReadStart {
        stream: Some(v1::StreamName::from(
            &StreamName::new("rows").expect("a valid name"),
        )),
        partition: "p0".to_owned(),
        follow: true,
        exclude: vec!["secret".to_owned(), "blob".to_owned()],
        ..v1::ReadStart::default()
    };
    let asked = super::request(start).expect("a valid start");
    assert_eq!(
        asked,
        request()
            .following(true)
            .excluding(crate::Exclusion::new(["blob", "secret"]))
    );
}

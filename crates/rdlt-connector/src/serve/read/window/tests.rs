use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use rdlt_testkit::cancel::{self, Ended};
use rdlt_wire::Limits;
use tokio::sync::mpsc;

use super::Window;
use crate::cost::push_charge;
use crate::cursor::Cursor;
use crate::error::ConnectorErrorKind;
use crate::sink::{
    Admission, Headroom, PartitionFeed, PartitionSink, Permit, Push, SourceEvent,
    admitted_partition_channel,
};

/// Bytes: the frame limit of the tests' host, and so their window.
const FRAME: u64 = 1_000;

/// How long the engine takes before it receives each event.
const SLOW: Duration = Duration::from_millis(30);

/// Bounds every wait of a test.
const WAIT: Duration = Duration::from_secs(60);

fn host() -> Limits {
    Limits {
        frame_bytes: FRAME,
        ..Limits::default()
    }
}

/// A JSON push of `text` bytes, charged three times them and a message's room.
fn json(text: usize) -> SourceEvent {
    SourceEvent::Push(Push::Json(Bytes::from(vec![b' '; text])))
}

/// The bytes `event`, a push, is charged.
fn charge(event: &SourceEvent) -> u64 {
    let SourceEvent::Push(push) = event else {
        panic!("{event:?} is not a push");
    };
    push_charge(push)
}

/// What admitting `event` gave, where it took no longer than `WAIT`.
async fn admitted(window: &Window, event: &SourceEvent) -> Option<Option<Permit>> {
    let admitted = tokio::time::timeout(WAIT, window.admit(event)).await;
    admitted
        .ok()
        .map(|admitted| admitted.expect("a window refuses nothing"))
}

#[tokio::test(start_paused = true)]
async fn a_push_takes_its_charge_of_the_window_and_one_beyond_what_is_left_waits() {
    let window = Window::new(host());
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let batch = SourceEvent::Push(Push::Arrow(
        RecordBatch::try_from_iter([("id", ids)]).expect("a batch"),
    ));
    let left = FRAME - charge(&batch);
    assert!(left > 256, "the batch leaves room for a JSON push: {left}");
    let _batch = admitted(&window, &batch).await.expect("room");
    // The longest JSON text whose charge fits what the batch left, and one a byte longer.
    let fits = usize::try_from((left - 256) / 3).expect("small");
    let (fitting, beyond) = (json(fits), json(fits + 1));
    assert!(charge(&fitting) <= left && charge(&beyond) > left);
    assert!(
        admitted(&window, &beyond).await.is_none(),
        "a push beyond what is left waits"
    );
    let fitted = admitted(&window, &fitting)
        .await
        .expect("a push within what is left");
    assert!(fitted.is_some(), "a push holds its room");
    assert!(
        admitted(&window, &json(0)).await.is_none(),
        "the window is full"
    );
    drop(fitted);
    assert!(
        admitted(&window, &json(0)).await.is_some(),
        "a push let go gives its room back"
    );
}

#[tokio::test(start_paused = true)]
async fn a_push_beyond_the_window_waits_until_no_push_holds_any_and_takes_it_all() {
    let window = Arc::new(Window::new(host()));
    let small = admitted(&window, &json(48)).await.expect("room");
    let large = json(1_000);
    assert!(charge(&large) > FRAME);
    let waiting = {
        let window = Arc::clone(&window);
        tokio::spawn(async move { window.admit(&large).await })
    };
    tokio::time::sleep(WAIT).await;
    assert!(
        !waiting.is_finished(),
        "a push beyond the window waits while another holds any of it"
    );
    drop(small);
    let large = tokio::time::timeout(WAIT, waiting)
        .await
        .expect("the large push is admitted once the window is empty")
        .expect("the admission does not panic")
        .expect("a window refuses nothing");
    assert!(large.is_some());
    assert!(
        admitted(&window, &json(0)).await.is_none(),
        "the large push holds the whole window"
    );
    drop(large);
    assert!(admitted(&window, &json(0)).await.is_some());
}

#[tokio::test(start_paused = true)]
async fn every_event_but_a_push_passes_a_full_window_and_holds_nothing() {
    let window = Window::new(host());
    let _all = admitted(&window, &json(1_000))
        .await
        .expect("room for one push alone");
    let others = [
        SourceEvent::Checkpoint {
            cursor: Cursor::new(1, b"c").expect("a small cursor"),
            answers: Some(1),
        },
        SourceEvent::Replan,
        SourceEvent::Behind {
            records: 7,
            headroom: Headroom::default(),
        },
    ];
    for event in others {
        let admitted = admitted(&window, &event).await;
        assert!(
            matches!(admitted, Some(None)),
            "{event:?} passes at once, holding nothing"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_frame_limit_beyond_what_a_semaphore_takes_at_once_is_a_window_of_that_much() {
    let window = Window::new(Limits {
        frame_bytes: u64::MAX,
        ..Limits::default()
    });
    assert_eq!(window.bytes, u32::MAX);
    assert!(admitted(&window, &json(48)).await.is_some());
}

#[test]
fn a_window_admits_within_its_host_s_limits_and_charges_nothing_a_read_keeps() {
    let window = Window::new(host());
    assert_eq!(window.limits(), host());
    assert!(window.charge(u64::MAX).is_ok());
}

/// Receives `feed`'s events as the engine does, each `SLOW` after the last, handing them on.
fn engine(mut feed: PartitionFeed) -> mpsc::UnboundedReceiver<SourceEvent> {
    let (engine, received) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SLOW).await;
            let Some(event) = feed.recv().await else {
                return;
            };
            if engine.send(event).is_err() {
                return;
            }
        }
    });
    received
}

/// A sink whose window a push the engine takes `SLOW` from now fills.
struct Sending {
    sink: PartitionSink,
    window: Arc<Window>,
    events: mpsc::UnboundedReceiver<SourceEvent>,
}

async fn sending() -> Sending {
    let window = Arc::new(Window::new(host()));
    let admission: Arc<dyn Admission> = Arc::clone(&window) as _;
    let channel = NonZeroUsize::new(16).expect("not zero");
    let (mut sink, feed) = admitted_partition_channel(channel, admission);
    sink.send(json(1_000)).await.expect("the push goes");
    Sending {
        sink,
        window,
        events: engine(feed),
    }
}

#[test]
fn a_send_cut_while_it_waits_for_room_leaves_the_window_whole_and_goes_once_retried() {
    let points = cancel::at_every_point(
        async || sending().await,
        async |sending: &mut Sending| sending.sink.send(json(48)).await,
        async |sending: &mut Sending, ended: Ended<crate::Result<()>>| {
            match ended {
                Ended::Returned(sent) => sent.expect("the push goes"),
                Ended::Cancelled { .. } => {
                    tokio::time::timeout(WAIT, sending.sink.send(json(48)))
                        .await
                        .expect("the retry ends")
                        .expect("the retry goes");
                }
            }
            for expected in [json(1_000), json(48)] {
                let event = tokio::time::timeout(WAIT, sending.events.recv()).await;
                assert_eq!(event.expect("an event arrives"), Some(expected));
            }
            tokio::time::sleep(WAIT).await;
            assert!(sending.events.try_recv().is_err(), "no push arrives twice");
            // Every byte of the window came back: a push that takes all of it goes at once.
            assert!(admitted(&sending.window, &json(1_000)).await.is_some());
        },
    );
    // Room in the window.
    assert_eq!(points, 1);
}

#[tokio::test(start_paused = true)]
async fn a_read_stopped_while_its_push_waits_for_room_ends_stopped() {
    let window: Arc<dyn Admission> = Arc::new(Window::new(host()));
    let channel = NonZeroUsize::new(16).expect("not zero");
    let (mut sink, feed) = admitted_partition_channel(channel, window);
    sink.send(json(1_000)).await.expect("the push goes");
    let waiting = tokio::spawn(async move { sink.send(json(48)).await });
    tokio::time::sleep(WAIT).await;
    assert!(!waiting.is_finished(), "the push waits for room");
    feed.stop();
    let sent = tokio::time::timeout(WAIT, waiting)
        .await
        .expect("the send ends")
        .expect("the send does not panic");
    assert_eq!(
        sent.expect_err("a stopped read sends nothing").kind(),
        ConnectorErrorKind::Stopped
    );
}

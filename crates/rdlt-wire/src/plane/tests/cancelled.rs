//! A call's reader, and the bounded body beneath it, cut at every point they wait and then read
//! on.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Context;
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body as _, Frame};
use rdlt_testkit::cancel::{self, Ended};
use tonic::Status;

use super::{credit, prefixed};
use crate::bounded::{Bounded, Bounds, Charge, Charging, Held, Window};
use crate::limits::{Class, Limits};
use crate::plane::{Incoming, PREFIX};
use crate::testing::fed;
use crate::v1;

/// How long each piece of the call takes to arrive, and each charge to be given.
const PACE: Duration = Duration::from_millis(10);

/// Bounds every wait of a check.
const WAIT: Duration = Duration::from_secs(60);

/// The messages the call carries.
fn carried() -> [v1::WriteAck; 3] {
    [credit(1), credit(2), credit(3)]
}

/// One of a paced charge's holds, counted until it is dropped.
struct Hold(Arc<AtomicUsize>);

impl Drop for Hold {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Charges each message `PACE` after it asks, counting the charges held.
struct Paced(Arc<AtomicUsize>);

impl Charge for Paced {
    fn charge(&self, _: Class, _: usize) -> Charging {
        let held = Arc::clone(&self.0);
        Box::pin(async move {
            tokio::time::sleep(PACE).await;
            held.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Hold(held)) as Held)
        })
    }
}

/// A call's reader, the window its body takes room in, and the charges it holds.
struct Reading {
    incoming: Incoming<v1::WriteAck>,
    window: Window,
    held: Arc<AtomicUsize>,
}

/// A reader of a call whose messages each arrive in two pieces, `PACE` apart, through a window of
/// one message that another body holds whole for its first `3 × PACE`.
fn reading() -> Reading {
    let bounds = Bounds::of(
        &Limits::default(),
        Class::of_answer("Write"),
        crate::scan::response("Write"),
    );
    let messages = carried().map(|message| prefixed(&message));
    let window = Window::new(messages[0].len());
    let (holding_feed, holding) = fed();
    let prefix = Bytes::copy_from_slice(&messages[0][..PREFIX]);
    holding_feed
        .send(Frame::data(prefix))
        .expect("the body is held");
    let mut holding = Bounded::new(holding, bounds, Some(window.clone()));
    let mut context = Context::from_waker(std::task::Waker::noop());
    let polled = Pin::new(&mut holding).poll_frame(&mut context);
    assert!(
        polled.is_pending(),
        "the other body waits for the rest of its message"
    );
    assert_eq!(
        window.taken(),
        messages[0].len(),
        "the other body holds all the room"
    );
    tokio::spawn(async move {
        tokio::time::sleep(3 * PACE).await;
        drop((holding, holding_feed));
    });
    let (feed, body) = fed();
    tokio::spawn(async move {
        for message in messages {
            let (head, tail) = message.split_at(3);
            for piece in [head, tail] {
                tokio::time::sleep(PACE).await;
                if feed
                    .send(Frame::data(Bytes::copy_from_slice(piece)))
                    .is_err()
                {
                    return;
                }
            }
        }
    });
    let held = Arc::new(AtomicUsize::new(0));
    let charge = Arc::new(Paced(Arc::clone(&held))) as Arc<dyn Charge>;
    let bounded = Bounded::new(body, bounds, Some(window.clone())).charged(Some(charge));
    Reading {
        incoming: Incoming::request(bounded),
        window,
        held,
    }
}

/// Reads on after a read cut at `ended`, or run to its end, and holds what was read to the
/// messages the call carries.
async fn read_on(reading: &mut Reading, ended: Ended<Result<Option<v1::WriteAck>, Status>>) {
    let mut read = Vec::new();
    if let Ended::Returned(first) = ended {
        read.push(first.expect("the message decodes").expect("a message"));
    }
    // One read more than the call carries messages: its end.
    for _ in 0..=carried().len() {
        reading.incoming.release();
        let next = tokio::time::timeout(WAIT, reading.incoming.message())
            .await
            .expect("the reader goes on")
            .expect("each message decodes");
        let Some(message) = next else {
            break;
        };
        read.push(message);
    }
    assert_eq!(read, carried(), "every message once, in order");
    assert_eq!(
        reading.window.taken(),
        0,
        "every message gave its room back"
    );
    assert_eq!(
        reading.held.load(Ordering::SeqCst),
        0,
        "every charge was released"
    );
}

#[test]
fn a_reader_cut_wherever_it_waits_reads_each_message_once_and_gives_its_room_back() {
    let points = cancel::at_every_point(
        async || reading(),
        async |reading: &mut Reading| reading.incoming.message().await,
        async |reading: &mut Reading, ended: Ended<Result<Option<v1::WriteAck>, Status>>| {
            read_on(reading, ended).await;
        },
    );
    // The first message's two pieces, its room, and its charge.
    assert_eq!(points, 4);
}

use std::sync::Arc;

use bytes::Bytes;
use http_body::Frame;
use parking_lot::Mutex;
use prost::Message as _;

use super::{Bounded, Bounds, PREFIX, Passed, fed, message, poll, poll_arrived};
use crate::aligned::Spare;
use crate::bounded::{Charge, Charging, Held};
use crate::limits::Class;
use crate::scan::response;
use crate::v1;

/// Charges nothing, keeps the last spare it is given, and gives it to a message it fits,
/// recording the room each message asked for.
#[derive(Default)]
struct Keeper {
    kept: Mutex<Option<Spare>>,
    asked: Mutex<Vec<usize>>,
}

impl Charge for Keeper {
    fn charge(&self, _class: Class, _bytes: usize) -> Charging {
        Box::pin(async { Ok(Box::new(()) as Held) })
    }

    fn keep_spare(&self, spare: Spare) {
        *self.kept.lock() = Some(spare);
    }

    fn take_spare(&self, room: usize) -> Option<Spare> {
        self.asked.lock().push(room);
        let mut kept = self.kept.lock();
        kept.as_ref()
            .is_some_and(|spare| spare.fits(room))
            .then(|| kept.take())
            .flatten()
    }
}

impl Keeper {
    /// Bytes: what the spare it keeps takes, where it keeps one.
    fn kept(&self) -> Option<usize> {
        self.kept.lock().as_ref().map(Spare::bytes)
    }
}

/// The bounds of a read's answers.
fn frames() -> Bounds {
    Bounds {
        class: Class::Data,
        form: response("Read"),
        wire: 1 << 20,
        decoded: 1 << 22,
    }
}

/// A read's frame carrying `len` bytes of JSON text, each `fill`, as a message on the wire.
fn frame(len: usize, fill: u8) -> Vec<u8> {
    let json = v1::JsonFrame {
        data: Bytes::from(vec![fill; len]),
    };
    let frame = v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Json(json)),
    };
    message(&frame.encode_to_vec())
}

/// A body of `bounds` charged to `keeper`, and what feeds it.
fn kept(bounds: Bounds, keeper: &Arc<Keeper>) -> (crate::testing::Feed, Bounded) {
    let (feed, body) = fed();
    let charge = Arc::clone(keeper) as Arc<dyn Charge>;
    (feed, Bounded::new(body, bounds, None).charged(Some(charge)))
}

#[test]
fn a_frame_arrives_in_the_allocation_of_one_before_once_nothing_holds_it() {
    let keeper = Arc::new(Keeper::default());
    let (feed, mut body) = kept(frames(), &keeper);
    let (first, second, third) = (frame(1000, 1), frame(1000, 2), frame(1000, 3));
    feed.send(Frame::data(first.clone().into())).unwrap();
    let arrived = poll_arrived(&mut body);
    assert_eq!(&arrived.bytes[..], &first[..]);
    let (start, len) = (arrived.allocation.as_ptr().addr(), arrived.allocation.len());
    // Held still, the first's allocation is no spare: the second arrives in one of its own.
    feed.send(Frame::data(second.clone().into())).unwrap();
    let held = poll_arrived(&mut body);
    assert_eq!(&held.bytes[..], &second[..]);
    assert_ne!(held.allocation.as_ptr().addr(), start);
    let slice = arrived.bytes.slice(100..200);
    drop(arrived);
    assert_eq!(keeper.kept(), None, "a slice of the first still holds it");
    drop(slice);
    assert_eq!(
        keeper.kept(),
        Some(len),
        "the first's whole allocation, kept"
    );
    // The third arrives split within its prefix, and takes the spare before more is copied.
    let (part, rest) = third.split_at(3);
    feed.send(Frame::data(Bytes::copy_from_slice(part)))
        .unwrap();
    feed.send(Frame::data(Bytes::copy_from_slice(rest)))
        .unwrap();
    let again = poll_arrived(&mut body);
    assert_eq!(&again.bytes[..], &third[..]);
    assert_eq!(
        again.allocation.as_ptr().addr(),
        start,
        "the first's allocation"
    );
    assert_eq!(again.allocation.len(), len);
    assert_eq!(keeper.kept(), None, "taken");
    // The fourth's prefix arrives alone, and the rest after a wait.
    drop(again);
    let fourth = frame(1000, 4);
    feed.send(Frame::data(Bytes::copy_from_slice(&fourth[..PREFIX])))
        .unwrap();
    assert_eq!(poll(&mut body), Passed::Waits);
    feed.send(Frame::data(Bytes::copy_from_slice(&fourth[PREFIX..])))
        .unwrap();
    let last = poll_arrived(&mut body);
    assert_eq!(&last.bytes[..], &fourth[..]);
    assert_eq!(
        last.allocation.as_ptr().addr(),
        start,
        "the first's allocation"
    );
    assert_eq!(
        *keeper.asked.lock(),
        [first.len(), second.len(), third.len(), fourth.len()],
        "once a frame, for its whole length"
    );
}

#[test]
fn a_frame_a_spare_does_not_fit_arrives_in_an_allocation_of_its_own() {
    let keeper = Arc::new(Keeper::default());
    let (feed, mut body) = kept(frames(), &keeper);
    feed.send(Frame::data(frame(1000, 1).into())).unwrap();
    let first = poll_arrived(&mut body);
    let start = first.allocation.as_ptr().addr();
    drop(first);
    let spare = keeper.kept().expect("kept");
    let mut held = Vec::new();
    for other in [frame(400, 2), frame(1100, 3)] {
        feed.send(Frame::data(other.clone().into())).unwrap();
        let arrived = poll_arrived(&mut body);
        assert_eq!(&arrived.bytes[..], &other[..]);
        assert_ne!(arrived.allocation.as_ptr().addr(), start);
        assert_eq!(keeper.kept(), Some(spare), "the spare stays");
        held.push(arrived);
    }
}

#[test]
fn an_answer_that_holds_no_frames_keeps_no_allocation() {
    let keeper = Arc::new(Keeper::default());
    let catalog = Bounds {
        class: Class::Catalog,
        form: response("Discover"),
        ..frames()
    };
    let (feed, mut body) = kept(catalog, &keeper);
    feed.send(Frame::data(message(&[0x0a, 0x00]).into()))
        .unwrap();
    drop(poll_arrived(&mut body));
    assert_eq!(keeper.kept(), None);
    assert!(keeper.asked.lock().is_empty());
}

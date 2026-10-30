mod charged;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use proptest::prelude::*;
use tonic::Code;

use super::{Arrived, Bounded, Bounds, PREFIX, Whole, Window};
use crate::aligned::{ALIGNMENT, Aligned, is_aligned};
use crate::scan::response;
pub(super) use crate::testing::{chunks, fed, message};

fn bounds(wire: usize, decoded: usize) -> Bounds {
    Bounds {
        class: crate::limits::Class::Catalog,
        form: response("Discover"),
        wire,
        decoded,
    }
}

/// What `body` passes on when polled once: a message, the trailers, the end, an error's code,
/// or that it waits.
#[derive(Debug, PartialEq, Eq)]
enum Passed {
    Data(Vec<u8>),
    Trailers,
    End,
    Failed(Code),
    Waits,
}

fn poll(body: &mut Bounded) -> Passed {
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    match Pin::new(body).poll_frame(&mut context) {
        Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
            Ok(data) => Passed::Data(data.to_vec()),
            Err(_) => Passed::Trailers,
        },
        Poll::Ready(Some(Err(status))) => Passed::Failed(status.code()),
        Poll::Ready(None) => Passed::End,
        Poll::Pending => Passed::Waits,
    }
}

/// The message `body` passes on when polled once, as it arrived.
fn poll_arrived(body: &mut Bounded) -> Arrived {
    poll_sized(body, &|whole| whole.decoded).expect("a message")
}

/// The message `body` passes on when polled once, as it arrived, charged what `size` says;
/// `None` where it waits.
fn poll_sized(body: &mut Bounded, size: &dyn Fn(&Whole<'_>) -> usize) -> Option<Arrived> {
    let waker = std::task::Waker::noop();
    match body.poll_arrived(&mut Context::from_waker(waker), size) {
        Poll::Ready(Some(Ok(frame))) => Some(frame.into_data().expect("a message")),
        Poll::Pending => None,
        other @ Poll::Ready(_) => panic!("a message, not {other:?}"),
    }
}

/// Everything `body` passes on until it ends, fails or waits.
fn passed(mut body: Bounded) -> Vec<Passed> {
    let mut passed = Vec::new();
    loop {
        let next = poll(&mut body);
        let last = !matches!(next, Passed::Data(_) | Passed::Trailers);
        passed.push(next);
        if last {
            return passed;
        }
    }
}

#[test]
fn the_body_says_it_has_ended_only_once_all_it_holds_is_passed_on() {
    // A caller stops polling a body that says it has ended: it says so once nothing it holds
    // waits to be passed on, and not before.
    let (first, second) = (message(&[0x0a, 0x00]), message(&[]));
    let (feed, body) = fed();
    feed.send(Frame::data([first.clone(), second.clone()].concat().into()))
        .unwrap();
    drop(feed);
    let mut body = Bounded::new(body, bounds(1024, 1 << 20), None);
    assert!(!body.is_end_stream(), "nothing has been read");
    assert_eq!(poll(&mut body), Passed::Data(first));
    assert!(!body.is_end_stream(), "a message waits to be passed on");
    assert_eq!(poll(&mut body), Passed::Data(second));
    assert_eq!(poll(&mut body), Passed::End);
    assert!(body.is_end_stream(), "all of it was passed on");
    let mut ended = Bounded::new(chunks(&[]), bounds(1024, 1 << 20), None);
    assert_eq!(poll(&mut ended), Passed::End);
    assert!(ended.is_end_stream());
}

#[test]
fn a_message_is_passed_on_once_it_has_arrived_whole() {
    let whole = message(&[0x0a, 0x00, 0x0a, 0x00]);
    let body = chunks(&[&whole[..3], &whole[3..7], &whole[7..]]);
    let passed = passed(Bounded::new(body, bounds(1024, 1 << 20), None));
    assert_eq!(passed, [Passed::Data(whole), Passed::End]);
}

#[test]
fn messages_arriving_together_are_passed_on_each_whole() {
    let (first, second) = (message(&[0x0a, 0x00]), message(&[]));
    let joined = [first.clone(), second.clone(), second[..2].to_vec()].concat();
    let (feed, body) = fed();
    feed.send(Frame::data(joined.into())).unwrap();
    let mut body = Bounded::new(body, bounds(1024, 1 << 20), None);
    assert_eq!(poll(&mut body), Passed::Data(first));
    assert_eq!(poll(&mut body), Passed::Data(second.clone()));
    assert_eq!(
        poll(&mut body),
        Passed::Waits,
        "the third has not arrived whole"
    );
    feed.send(Frame::data(Bytes::copy_from_slice(&second[2..])))
        .unwrap();
    assert_eq!(poll(&mut body), Passed::Data(second));
}

#[test]
fn a_message_longer_than_its_wire_bound_is_refused_from_its_prefix() {
    let declared = message(&[0; 11]);
    let refused = Bounded::new(chunks(&[&declared[..5]]), bounds(10, usize::MAX), None);
    assert_eq!(passed(refused), [Passed::Failed(Code::OutOfRange)]);
    let at = message(&[0x0a, 0x00].repeat(5));
    let taken = Bounded::new(chunks(&[&at]), bounds(10, usize::MAX), None);
    assert_eq!(passed(taken), [Passed::Data(at), Passed::End]);
}

#[test]
fn a_message_holding_more_decoded_than_its_bound_is_refused() {
    let payload = [0x0a, 0x00].repeat(4);
    let held = crate::scan::decoded(response("Discover").unwrap(), &payload, usize::MAX).unwrap();
    let whole = message(&payload);
    let refused = Bounded::new(chunks(&[&whole]), bounds(1024, held - 1), None);
    assert_eq!(passed(refused), [Passed::Failed(Code::OutOfRange)]);
    let taken = Bounded::new(chunks(&[&whole]), bounds(1024, held), None);
    assert_eq!(passed(taken), [Passed::Data(whole.clone()), Passed::End]);
}

#[test]
fn a_message_the_scan_cannot_walk_is_refused_never_passed_on() {
    let least = size_of::<crate::v1::Catalog>();
    for payload in [&[0x0b][..], &[0x0c], &[0x0a, 0x05]] {
        let body = chunks(&[&message(payload)]);
        let refused = Bounded::new(body, bounds(1024, least * 100), None);
        assert_eq!(
            passed(refused),
            [Passed::Failed(Code::InvalidArgument)],
            "{payload:?}"
        );
    }
}

#[test]
fn a_message_the_body_ends_within_is_refused_never_passed_on() {
    let whole = message(&[0x0a, 0x00]);
    for cut in [1, 4, 5, 6] {
        let body = Bounded::new(chunks(&[&whole[..cut]]), bounds(1024, 1 << 20), None);
        assert_eq!(passed(body), [Passed::Failed(Code::Internal)], "{cut}");
    }
}

#[test]
fn trailers_are_passed_on_after_the_messages_before_them() {
    let whole = message(&[0x0a, 0x00]);
    let (feed, body) = fed();
    feed.send(Frame::data(whole.clone().into())).unwrap();
    feed.send(Frame::trailers(tonic::codegen::http::HeaderMap::new()))
        .unwrap();
    drop(feed);
    let passed = passed(Bounded::new(body, bounds(1024, 1 << 20), None));
    assert_eq!(passed, [Passed::Data(whole), Passed::Trailers, Passed::End]);
}

/// Bodies each fed one message of `payload`, in pieces of `piece`, in turns, over a window of
/// `messages` such messages: what each passed on, and the most room the window gave.
fn in_turns(bodies: usize, payload: &[u8], piece: usize, messages: usize) -> (Vec<bool>, usize) {
    let whole = message(payload);
    let window = Window::new(whole.len() * messages);
    let mut feeds = Vec::new();
    let mut bounded = Vec::new();
    for _ in 0..bodies {
        let (feed, body) = fed();
        feeds.push((feed, whole.chunks(piece).collect::<Vec<_>>().into_iter()));
        let bounds = bounds(whole.len(), usize::MAX);
        bounded.push(Bounded::new(body, bounds, Some(window.clone())));
    }
    let (mut done, mut most) = (vec![false; bodies], 0);
    for _ in 0..whole.len() * bodies {
        for (index, ((feed, pieces), body)) in feeds.iter_mut().zip(&mut bounded).enumerate() {
            if done[index] {
                continue;
            }
            // A body is fed only once it has read all it was fed: one that waits holds its sender.
            match poll(body) {
                Passed::Data(passed) => {
                    assert_eq!(passed, whole);
                    done[index] = true;
                }
                Passed::Waits => {
                    if let Some(piece) = pieces.next() {
                        feed.send(Frame::data(Bytes::copy_from_slice(piece)))
                            .unwrap();
                    }
                }
                other => panic!("{other:?}"),
            }
            most = most.max(window.taken());
        }
    }
    (done, most)
}

#[test]
fn bodies_finding_no_room_are_held_until_room_comes_back_and_none_is_refused() {
    let payload = [0x0a, 0x00].repeat(64);
    let whole = message(&payload).len();
    // Five writers of a message each, four messages' room.
    let (done, most) = in_turns(5, &payload, 16, 4);
    assert_eq!(done, [true; 5]);
    assert!(most <= 4 * whole, "{most} of {}", 4 * whole);
    assert_eq!(most, 4 * whole, "the window was full");
}

#[test]
fn a_message_behind_messages_holding_the_window_is_passed_on_once_they_are() {
    let write = message(&[0x0a, 0x00].repeat(64));
    let commit = message(&[0x0a, 0x00]);
    let window = Window::new(2 * write.len());
    let bounds = bounds(write.len(), usize::MAX);
    let mut writes = Vec::new();
    for _ in 0..2 {
        let (feed, body) = fed();
        let mut body = Bounded::new(body, bounds, Some(window.clone()));
        feed.send(Frame::data(Bytes::copy_from_slice(&write[..10])))
            .unwrap();
        assert_eq!(poll(&mut body), Passed::Waits);
        writes.push((feed, body));
    }
    assert_eq!(window.taken(), 2 * write.len());
    let (feed, body) = fed();
    let mut committing = Bounded::new(body, bounds, Some(window.clone()));
    feed.send(Frame::data(commit.clone().into())).unwrap();
    assert_eq!(
        poll(&mut committing),
        Passed::Waits,
        "no room behind the writes"
    );
    // The writes' messages arrive, as their senders keep sending, and give their room back.
    for (feed, body) in &mut writes {
        feed.send(Frame::data(Bytes::copy_from_slice(&write[10..])))
            .unwrap();
        assert_eq!(poll(body), Passed::Data(write.clone()));
    }
    assert_eq!(poll(&mut committing), Passed::Data(commit));
    drop(writes);
    assert_eq!(window.taken(), 0);
}

/// A body still receiving a message of `held`, holding its room in `window`, and what feeds it.
fn holding(window: &Window, bounds: Bounds, held: &[u8]) -> (crate::testing::Feed, Bounded) {
    let (feed, body) = fed();
    let mut body = Bounded::new(body, bounds, Some(window.clone()));
    feed.send(Frame::data(Bytes::copy_from_slice(&held[..10])))
        .unwrap();
    assert_eq!(poll(&mut body), Passed::Waits);
    (feed, body)
}

#[test]
fn a_body_waiting_for_room_holds_none_of_its_message() {
    let held = message(&[0x0a, 0x00].repeat(64));
    let waiting = message(&[0x0a, 0x00].repeat(8));
    // Room for one message, which a body still receiving holds.
    let window = Window::new(held.len());
    let bounds = bounds(held.len(), usize::MAX);
    let (holding_feed, mut holder) = holding(&window, bounds, &held);
    assert_eq!(window.taken(), held.len());
    // One transport frame brings the second body's whole message.
    let (feed, body) = fed();
    let mut body = Bounded::new(body, bounds, Some(window.clone()));
    feed.send(Frame::data(waiting.clone().into())).unwrap();
    assert_eq!(poll(&mut body), Passed::Waits, "no room");
    assert_eq!(
        body.arriving.len(),
        0,
        "nothing of the message is copied before its room"
    );
    assert_eq!(body.arriving.room(), 0, "nor is room for it allocated");
    assert_eq!(
        body.unread.len(),
        waiting.len(),
        "the frame waits as it came"
    );
    assert_eq!(window.taken(), held.len());
    holding_feed
        .send(Frame::data(Bytes::copy_from_slice(&held[10..])))
        .unwrap();
    assert_eq!(poll(&mut holder), Passed::Data(held));
    assert_eq!(poll(&mut body), Passed::Data(waiting));
    drop((holder, body));
    assert_eq!(window.taken(), 0);
}

#[test]
fn a_message_behind_one_passed_on_from_its_chunk_takes_room_before_it_is_copied() {
    let held = message(&[0x0a, 0x00].repeat(64));
    let (first, second) = (message(&[0x0a, 0x00]), message(&[0x0a, 0x00, 0x0a, 0x00]));
    // Room for two of the largest messages: one held by a body still receiving, one free.
    let window = Window::new(2 * held.len());
    let bounds = bounds(held.len(), usize::MAX);
    let (holding_feed, mut holder) = holding(&window, bounds, &held);
    // One transport frame brings two whole messages; the first has room and is passed on.
    let (feed, body) = fed();
    let mut body = Bounded::new(body, bounds, Some(window.clone()));
    feed.send(Frame::data([first.clone(), second.clone()].concat().into()))
        .unwrap();
    assert_eq!(poll(&mut body), Passed::Data(first));
    // Another body takes the room the first gave back.
    let (other_feed, mut other) = holding(&window, bounds, &held);
    assert_eq!(window.taken(), 2 * held.len());
    assert_eq!(poll(&mut body), Passed::Waits, "no room for the second");
    assert_eq!(body.arriving.len(), 0, "the second is not copied");
    assert_eq!(body.unread.len(), second.len());
    holding_feed
        .send(Frame::data(Bytes::copy_from_slice(&held[10..])))
        .unwrap();
    assert_eq!(poll(&mut holder), Passed::Data(held.clone()));
    assert_eq!(poll(&mut body), Passed::Data(second));
    other_feed
        .send(Frame::data(Bytes::copy_from_slice(&held[10..])))
        .unwrap();
    assert_eq!(poll(&mut other), Passed::Data(held));
    drop((holder, body, other));
    assert_eq!(window.taken(), 0);
}

#[test]
fn room_for_an_arriving_message_doubles_then_goes_to_its_end() {
    let mut body = Bounded::new(chunks(&[]), bounds(1024, 1 << 20), None);
    let prefix = message(&[0; 100]);
    let mut rooms = Vec::new();
    // Before its prefix the length is unknown: room is what arrived. Then it doubles, until the
    // message's end is no more than twice as far, and then it is the end.
    for (from, to) in [(0, 3), (3, 5), (5, 15), (15, 35), (35, 45), (45, 105)] {
        body.arrive(&prefix[from..to]).expect("within its bound");
        rooms.push(body.arriving.room());
    }
    assert_eq!(rooms, [3, 5, 15, 35, 105, 105]);
    // Room that is full takes nothing more for nothing more.
    let mut full = Bounded::new(chunks(&[]), bounds(1024, 1 << 20), None);
    full.arrive(&prefix[..5]).expect("within its bound");
    full.arrive(&prefix[5..15]).expect("within its bound");
    full.arrive(&[]).expect("nothing");
    assert_eq!(full.arriving.room(), 15);
}

#[test]
fn a_body_shows_its_bounds_and_what_has_arrived() {
    let mut body = Bounded::new(chunks(&[]), bounds(1024, 1 << 20), None);
    body.arrive(&[0, 0]).expect("within its bound");
    let shown = format!("{body:?}");
    assert!(shown.starts_with("Bounded {"), "{shown}");
    assert!(
        shown.contains("wire: 1024") && shown.contains("arriving: 2"),
        "{shown}"
    );
}

/// A message of `pairs` empty catalog entries, which its scan walks.
fn entries(pairs: usize) -> Vec<u8> {
    message(&[0x0a, 0x00].repeat(pairs))
}

#[test]
fn a_chunk_of_many_messages_gives_each_an_allocation_of_its_own_length() {
    // One transport chunk: three whole messages of 10, 1000 and 0 bytes, and the start of a
    // fourth of 200, which ends in a second chunk.
    let messages: Vec<Vec<u8>> = [5, 500, 0, 100].into_iter().map(entries).collect();
    let mut joined: Vec<u8> = messages.concat();
    let second = joined.split_off(joined.len() - 150);
    let mut body = Bounded::new(chunks(&[&joined, &second]), bounds(4_096, 1 << 20), None);
    for message in &messages {
        let arrived = poll_arrived(&mut body);
        assert_eq!(&arrived.bytes[..], &message[..]);
        assert!(is_aligned(&arrived.bytes));
        // Its allocation holds it and the alignment's room, never the message after it.
        assert!(
            arrived.allocation.len() < message.len() + ALIGNMENT,
            "{} for {}",
            arrived.allocation.len(),
            message.len()
        );
    }
    assert_eq!(poll(&mut body), Passed::End);
}

#[test]
fn a_message_split_at_every_byte_arrives_aligned_and_whole() {
    let message = entries(128);
    for at in 0..=message.len() {
        let (first, second) = message.split_at(at);
        let mut body = Bounded::new(chunks(&[first, second]), bounds(4_096, 1 << 20), None);
        let arrived = poll_arrived(&mut body);
        assert_eq!(&arrived.bytes[..], &message[..], "{at}");
        assert!(is_aligned(&arrived.bytes), "{at}");
        assert!(arrived.allocation.len() < message.len() + ALIGNMENT, "{at}");
        assert_eq!(poll(&mut body), Passed::End, "{at}");
    }
}

#[test]
fn a_message_s_prefix_waiting_whole_is_held_to_its_bound() {
    // The prefix of the second message waits behind the first, and declares more than the bound.
    let at = message(&[0x0a, 0x00].repeat(5));
    let declared = message(&[0; 11]);
    let joined = [at.clone(), declared[..5].to_vec()].concat();
    let body = Bounded::new(chunks(&[&joined]), bounds(10, usize::MAX), None);
    assert_eq!(
        passed(body),
        [Passed::Data(at), Passed::Failed(Code::OutOfRange)]
    );
}

#[test]
fn a_body_waiting_for_room_holds_no_more_than_a_prefix_split_across_frames() {
    let held = message(&[0x0a, 0x00].repeat(64));
    let waiting = message(&[0x0a, 0x00].repeat(8));
    let window = Window::new(held.len());
    let bounds = bounds(held.len(), usize::MAX);
    let (holding_feed, mut holder) = holding(&window, bounds, &held);
    let (feed, body) = fed();
    let mut body = Bounded::new(body, bounds, Some(window.clone()));
    for (from, to) in [(0, 3), (3, waiting.len())] {
        feed.send(Frame::data(Bytes::copy_from_slice(&waiting[from..to])))
            .unwrap();
        assert_eq!(poll(&mut body), Passed::Waits, "{from}..{to}");
        assert!(body.arriving.len() <= PREFIX, "{}", body.arriving.len());
        assert_eq!(window.taken(), held.len());
    }
    assert_eq!(
        body.arriving.len(),
        PREFIX,
        "the prefix, and nothing past it"
    );
    holding_feed
        .send(Frame::data(Bytes::copy_from_slice(&held[10..])))
        .unwrap();
    assert_eq!(poll(&mut holder), Passed::Data(held));
    assert_eq!(poll(&mut body), Passed::Data(waiting));
}

#[test]
fn a_body_dropped_while_waiting_for_room_leaves_the_window_to_the_others() {
    let held = message(&[0x0a, 0x00].repeat(64));
    let waiting = message(&[0x0a, 0x00].repeat(8));
    let left = 3;
    let window = Window::new(held.len() + left);
    let bounds = bounds(held.len(), usize::MAX);
    let (holding_feed, mut holder) = holding(&window, bounds, &held);
    let (feed, body) = fed();
    let mut dropped = Bounded::new(body, bounds, Some(window.clone()));
    feed.send(Frame::data(waiting.clone().into())).unwrap();
    assert_eq!(poll(&mut dropped), Passed::Waits);
    assert_eq!(
        window.taken(),
        held.len() + left,
        "its wait gathers the room left"
    );
    drop(dropped);
    assert_eq!(window.taken(), held.len(), "the room it gathered came back");
    holding_feed
        .send(Frame::data(Bytes::copy_from_slice(&held[10..])))
        .unwrap();
    assert_eq!(poll(&mut holder), Passed::Data(held));
    assert_eq!(window.taken(), 0);
    let later = Bounded::new(chunks(&[&waiting]), bounds, Some(window.clone()));
    assert_eq!(passed(later), [Passed::Data(waiting), Passed::End]);
    assert_eq!(window.taken(), 0);
}

/// A waker that records that it was woken.
#[derive(Default)]
struct Woken(AtomicBool);

impl std::task::Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn a_body_waiting_for_room_is_woken_once_room_comes_back() {
    let held = message(&[0x0a, 0x00].repeat(64));
    let waiting = message(&[0x0a, 0x00].repeat(8));
    let window = Window::new(held.len());
    let bounds = bounds(held.len(), usize::MAX);
    let (holding_feed, mut holder) = holding(&window, bounds, &held);
    let (feed, body) = fed();
    let mut body = Bounded::new(body, bounds, Some(window.clone()));
    feed.send(Frame::data(waiting.clone().into())).unwrap();
    let woken = Arc::new(Woken::default());
    let waker = std::task::Waker::from(Arc::clone(&woken));
    let polled = Pin::new(&mut body).poll_frame(&mut Context::from_waker(&waker));
    assert!(polled.is_pending());
    assert!(!woken.0.load(Ordering::SeqCst), "no room has come back");
    holding_feed
        .send(Frame::data(Bytes::copy_from_slice(&held[10..])))
        .unwrap();
    assert_eq!(poll(&mut holder), Passed::Data(held));
    assert!(
        woken.0.load(Ordering::SeqCst),
        "the room it waits for came back"
    );
    assert_eq!(poll(&mut body), Passed::Data(waiting));
}

#[test]
fn a_prefix_beyond_its_bound_behind_a_full_window_is_refused_at_once() {
    let held = message(&[0x0a, 0x00].repeat(64));
    let window = Window::new(held.len());
    let bounds = bounds(held.len(), usize::MAX);
    let (_holding_feed, _holder) = holding(&window, bounds, &held);
    // A prefix declaring a byte more than the wire bound, its message never sent.
    let declared = message(&vec![0; held.len() + 1]);
    let refused = Bounded::new(chunks(&[&declared[..PREFIX]]), bounds, Some(window.clone()));
    assert_eq!(passed(refused), [Passed::Failed(Code::OutOfRange)]);
    assert_eq!(window.taken(), held.len());
}

#[test]
fn a_message_longer_than_a_full_window_waits_for_the_whole_of_it() {
    let window = Window::new(10);
    let other = Arc::clone(&window.room)
        .try_acquire_many_owned(10)
        .expect("the whole window");
    // Thirteen bytes with its prefix: it takes the whole window, none of which is free.
    let waiting = message(&[0x0a, 0x00].repeat(4));
    let bounds = bounds(waiting.len(), usize::MAX);
    let mut body = Bounded::new(chunks(&[&waiting]), bounds, Some(window.clone()));
    assert_eq!(poll(&mut body), Passed::Waits);
    assert!(body.arriving.is_empty(), "nothing of it is copied");
    drop(other);
    assert_eq!(poll(&mut body), Passed::Data(waiting));
    assert_eq!(window.taken(), 0);
    assert_eq!(poll(&mut body), Passed::End);
}

/// A body over a call that sends nothing more, its messages held to `wire` bytes on the wire.
fn idle(wire: usize) -> Bounded {
    Bounded::new(tonic::body::Body::empty(), bounds(wire, usize::MAX), None)
}

/// The length the prefix at the start of `bytes` declares, where it is whole.
fn declared(bytes: &[u8]) -> Option<usize> {
    let prefix = bytes.get(..PREFIX)?;
    let length = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]);
    Some(usize::try_from(length).unwrap())
}

/// Wire bounds from nothing to the most there is, with those near the lengths below.
fn any_wire() -> impl Strategy<Value = usize> {
    prop_oneof![Just(0), Just(usize::MAX), 0..24_usize, any::<usize>()]
}

/// Up to sixteen bytes: any, or a prefix of a small or any length and some of its message.
fn any_bytes() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..=16),
        (
            any::<u8>(),
            prop_oneof![0..24_u32, any::<u32>()],
            proptest::collection::vec(any::<u8>(), 0..=11),
        )
            .prop_map(|(flag, length, rest)| {
                let mut bytes = vec![flag];
                bytes.extend(length.to_be_bytes());
                bytes.extend(rest);
                bytes
            }),
    ]
}

/// Whether `arriving`'s room keeps the growth rule: what arrived while the prefix is not whole,
/// and once it is, within the message's whole length and four times what arrived.
fn grown(arriving: &Aligned, declared: Option<usize>) -> bool {
    match declared {
        None => arriving.room() == arriving.len(),
        Some(length) => {
            arriving.room() <= PREFIX.saturating_add(length)
                && arriving.room() <= arriving.len().saturating_mul(4)
        }
    }
}

proptest! {
    /// A prefix read from the start of any bytes is nothing until it is whole, then the length it
    /// declares where that is within the wire bound, and a refusal beyond it.
    #[test]
    fn a_prefix_is_read_from_any_bytes_within_its_bound(
        wire in any_wire(),
        bytes in any_bytes(),
    ) {
        match (idle(wire).declared_in(&bytes), declared(&bytes)) {
            (Ok(None), None) => {}
            (Ok(Some(length)), Some(declared)) => {
                prop_assert_eq!(length, declared);
                prop_assert!(length <= wire);
            }
            (Err(status), Some(declared)) => {
                prop_assert!(declared > wire);
                prop_assert_eq!(status.code(), Code::OutOfRange);
            }
            _ => prop_assert!(false, "a prefix is read as nothing exactly while it is not whole"),
        }
    }

    /// What a body lacks of its arriving message never reaches past the message's end: the rest
    /// of its prefix, then the rest of the length the prefix declares, read from what has arrived
    /// or, before anything has, from what waits unread.
    #[test]
    fn what_an_arriving_message_lacks_ends_where_it_does(
        wire in any_wire(),
        arrived in any_bytes(),
        unread in any_bytes(),
    ) {
        let mut body = idle(wire);
        body.arriving.extend(&arrived);
        body.unread = Bytes::from(unread.clone());
        let known = declared(if arrived.is_empty() { &unread } else { &arrived });
        match body.wanted() {
            Ok(wanted) => {
                let end = known.map_or(PREFIX, |length| PREFIX + length);
                prop_assert_eq!(wanted, end.saturating_sub(arrived.len()));
                prop_assert!(known.is_none_or(|length| length <= wire));
            }
            Err(status) => {
                prop_assert!(known.is_some_and(|length| length > wire));
                prop_assert_eq!(status.code(), Code::OutOfRange);
            }
        }
    }

    /// One arrival of what a body lacks keeps its room within the growth rule and room enough
    /// for what arrived, and is refused only for a prefix beyond the wire bound.
    #[test]
    fn an_arriving_message_s_room_never_passes_its_length_or_four_times_what_arrived(
        wire in any_wire(),
        held in any_bytes(),
        slack in 0..96_usize,
        mut data in any_bytes(),
    ) {
        let length = declared(&held);
        // Any room the rule admits for what is held: from what is held up to its cap.
        let cap = match length {
            None => held.len(),
            Some(length) => PREFIX.saturating_add(length).min(held.len().saturating_mul(4)),
        };
        prop_assume!(cap >= held.len());
        let mut body = idle(wire);
        body.arriving = Aligned::with_room(held.len() + slack % (cap - held.len() + 1));
        body.arriving.extend(&held);
        prop_assert!(grown(&body.arriving, length));
        // What arrives is split from what waits unread, as a poll splits it.
        body.unread = Bytes::from(data.clone());
        let known = declared(if held.is_empty() { &data } else { &held });
        let wanted = match body.wanted() {
            Ok(wanted) => wanted,
            Err(status) => {
                prop_assert!(known.is_some_and(|length| length > wire));
                prop_assert_eq!(status.code(), Code::OutOfRange);
                return Ok(());
            }
        };
        data.truncate(wanted);
        prop_assert!(body.arrive(&data).is_ok());
        let all = [held.as_slice(), data.as_slice()].concat();
        prop_assert_eq!(body.arriving.as_slice(), all.as_slice());
        prop_assert!(body.arriving.room() >= body.arriving.len());
        prop_assert!(grown(&body.arriving, declared(&all)));
    }
}

/// The bytes of a transport frame: a prefix of `flag` declaring `length`, then `payload`, cut
/// to `cut` bytes.
fn transport_frame(flag: u8, length: u32, payload: &[u8], cut: usize) -> Vec<u8> {
    let mut frame = vec![flag];
    frame.extend(length.to_be_bytes());
    frame.extend(payload);
    frame.truncate(cut);
    frame
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(512)))]

    /// A message whose whole prefix waits takes room in its connection's window before anything
    /// past its prefix is copied, holds none of its bytes while it waits for room, goes on once
    /// room comes back, and gives its room back once it is passed on, or once its body is dropped.
    ///
    /// A waiting message is queued for the whole of its room and holds what is free of it so far,
    /// so the window counts that as taken, and gives it back with the body.
    #[test]
    fn room_is_taken_before_a_message_is_copied_and_given_back_in_balance(
        size in 0_usize..=96,
        held in 0_usize..=96,
        wire in prop_oneof![0_usize..96, Just(usize::MAX)],
        flag in any::<u8>(),
        length in prop_oneof![0_u32..96, Just(u32::MAX), any::<u32>()],
        payload in proptest::collection::vec(any::<u8>(), 0..96),
        cut in 0_usize..=101,
    ) {
        let window = Window::new(size);
        let held = u32::try_from(held.min(size)).expect("a small window");
        let mut other = Some(
            Arc::clone(&window.room)
                .try_acquire_many_owned(held)
                .expect("within the window"),
        );
        let others = window.taken();
        let bounds = Bounds { form: None, ..bounds(wire, usize::MAX) };
        let mut body = Bounded::new(chunks(&[]), bounds, Some(window.clone()));
        let frame = transport_frame(flag, length, &payload, cut);
        body.unread = Bytes::from(frame.clone());
        let declared = declared(&frame);
        // A message within its bound waits while the room it takes, its whole length or the
        // whole window where it is longer, is more than the window has free.
        let free = size - held as usize;
        let waits = declared.is_some_and(|length| {
            length <= wire && PREFIX.saturating_add(length).min(size) > free
        });
        let expected = match declared {
            None if frame.is_empty() => Passed::End,
            Some(length) if length > wire => Passed::Failed(Code::OutOfRange),
            Some(length) if frame.len() >= PREFIX + length => {
                Passed::Data(frame[..PREFIX + length].to_vec())
            }
            _ => Passed::Failed(Code::Internal),
        };
        if waits {
            prop_assert_eq!(poll(&mut body), Passed::Waits);
            prop_assert!(body.arriving.is_empty());
            prop_assert_eq!(body.arriving.room(), 0);
            prop_assert_eq!(&body.unread[..], &frame[..]);
            prop_assert!((others..=size).contains(&window.taken()));
            drop(other.take());
        }
        let delivered = matches!(expected, Passed::Data(_));
        prop_assert_eq!(poll(&mut body), expected);
        let others = if other.is_some() { others } else { 0 };
        if delivered {
            prop_assert_eq!(window.taken(), others);
        }
        drop(body);
        prop_assert_eq!(window.taken(), others);
        drop(other);
        prop_assert_eq!(window.taken(), 0);
    }
}

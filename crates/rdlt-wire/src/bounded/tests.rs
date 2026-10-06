mod charged;

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use tonic::Code;

use super::{Bounded, Bounds, Window};
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

#[test]
fn room_for_an_arriving_message_doubles_then_goes_to_its_end() {
    let mut body = Bounded::new(chunks(&[]), bounds(1024, 1 << 20), None);
    let prefix = message(&[0; 100]);
    let mut rooms = Vec::new();
    // Before its prefix the length is unknown: room is what arrived. Then it doubles, until the
    // message's end is no more than twice as far, and then it is the end.
    for (from, to) in [(0, 3), (3, 5), (5, 15), (15, 35), (35, 45), (45, 105)] {
        body.arrive(&prefix[from..to]).expect("within its bound");
        rooms.push(body.arriving.capacity());
    }
    assert_eq!(rooms, [3, 5, 15, 35, 105, 105]);
    // Room that is full takes nothing more for nothing more.
    let mut full = Bounded::new(chunks(&[]), bounds(1024, 1 << 20), None);
    full.arrive(&prefix[..5]).expect("within its bound");
    full.arrive(&prefix[5..15]).expect("within its bound");
    full.arrive(&[]).expect("nothing");
    assert_eq!(full.arriving.capacity(), 15);
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

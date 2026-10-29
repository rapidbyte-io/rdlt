use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf as _, Bytes};
use proptest::prelude::*;
use rdlt_testkit::cancel::{Ended, at_every_point};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{
    Bound, Bounds, Call, End, FrameReader, HEAD_BYTES, Head, HeadError, Kind, Malformed, Outgoing,
    Rows, StreamError,
};
use crate::aligned::is_aligned;
use crate::limits::{Limit, Limits};
use crate::v1;

/// An end that sends every kind of frame, each within `bytes`.
fn every(bytes: u32) -> Bounds {
    let bound = |limit| Bound { bytes, limit };
    Bounds {
        batch: Some(bound(Limit::FrameBytes)),
        json: Some(bound(Limit::JsonPushBytes)),
        message: bound(Limit::ControlMessageBytes),
    }
}

/// What a sender writes for `frames`, each the kind and pieces given.
fn sent(frames: &[(Kind, Vec<Bytes>)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (kind, pieces) in frames {
        let mut frame = Outgoing::new(*kind, pieces.clone()).expect("a short frame");
        while frame.has_remaining() {
            bytes.extend_from_slice(frame.chunk());
            let taken = frame.chunk().len();
            frame.advance(taken);
        }
    }
    bytes
}

/// A stream of `bytes` that arrive in the chunks `cuts` makes, each after a wait.
struct Trickle {
    chunks: VecDeque<Bytes>,
    waited: bool,
}

impl Trickle {
    fn new(bytes: &[u8], cuts: &[usize]) -> Self {
        let bytes = Bytes::copy_from_slice(bytes);
        let mut chunks = VecDeque::new();
        let mut at = 0;
        for cut in cuts {
            let end = (at + cut).min(bytes.len());
            chunks.push_back(bytes.slice(at..end));
            at = end;
        }
        chunks.push_back(bytes.slice(at..));
        Self {
            chunks,
            waited: false,
        }
    }
}

impl AsyncRead for Trickle {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.waited {
            self.waited = true;
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.waited = false;
        while let Some(chunk) = self.chunks.front_mut() {
            if chunk.is_empty() {
                self.chunks.pop_front();
                continue;
            }
            let taken = chunk.len().min(buffer.remaining());
            buffer.put_slice(&chunk[..taken]);
            chunk.advance(taken);
            break;
        }
        Poll::Ready(Ok(()))
    }
}

/// What `future` comes to, polled until it is.
fn ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(std::task::Waker::noop());
    let mut future = std::pin::pin!(future);
    for _ in 0..1_000_000 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
    panic!("a stream whose every chunk comes after one wait never ended");
}

/// Every frame `io` carries, read to its end.
fn read_all(io: Trickle, bounds: Bounds) -> Result<Vec<(Head, Bytes)>, StreamError> {
    let mut reader = FrameReader::new(io, bounds);
    ready(async {
        let mut frames = Vec::new();
        while let Some(frame) = reader.next().await? {
            assert!(
                frame.body.bytes.is_empty() || is_aligned(&frame.body.bytes),
                "a body is read where it is aligned"
            );
            frames.push((frame.head, frame.body.bytes));
            assert!(frames.len() <= 64, "each frame takes bytes of the stream");
        }
        Ok(frames)
    })
}

/// Any frame a sender may send, of up to `most` pieces.
fn any_frame(most: usize) -> impl Strategy<Value = (Kind, Vec<Bytes>)> {
    let opens = prop_oneof![
        Just(None),
        Just(Some(Call::Read)),
        Just(Some(Call::Write)),
        Just(Some(Call::ReadPublished)),
    ];
    let rows = prop_oneof![Just(Rows::Arrow), Just(Rows::Changes)];
    let kind = prop_oneof![
        (rows, any::<bool>(), any::<u64>()).prop_map(|(rows, dictionary, tag)| Kind::Batch {
            rows,
            dictionary,
            tag
        }),
        Just(Kind::Json),
        opens.prop_map(|opens| Kind::Message { opens }),
    ];
    let piece = proptest::collection::vec(any::<u8>(), 0..300).prop_map(Bytes::from);
    (kind, proptest::collection::vec(piece, 0..most))
}

proptest! {
    /// Whatever frames are sent, from however many pieces each, and in whatever chunks they
    /// arrive, they are read as they were sent, each body whole and where it is aligned.
    #[test]
    fn frames_are_read_as_they_were_sent_in_any_chunks(
        frames in proptest::collection::vec(any_frame(6), 0..8),
        cuts in proptest::collection::vec(1_usize..400, 0..32),
    ) {
        let bytes = sent(&frames);
        let read = read_all(Trickle::new(&bytes, &cuts), every(u32::MAX)).expect("reads");
        let expected: Vec<(Head, Bytes)> = frames
            .iter()
            .map(|(kind, pieces)| {
                let body: Vec<u8> = pieces.iter().flat_map(|piece| piece.to_vec()).collect();
                let len = u32::try_from(body.len()).expect("a short body");
                (Head::new(*kind, len), Bytes::from(body))
            })
            .collect();
        prop_assert_eq!(read, expected);
    }
}

#[test]
fn a_stream_ended_within_a_frame_fails_where_it_ended() {
    let bytes = sent(&[(Kind::Json, vec![Bytes::from_static(b"{\"a\": 1}")])]);
    for end in [1, HEAD_BYTES - 1, HEAD_BYTES + 1, bytes.len() - 1] {
        let read = read_all(Trickle::new(&bytes[..end], &[]), every(u32::MAX));
        let expected = if end < HEAD_BYTES {
            (end, HEAD_BYTES)
        } else {
            (end - HEAD_BYTES, bytes.len() - HEAD_BYTES)
        };
        let Err(StreamError::Truncated {
            arrived,
            expected: whole,
        }) = read
        else {
            panic!("a stream ended at {end} is truncated: {read:?}");
        };
        assert_eq!(
            (arrived, whole),
            (expected.0 as u64, expected.1 as u64),
            "ended at {end}"
        );
    }
}

#[test]
fn a_refused_head_ends_the_stream_before_any_of_its_body_is_read() {
    let bounds = Bounds {
        json: None,
        ..every(8)
    };
    let mut bytes = sent(&[(Kind::Json, vec![Bytes::from_static(b"text")])]);
    let batch = Kind::Batch {
        rows: Rows::Arrow,
        dictionary: false,
        tag: 0,
    };
    let long = sent(&[(batch, vec![Bytes::from(vec![0; 9])])]);
    bytes.extend_from_slice(&long);
    let mut reader = FrameReader::new(Trickle::new(&bytes, &[]), bounds);
    let refused = ready(reader.head());
    assert!(
        matches!(
            refused,
            Err(StreamError::Head(HeadError::Malformed(
                Malformed::Unexpected("json")
            )))
        ),
        "{refused:?}"
    );
    assert!(
        matches!(ready(reader.next()), Ok(None)),
        "nothing is read after a refusal"
    );
    let mut reader = FrameReader::new(Trickle::new(&long, &[]), every(8));
    let refused = ready(reader.head());
    assert!(
        matches!(refused, Err(StreamError::Head(HeadError::Refused(_)))),
        "{refused:?}"
    );
}

#[test]
fn a_head_is_read_once_and_given_again_until_its_body_is() {
    let bytes = sent(&[
        (Kind::Json, vec![Bytes::from_static(b"one")]),
        (Kind::Json, vec![Bytes::from_static(b"two")]),
    ]);
    let mut reader = FrameReader::new(Trickle::new(&bytes, &[5, 9]), every(64));
    let first = ready(reader.head()).expect("a head").expect("a frame");
    assert_eq!(ready(reader.head()).expect("a head"), Some(first));
    let body = ready(reader.body()).expect("its body");
    assert_eq!(&body.body.bytes[..], b"one");
    let second = ready(reader.next()).expect("a frame").expect("another");
    assert_eq!(&second.body.bytes[..], b"two");
    assert!(ready(reader.next()).expect("the end").is_none());
}

/// The frames read from a stream of `frames`, sent to a reader in chunks of 7 bytes after a
/// wait each, which a read cut at any wait leaves to be read on.
#[test]
fn a_read_cut_at_any_wait_loses_nothing_of_the_frames() {
    let frames = vec![
        (
            Kind::Message {
                opens: Some(Call::Write),
            },
            vec![Bytes::from_static(b"start")],
        ),
        (
            Kind::Batch {
                rows: Rows::Arrow,
                dictionary: false,
                tag: 7,
            },
            vec![Bytes::from(vec![3; 40])],
        ),
        (Kind::Json, Vec::new()),
    ];
    let bytes = sent(&frames);
    let cuts = vec![7; bytes.len() / 7];
    let expected = read_all(Trickle::new(&bytes, &cuts), every(64)).expect("reads");
    at_every_point(
        async || {
            (
                FrameReader::new(Trickle::new(&bytes, &cuts), every(64)),
                Vec::new(),
            )
        },
        async |(reader, read): &mut (FrameReader<Trickle>, Vec<(Head, Bytes)>)| {
            while let Some(frame) = reader.next().await.expect("a frame") {
                read.push((frame.head, frame.body.bytes));
            }
        },
        async |(reader, read), _: Ended<()>| {
            while let Some(frame) = reader.next().await.expect("a frame") {
                read.push((frame.head, frame.body.bytes));
            }
            assert_eq!(*read, expected);
        },
    );
}

/// A stream that takes at most `each` bytes a write, each after a wait, into what it holds.
#[derive(Default)]
struct Narrow {
    held: Vec<u8>,
    each: usize,
    waited: bool,
}

impl AsyncWrite for Narrow {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if !self.waited {
            self.waited = true;
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.waited = false;
        let taken = buffer.len().min(self.each);
        self.held.extend_from_slice(&buffer[..taken]);
        Poll::Ready(Ok(taken))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn a_write_cut_at_any_wait_goes_on_where_it_was() {
    let pieces = vec![
        Bytes::from_static(b"first piece"),
        Bytes::new(),
        Bytes::from(vec![9; 30]),
    ];
    let kind = Kind::Batch {
        rows: Rows::Changes,
        dictionary: true,
        tag: 3,
    };
    let expected = sent(&[(kind, pieces.clone())]);
    at_every_point(
        async || {
            let narrow = Narrow {
                each: 5,
                ..Narrow::default()
            };
            (
                narrow,
                Outgoing::new(kind, pieces.clone()).expect("a short frame"),
            )
        },
        async |(io, frame): &mut (Narrow, Outgoing)| frame.write(io).await.expect("written"),
        async |(io, frame), _: Ended<()>| {
            frame.write(io).await.expect("written");
            assert_eq!(io.held, expected);
        },
    );
}

#[test]
fn a_frame_is_written_with_its_pieces_as_slices_of_one_write() {
    let pieces = vec![
        Bytes::from_static(b"ab"),
        Bytes::new(),
        Bytes::from_static(b"cd"),
    ];
    let mut frame = Outgoing::new(Kind::Json, pieces).expect("a short frame");
    {
        let mut slices = [std::io::IoSlice::new(&[]); 8];
        assert_eq!(
            frame.chunks_vectored(&mut slices),
            3,
            "the head and two pieces"
        );
        assert_eq!(slices[0].len(), HEAD_BYTES);
        assert_eq!((&*slices[1], &*slices[2]), (&b"ab"[..], &b"cd"[..]));
    }
    frame.advance(HEAD_BYTES + 1);
    let mut slices = [std::io::IoSlice::new(&[]); 8];
    assert_eq!(frame.chunks_vectored(&mut slices), 2);
    assert_eq!((&*slices[0], &*slices[1]), (&b"b"[..], &b"cd"[..]));
    assert_eq!(frame.remaining(), 3);
}

#[test]
fn a_message_frame_is_scanned_before_it_is_decoded() {
    let request = v1::ReadControl {
        control: Some(v1::read_control::Control::Stop(v1::Stop {
            mode: v1::StopMode::Now as i32,
        })),
    };
    let bytes = sent(&[(
        Kind::Message { opens: None },
        vec![Bytes::from(prost::Message::encode_to_vec(&request))],
    )]);
    let form = Call::Read
        .form(End::Host)
        .expect("a read's host sends controls");
    let mut reader = FrameReader::new(Trickle::new(&bytes, &[]), every(64));
    let frame = ready(reader.next()).expect("a frame").expect("one");
    let decoded: v1::ReadControl = frame.message(form, 4_096).expect("decodes");
    assert_eq!(decoded, request);
    assert!(matches!(
        frame.message::<v1::ReadControl>(form, 1),
        Err(StreamError::Inflated { bound: 1, .. })
    ));
    let garbage = sent(&[(
        Kind::Message { opens: None },
        vec![Bytes::from_static(&[0xff])],
    )]);
    let mut reader = FrameReader::new(Trickle::new(&garbage, &[]), every(64));
    let frame = ready(reader.next()).expect("a frame").expect("one");
    assert!(matches!(
        frame.message::<v1::ReadControl>(form, 4_096),
        Err(StreamError::Unscanned { .. })
    ));
}

#[test]
fn each_end_of_each_call_sends_only_its_kinds_within_the_receiver_s_limits() {
    let limits = Limits::default();
    for call in Call::ALL {
        let host = call.bounds(End::Host, &limits);
        assert_eq!(host.json, None, "{call:?}: a host sends no JSON");
        assert_eq!(host.batch.is_some(), call == Call::Write, "{call:?}");
        let connector = call.bounds(End::Connector, &limits);
        assert_eq!(connector.json.is_some(), call == Call::Read, "{call:?}");
        assert_eq!(connector.batch.is_some(), call != Call::Write, "{call:?}");
        for bound in [host.batch, connector.batch].into_iter().flatten() {
            assert_eq!(
                u64::from(bound.bytes),
                limits.frame_bytes + crate::limits::MESSAGE_ROOM
            );
        }
        // The opening's message is within what the host sends.
        let opening = u64::from(host.message.bytes);
        assert!(opening >= limits.control_message_bytes, "{call:?}");
    }
}

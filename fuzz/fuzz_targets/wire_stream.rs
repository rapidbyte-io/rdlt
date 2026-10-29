//! Whatever bytes a data call's stream carries, in whatever chunks, its reader refuses a frame
//! beyond what its sender may send before it holds the frame's body, without panicking: each body
//! it reads it holds in one allocation of the length its head declared, within its kind's bound,
//! and each message it decodes is sent again as the bytes prost encodes it to.

#![forbid(unsafe_code)]
#![no_main]

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf as _, Bytes};
use libfuzzer_sys::fuzz_target;
use rdlt_wire::aligned::ALIGNMENT;
use rdlt_wire::limits::Limits;
use rdlt_wire::prost::Message;
use rdlt_wire::stream::{Bounds, Call, End, FrameReader, Head, Kind, Received};
use rdlt_wire::{IpcFrame, v1};
use tokio::io::{AsyncRead, ReadBuf};

/// A stream of chunks, each ready at once, which then ends.
struct Chunked(VecDeque<Bytes>);

impl AsyncRead for Chunked {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        while let Some(chunk) = self.0.front_mut() {
            if chunk.is_empty() {
                self.0.pop_front();
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

/// `bytes` cut at each of `cuts`, taken as offsets into what is left.
fn chunked(bytes: &[u8], cuts: &[u16]) -> VecDeque<Bytes> {
    let bytes = Bytes::copy_from_slice(bytes);
    let mut chunks = VecDeque::new();
    let mut at = 0;
    for cut in cuts {
        let end = (at + usize::from(*cut)).min(bytes.len());
        chunks.push_back(bytes.slice(at..end));
        at = end;
    }
    chunks.push_back(bytes.slice(at..));
    chunks
}

/// Limits small enough that the fuzzer reaches every bound.
fn limits() -> Limits {
    Limits {
        frame_bytes: 1 << 16,
        json_push_bytes: 1 << 16,
        cursor_bytes: 1 << 12,
        schema_bytes: 1 << 12,
        control_message_bytes: 1 << 12,
        ..Limits::default()
    }
}

/// What `future` comes to, which it does when first polled: every chunk is ready.
fn ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("a stream whose every chunk is ready waits"),
    }
}

/// The bytes the bound of `head`'s kind lets its body take, where `bounds` lets it come at all.
fn bound(head: &Head, bounds: &Bounds) -> Option<u32> {
    match head.kind {
        Kind::Batch { .. } => bounds.batch.map(|bound| bound.bytes),
        Kind::Json => bounds.json.map(|bound| bound.bytes),
        Kind::Message { .. } => Some(bounds.message.bytes),
    }
}

/// Checks that a message `frame` holds that decodes as `M` is sent again as prost encodes it.
fn sent_again<M: Message + Default + PartialEq + std::fmt::Debug>(
    frame: &Received,
    form: &'static rdlt_wire::scan::Form,
) {
    let len = frame.body.bytes.len();
    if let Ok(message) = frame.message::<M>(form, len.saturating_mul(16).max(4_096)) {
        let again = M::decode(message.encode_to_vec().as_slice()).unwrap();
        assert_eq!(again, message, "a message decodes as it was encoded");
    }
}

fuzz_target!(|input: (u8, bool, Vec<u8>, Vec<u16>)| {
    let (call, connector, bytes, cuts) = input;
    let call = Call::ALL[usize::from(call) % Call::ALL.len()];
    let end = if connector { End::Connector } else { End::Host };
    let bounds = call.bounds(end, &limits());
    let mut reader = FrameReader::new(Chunked(chunked(&bytes, &cuts)), bounds);
    for _ in 0..=bytes.len() {
        let Ok(Some(head)) = ready(reader.head()) else {
            return;
        };
        let within = bound(&head, &bounds).expect("a head of a kind its end does not send is refused");
        assert!(head.len <= within, "a body beyond its bound is refused before it is read");
        let Ok(frame) = ready(reader.body()) else {
            return;
        };
        let len = usize::try_from(head.len).unwrap();
        assert_eq!(frame.body.bytes.len(), len);
        assert!(
            frame.body.allocation.len() <= len + ALIGNMENT,
            "a body is held in an allocation of its length"
        );
        match (head.kind, end) {
            (Kind::Message { opens: Some(_) }, _) => {}
            (Kind::Message { opens: None }, End::Connector) => {
                if call == Call::Write {
                    sent_again::<v1::WriteAck>(&frame, call.form(end).unwrap());
                } else {
                    sent_again::<v1::ReadFrame>(&frame, call.form(end).unwrap());
                }
            }
            (Kind::Message { opens: None }, End::Host) => match call {
                Call::Read => sent_again::<v1::ReadControl>(&frame, call.form(end).unwrap()),
                Call::Write => sent_again::<v1::WriteFrame>(&frame, call.form(end).unwrap()),
                Call::ReadPublished => {}
            },
            (Kind::Batch { .. }, _) => drop(IpcFrame::framed(frame.body)),
            (Kind::Json, _) => {}
        }
    }
});

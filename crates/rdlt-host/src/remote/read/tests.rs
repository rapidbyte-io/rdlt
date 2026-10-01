use bytes::{BufMut, Bytes, BytesMut};
use rdlt_connector::wire::v1;
use rdlt_connector::{Push, SourceEvent};
use rdlt_wire::prost::Message as _;
use rdlt_wire::prost::encoding::{WireType, encode_key, encode_varint};
use rdlt_wire::{Decoder, Limits};

use super::{Read, Reader};

fn reader() -> Reader {
    let limits = Limits::default();
    Reader {
        decoder: Decoder::new(limits),
        limits,
        epoch: None,
    }
}

/// `frame` as a transport receives it: one buffer holding the message, padded with a megabyte
/// of a field this end does not know.
fn padded(frame: &v1::ReadFrame) -> Bytes {
    const PADDING: usize = 1 << 20;
    let mut message = BytesMut::with_capacity(PADDING + 1024);
    frame.encode(&mut message).expect("room for the frame");
    encode_key(1000, WireType::LengthDelimited, &mut message);
    encode_varint(PADDING as u64, &mut message);
    message.put_bytes(0xaa, PADDING);
    message.freeze()
}

/// The event `message` becomes; the frame decoded from it is dropped with the read's turn.
fn event(message: &Bytes) -> SourceEvent {
    let frame = v1::ReadFrame::decode(message.clone()).expect("a read frame");
    match reader().event(frame).expect("an admitted frame") {
        Read::Event(event) => event,
        Read::Done | Read::Nothing => panic!("the frame is an event"),
    }
}

#[test]
fn a_json_push_does_not_keep_its_message_alive() {
    let frame = v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Json(v1::JsonFrame {
            data: Bytes::from_static(b"[{\"a\":1}]"),
        })),
    };
    let message = padded(&frame);
    let event = event(&message);
    assert_eq!(
        event,
        SourceEvent::Push(Push::Json(Bytes::from_static(b"[{\"a\":1}]")))
    );
    assert!(message.is_unique(), "the push keeps its message alive");
}

#[test]
fn a_checkpoint_does_not_keep_its_message_alive() {
    let frame = v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Checkpoint(v1::CheckpointFrame {
            cursor: Some(v1::Cursor {
                version: 1,
                bytes: Bytes::from_static(b"7"),
            }),
            barrier: None,
        })),
    };
    let message = padded(&frame);
    let event = event(&message);
    let SourceEvent::Checkpoint { cursor, .. } = &event else {
        panic!("the frame is a checkpoint");
    };
    assert_eq!(cursor.bytes().as_ref(), b"7");
    assert!(message.is_unique(), "the cursor keeps its message alive");
}

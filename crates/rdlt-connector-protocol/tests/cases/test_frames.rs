//! Golden-frame pins: representative messages from BOTH directions —
//! request-side (client → connector) and reply-side (connector → client) —
//! encoded with fixed field values and checked against hardcoded hex.
//! Field numbers are FROZEN; a pin breaks if a number moves — that is the
//! point. A protobuf field renumber is silent at the type level (the
//! struct still compiles, the wire bytes just mean something else), so
//! this is the one net that would actually catch it.

use prost::Message;
use rdlt_connector_protocol::PROTOCOL_VERSION;
use rdlt_connector_protocol::proto::{
    HandshakeRequest, PartClosedEvent, ReadFrame, SessionReply, SessionRequest, Write, read_frame,
    session_reply, session_request,
};

#[test]
fn protocol_version_is_pinned_at_zero() {
    assert_eq!(PROTOCOL_VERSION, 0);
}

#[test]
fn handshake_request_golden_frame() {
    let request = HandshakeRequest {
        protocol_version: 0,
        expected_role: "source".to_string(),
        config_json: b"{}".to_vec(),
    };

    let mut encoded = Vec::new();
    request.encode(&mut encoded).expect("encode");

    // field 1 (protocol_version, varint) omitted at its zero default;
    // field 2 (expected_role, LEN) tag 0x12, len 6, "source";
    // field 3 (config_json, LEN) tag 0x1a, len 2, "{}"
    let golden = hex_literal(
        "12 06 73 6f 75 72 63 65 \
         1a 02 7b 7d",
    );
    assert_eq!(
        encoded, golden,
        "field numbers are FROZEN; this pin breaks if a number moves — that is the point"
    );

    let decoded = HandshakeRequest::decode(encoded.as_slice()).expect("decode");
    assert_eq!(decoded, request);
}

#[test]
fn session_request_write_golden_frame() {
    let request = SessionRequest {
        request: Some(session_request::Request::Write(Write {
            table: "events".to_string(),
            arrow_ipc: vec![0xde, 0xad, 0xbe, 0xef],
        })),
    };

    let mut encoded = Vec::new();
    request.encode(&mut encoded).expect("encode");

    // oneof field 3 (Write, LEN) tag 0x1a, len 14, containing:
    //   Write.table (field 1, LEN) tag 0x0a, len 6, "events"
    //   Write.arrow_ipc (field 2, LEN) tag 0x12, len 4, de ad be ef
    let golden = hex_literal(
        "1a 0e \
         0a 06 65 76 65 6e 74 73 \
         12 04 de ad be ef",
    );
    assert_eq!(
        encoded, golden,
        "field numbers are FROZEN; this pin breaks if a number moves — that is the point"
    );

    let decoded = SessionRequest::decode(encoded.as_slice()).expect("decode");
    assert_eq!(decoded, request);
}

#[test]
fn session_reply_part_closed_golden_frame() {
    let reply = SessionReply {
        reply: Some(session_reply::Reply::PartClosed(PartClosedEvent {
            table: "events".to_string(),
            encoded_bytes: 4096,
            reason: "target".to_string(),
        })),
    };

    let mut encoded = Vec::new();
    reply.encode(&mut encoded).expect("encode");

    // oneof field 10 (PartClosedEvent, LEN) tag 0x52, len 19, containing:
    //   PartClosedEvent.table (field 1, LEN) tag 0x0a, len 6, "events"
    //   PartClosedEvent.encoded_bytes (field 2, varint) tag 0x10, 4096 as 80 20
    //   PartClosedEvent.reason (field 3, LEN) tag 0x1a, len 6, "target"
    let golden = hex_literal(
        "52 13 \
         0a 06 65 76 65 6e 74 73 \
         10 80 20 \
         1a 06 74 61 72 67 65 74",
    );
    assert_eq!(
        encoded, golden,
        "field numbers are FROZEN; this pin breaks if a number moves — that is the point"
    );

    let decoded = SessionReply::decode(encoded.as_slice()).expect("decode");
    assert_eq!(decoded, reply);
}

#[test]
fn read_frame_arrow_ipc_golden_frame() {
    let frame = ReadFrame {
        frame: Some(read_frame::Frame::ArrowIpc(vec![0xde, 0xad, 0xbe, 0xef])),
    };

    let mut encoded = Vec::new();
    frame.encode(&mut encoded).expect("encode");

    // oneof field 2 (arrow_ipc, LEN) tag 0x12, len 4, de ad be ef
    let golden = hex_literal("12 04 de ad be ef");
    assert_eq!(
        encoded, golden,
        "field numbers are FROZEN; this pin breaks if a number moves — that is the point"
    );

    let decoded = ReadFrame::decode(encoded.as_slice()).expect("decode");
    assert_eq!(decoded, frame);
}

/// Turns a whitespace-separated hex literal (as laid out above, one byte
/// per pair, free to wrap across lines) into bytes — a small, local, no-dep
/// helper rather than pulling in a hex crate for two test fixtures.
fn hex_literal(spelled: &str) -> Vec<u8> {
    spelled
        .split_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).expect("valid hex byte"))
        .collect()
}

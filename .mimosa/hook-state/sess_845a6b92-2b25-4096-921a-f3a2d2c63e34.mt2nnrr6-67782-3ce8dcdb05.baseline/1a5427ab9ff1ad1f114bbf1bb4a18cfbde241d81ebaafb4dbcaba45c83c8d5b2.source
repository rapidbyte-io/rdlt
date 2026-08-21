//! Fuzz: wire frame decode (047 M7) — prost decode of the frames a
//! third-party connector authors (`ReadFrame`, `SessionReply`,
//! `HandshakeReply`) and the write direction's `Write`, over arbitrary
//! bytes: decoded or refused, never a panic. Decode alone — the arrow
//! payload's own parser has its own target (`arrow_ipc_decode`).

#![no_main]

use libfuzzer_sys::fuzz_target;
use prost::Message as _;
use rdlt_connector_protocol::proto;

fuzz_target!(|data: &[u8]| {
    let _ = proto::ReadFrame::decode(data);
    let _ = proto::SessionReply::decode(data);
    let _ = proto::HandshakeReply::decode(data);
    let _ = proto::Write::decode(data);
});

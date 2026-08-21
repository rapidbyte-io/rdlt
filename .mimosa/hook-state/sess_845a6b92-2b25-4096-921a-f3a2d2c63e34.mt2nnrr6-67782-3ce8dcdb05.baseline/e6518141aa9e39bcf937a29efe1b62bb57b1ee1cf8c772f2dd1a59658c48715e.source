//! Fuzz: the stdout handshake line parser (047 M7) — child-controlled
//! text, parsed on every spawn by providers and probes alike: parsed
//! or refused, never a panic.

#![no_main]

use libfuzzer_sys::fuzz_target;
use rdlt_connector_protocol::handshake::Line;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = Line::parse(text);
    }
});

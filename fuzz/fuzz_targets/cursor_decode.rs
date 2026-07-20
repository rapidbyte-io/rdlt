//! Fuzz: persisted state decoding (feature 003 R22 target 2).
//!
//! State docs come back from destinations — bytes rdlt did not necessarily
//! produce. Decoding must return typed errors, never panic; a successful decode
//! must survive an encode/decode roundtrip unchanged.

#![no_main]

use libfuzzer_sys::fuzz_target;
use rdlt_connector::Cursor;
use rdlt_source_file::cursor::FileCursor;

fuzz_target!(|data: &[u8]| {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    if let Ok(decoded) = FileCursor::decode(Some(&Cursor::new(value))) {
        let again = FileCursor::decode(Some(&decoded.encode())).expect("roundtrip decodes");
        assert_eq!(again, decoded, "encode/decode roundtrip must be stable");
    }
});

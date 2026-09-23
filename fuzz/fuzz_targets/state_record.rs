//! Decoding untrusted state records never panics, and whatever decodes re-encodes losslessly.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use rdlt_connector::{StateEntry, StateRecord};

fuzz_target!(|input: (String, Vec<u8>)| {
    let (key, value) = input;
    let record = StateRecord { key, value: Bytes::from(value) };
    if let Ok(entry) = StateEntry::from_record(&record) {
        let again = StateEntry::from_record(&entry.to_record()).expect("a re-encoded entry decodes");
        assert_eq!(again, entry);
    }
});

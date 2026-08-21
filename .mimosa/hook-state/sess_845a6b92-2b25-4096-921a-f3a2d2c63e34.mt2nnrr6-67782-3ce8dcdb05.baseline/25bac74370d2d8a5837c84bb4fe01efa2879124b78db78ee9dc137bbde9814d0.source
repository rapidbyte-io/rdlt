//! Fuzz: WAL manifest line classification (047 M7) — the reader's own
//! comments invite hand-corrupt input: `Record`, `Corrupt`, or
//! `Untrailered`, never a panic, with the segment-name gate riding
//! along over the same untrusted text.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        rdlt_engine::fuzzing::wal_manifest_line(text);
    }
});

//! Fuzz: raw-JSON slab parsing (feature 003 R22 target 1).
//!
//! The engine's NDJSON/array/document parser over arbitrary bytes: typed errors
//! only — no panic, hang, or memory blowup.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rdlt_engine::fuzzing::parse_slab(data);
});

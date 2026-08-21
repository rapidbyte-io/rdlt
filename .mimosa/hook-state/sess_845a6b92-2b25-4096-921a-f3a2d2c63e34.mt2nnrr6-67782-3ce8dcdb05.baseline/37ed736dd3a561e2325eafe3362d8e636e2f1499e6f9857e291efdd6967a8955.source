//! Fuzz: the FULL shred path (feature 003 R22 target 5) — parse, shape
//! observation, schema resolution, Arrow build — with inline invariant checks
//! (unique destination column names).

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rdlt_engine::fuzzing::shred_slab(data);
});

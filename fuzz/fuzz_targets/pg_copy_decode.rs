//! Feature 005 T014: the binary-COPY decoder is total — arbitrary bytes may
//! only produce typed decode errors, never a panic or runaway allocation.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rdlt_source_postgres::testhook::fuzz_copy_decode(data);
});

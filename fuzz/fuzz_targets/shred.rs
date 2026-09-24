//! Shredding untrusted JSON never panics and never fails inside the shredder, whatever the bytes
//! and however they are cut into chunks.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (u8, Vec<u8>)| {
    let (size, json) = input;
    let chunk_bytes = usize::from(size) * 16 + 1;
    let push = Bytes::from(json);
    // Two copies meet across pushes and chunks, joining their shapes.
    if let Err(refused) = rdlt_engine::bench::shred(&[push.clone(), push], chunk_bytes) {
        assert_ne!(refused.code, "shred_internal", "{}", refused.message);
    }
});

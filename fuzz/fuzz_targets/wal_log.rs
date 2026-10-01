//! Reading a write-ahead log, as replay reads one, never panics, whatever its bytes.
//!
//! The bytes are any at all, or a valid log cut short and garbled where the fuzzer says, its
//! checksums made to match again where it says so, so garbled payloads reach the decoders. A log
//! is read up to where it was torn, or refused as one the engine did not write.

#![forbid(unsafe_code)]
#![no_main]

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use libfuzzer_sys::fuzz_target;

/// Makes each frame's checksum match its payload again, following its length as it now reads.
fn rechecked(log: &mut [u8]) {
    let mut offset = 0;
    while let Some(head) = log.get(offset..offset + 9) {
        let len = u32::from_le_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let Some(end) = (offset + 9).checked_add(len).filter(|end| *end <= log.len()) else {
            return;
        };
        let crc = crc32c::crc32c(&log[offset + 9..end]);
        log[offset + 5..offset + 9].copy_from_slice(&crc.to_le_bytes());
        offset = end;
    }
}

fuzz_target!(|input: (bool, bool, u16, Vec<(u16, u8)>, Vec<u8>)| {
    // Arrow's panics are contained by the wire's decoder, which refuses them without reaching the
    // panic hook libfuzzer aborts in: only one that escapes it fails the target.
    let (garbled, recheck, cut, edits, bytes) = input;
    let log = if garbled {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        let batch = RecordBatch::try_from_iter([("id", ids), ("name", names)])
            .expect("a valid batch");
        let mut log = rdlt_engine::bench::sample_log(batch);
        for (at, xor) in edits {
            let len = log.len();
            if let Some(byte) = log.get_mut(usize::from(at) % len.max(1)) {
                *byte ^= xor;
            }
        }
        if recheck {
            rechecked(&mut log);
        }
        log.truncate(log.len() - usize::from(cut) % (log.len() + 1));
        log
    } else {
        bytes
    };
    if let Err(refused) = rdlt_engine::bench::scan_log(&log) {
        assert_eq!(refused.code, "wal_unreadable", "{}", refused.message);
    }
});

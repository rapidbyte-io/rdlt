//! Reading a write-ahead log, as replay reads one, never panics, whatever its bytes.
//!
//! The bytes are any at all, or a valid log cut short and garbled where the fuzzer says. A log is
//! read up to where it was torn, or refused as one the engine did not write.

#![no_main]

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (bool, u16, Vec<(u16, u8)>, Vec<u8>)| {
    let (garbled, cut, edits, bytes) = input;
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
        log.truncate(log.len() - usize::from(cut) % (log.len() + 1));
        log
    } else {
        bytes
    };
    if let Err(refused) = rdlt_engine::bench::scan_log(&log) {
        assert_eq!(refused.code, "wal_unreadable", "{}", refused.message);
    }
});

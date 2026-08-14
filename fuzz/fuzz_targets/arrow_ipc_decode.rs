//! Fuzz: Arrow IPC stream decode over arbitrary bytes (047 M7) — the
//! construction the client performs on every `arrow_ipc` payload a
//! connector serves: a `StreamReader` over the raw frame bytes,
//! batches pulled to stream end. A whole nested parser sitting
//! opposite arbitrary third-party bytes.
//!
//! This target found a REACHABLE panic in arrow-ipc 58.3's own schema
//! converter (`convert.rs:332`, a negative Int bit width) that GLM
//! never listed. The CLIENT seat is now hardened against it: commit
//! `11a396ed` wraps `rdlt-connector-client`'s `decode_one_batch` in
//! `catch_unwind`, so a crafted frame refuses typed there rather than
//! unwinding. But arrow's library panic itself is not fixable in-tree,
//! and this target still drives arrow's RAW reader — so it stays OUT
//! of the Makefile run set (it would re-find arrow's own panic) until
//! it is REPOINTED at the client's hardened seat via a client-side
//! fuzzing hook (mirroring the engine's `fuzzing` module). Kept in the
//! tree, compiled by `cargo fuzz build`, as the standing coverage door
//! and the reproducer's home.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(reader) = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)
    else {
        return;
    };
    for batch in reader {
        if batch.is_err() {
            return;
        }
    }
});

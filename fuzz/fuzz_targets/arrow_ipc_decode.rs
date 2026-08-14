//! Fuzz: the client's Arrow IPC decode seat over arbitrary bytes
//! (047 M7) — `rdlt-connector-client`'s hardened `decode_one_batch`,
//! the construction the client performs on every `arrow_ipc` payload a
//! connector serves: a `StreamReader` over the raw frame bytes,
//! batches pulled to stream end, the whole decode under `catch_unwind`.
//!
//! This target FOUND a reachable panic GLM never listed: arrow-ipc
//! 58.3's own schema converter `panic!`s on a crafted frame (a negative
//! Int bit width, `convert.rs:332`), before any `.map_err`. The client
//! seat is now hardened against it (commit `11a396ed` — the whole
//! decode runs under `catch_unwind`, a crafted frame refusing typed
//! rather than unwinding), and this target drives THAT seat through the
//! client's doc-hidden `fuzzing` hook (commit `3116c26e`) rather than
//! arrow's raw reader.
//!
//! It is BUILT but NOT in the Makefile run set, deliberately: the
//! catch_unwind containment is real in PRODUCTION (panic=unwind) and
//! pinned by the client's own embedded-reproducer unit test, but
//! libfuzzer-sys installs a panic hook that `abort()`s the instant a
//! panic STARTS — before `catch_unwind` runs — so arrow's internal
//! panic still reads as a libFuzzer crash under the harness. Running it
//! would red the gate on a production-contained defect. Kept compiled
//! (the reproducer's home, the coverage door), targeting the real seat
//! so it becomes runnable the day arrow itself stops panicking here.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rdlt_connector_client::fuzzing::decode_one_batch(data);
});

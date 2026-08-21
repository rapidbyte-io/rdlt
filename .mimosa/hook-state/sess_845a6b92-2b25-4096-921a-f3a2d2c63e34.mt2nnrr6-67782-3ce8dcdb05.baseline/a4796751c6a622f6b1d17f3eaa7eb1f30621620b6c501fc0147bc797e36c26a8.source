//! Fuzzing entry points. `#[doc(hidden)]` — these exist ONLY so the
//! out-of-workspace `fuzz/` targets can reach `pub(crate)` hot paths
//! (the engine's `fuzzing` module precedent); they are not API and may
//! change at any time.

/// The HARDENED wire-edge arrow decode over arbitrary bytes — the seat
/// a rogue source's `arrow_ipc` frames actually hit, catch_unwind
/// included, not arrow's raw parser (fuzzing that only re-finds
/// arrow's own library panics forever). Never panics on any input by
/// construction: a regression that lets an unwind escape the seat
/// surfaces here as the fuzzer's crash.
pub fn decode_one_batch(bytes: &[u8]) {
    let _ = crate::source::decode_one_batch(bytes);
}

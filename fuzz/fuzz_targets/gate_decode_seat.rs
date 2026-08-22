//! Fuzz: THE shared Arrow IPC decode seat —
//! `rdlt_connector::gate::decode_one_batch_ipc`, the one function both
//! wire directions run a frame's batch payload through since 076: the
//! framing pre-pass, the catch_unwind belt, the width and row caps, the
//! one-batch rule, and the field-name walk, in whichever direction the
//! bytes travel. One target now attacks what used to be two mirrored
//! seats; every hostile input either refuses typed behind the refusal
//! prefix or decodes — never an unwind escaping.
//!
//! BUILT but NOT in the Makefile run set, for the same measured reason
//! as `arrow_ipc_decode` and `wal_segment_decode`: the seat's belt
//! contains arrow's internal panics in PRODUCTION (panic=unwind), and
//! the known panic input (the 160-byte negative-bit-width REPRO, now in
//! this target's corpus) refuses even earlier at the framing pre-pass —
//! but libfuzzer-sys aborts on any FUTURE arrow-internal panic before
//! containment can run, which would red the nightly gate on a
//! production-contained defect. The corpus carries that reproducer plus
//! framed variants, so the seat can be probed by hand:
//!   cd fuzz && cargo +nightly fuzz run gate_decode_seat
//! and it graduates to the RUN set the day arrow stops panicking here.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The generic refusal spellings are what the seat pins; the no-op
    // field predicate is the loosest legal configuration — the seat's
    // own defenses must hold without any caller-side help.
    let _ = rdlt_connector::gate::decode_one_batch_ipc(
        data,
        "fuzz: refused",
        "fuzz: multi",
        |message: String| message,
        &mut |_| Ok(()),
    );
});

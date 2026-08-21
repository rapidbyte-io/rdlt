//! Fuzz: the WAL segment decode seat over arbitrary bytes (047 3L6) —
//! the Arrow IPC **FileReader** path replay drives on every WAL segment
//! (footer verification in `try_new`, then per-batch `next()`), which
//! `arrow_ipc_decode`'s StreamReader coverage never touches. Driven
//! through the engine's doc-hidden `fuzzing::wal_segment_decode` hook so
//! it exercises the same containment replay uses: a typed error or a
//! caught unwind, never an escape.
//!
//! Whether it can sit in the Makefile RUN set is decided empirically:
//! the seat shares arrow-ipc's schema converter with the client's
//! StreamReader seat, whose internal panic arms (047 M7) abort under
//! libfuzzer-sys's panic hook before `catch_unwind` can contain them —
//! see the FUZZ_TARGETS comment in the Makefile for the disposition.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rdlt_engine::fuzzing::wal_segment_decode(data);
});

//! Engine fail-point registry (feature 003 R20, gate G2). The `crash_point!`
//! macro is defined once in `rdlt_core::failpoint` — call sites import it from
//! there; this module holds nothing but the registry.
//!
//! Registry discipline (G2.2): every `crash_point!` site in this crate MUST be
//! listed here — the crash sweep asserts its enumeration matches, so an
//! instrumented-but-unswept boundary fails tests.

#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const ENGINE_POINTS: &[&str] = &[
    "wal.segment.write",
    "wal.segment.fsync",
    "wal.manifest.append",
    "wal.manifest.fsync",
    "session.after_ensure",
    "session.after_write",
    "session.after_commit",
];

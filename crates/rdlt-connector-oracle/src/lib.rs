//! The Oracle source — a pure-Rust wire connection, consistent-SCN
//! snapshot reads paged by ROWID keyset, and the watermark cursor
//! rulebook, born on the rdlt connector sdk.
//!
//! The driver's release cannot stream past its 100-row prefetch (the
//! T001 probe record in specs/032-oracle/plan.md), so the read path
//! never continues a cursor: every page is a fresh bounded query that
//! arrives complete, and a connection that has seen ANY error is
//! dropped, never reused.

pub mod source;

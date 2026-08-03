//! The Oracle source — a pure-Rust wire connection, keyset-paged
//! reads, and the watermark cursor rulebook, born on the rdlt
//! connector sdk.
//!
//! CONSISTENCY IS PER PAGE, NOT PER READ. Oracle's default is
//! statement-level consistency and this connector opens no
//! transaction, so a multi-page read sees each page at its own SCN.
//! A cursor-paged stream converges — rows are ordered by the
//! watermark and the next run resumes from it — but a full-refresh
//! stream under concurrent DML can miss or repeat rows depending on
//! where they land physically. `SET TRANSACTION READ ONLY` would fix
//! it and is NOT used: it was probed and it poisons this driver's
//! connection (specs/032-oracle/plan.md).
//!
//! THE READ PATH NEVER CONTINUES A CURSOR. What forces that is the
//! standing SDU limit (plan.md T003): one query reply is read as ONE
//! packet, and the multi-packet read that would lift it was
//! attempted and reverted on evidence. So every page is a fresh
//! bounded query sized from the described column widths, the
//! connection is recycled before the server's open-cursor limit, and
//! a connection that has seen ANY error is dropped, never reused.

pub mod source;

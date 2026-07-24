//! CDC via logical replication: pgoutput decoding, slot lifecycle, and the
//! per-table pass machinery over the SQL peek/advance interface.
//!
//! Delivery design: bounded catch-up pins `target_lsn` once per run; every
//! CDC stream's `read()` peeks `(its cursor, target]` and filters its own
//! table (peeking consumes NOTHING). First run: slot FIRST, then ONE
//! `REPEATABLE READ` transaction snapshots every CDC table; the
//! slot-to-snapshot window applies twice and CONVERGES. Checkpoints land only
//! at transaction-commit positions. The slot's
//! acknowledged position advances once per run to the min DESTINATION-
//! COMMITTED position across CDC streams — each stream's `since` (only ever
//! a cursor the destination durably committed) or its fresh-snapshot start
//! point — so an ack can never outrun a commit, run shapes be damned (the
//! current run's own checkpoints are not yet known-committed, so acking
//! trails one run behind: hygiene, never correctness).

pub(crate) mod pgoutput;
pub mod slot;
pub(crate) mod values;

mod apply;
mod read;
mod runtime;
mod tail;

pub(crate) use read::{TableCtx, read_stream};
pub(crate) use runtime::Runtime;

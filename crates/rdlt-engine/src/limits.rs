//! Limits the engine enforces on what it holds: the shares its memory budget is divided into,
//! and how long a request waits for one.
//!
//! Each share is a fraction of the budget, given by its denominator, and is its holders' alone:
//! bytes of one share are never reserved from another, so the shares together never pass the
//! budget.

use std::num::NonZeroUsize;
use std::time::Duration;

/// The cursors of the seals waiting for a commit: a 64th of the budget, 4 MiB of the default
/// 256 MiB, which is one cursor of the longest a connector may send.
///
/// Only a commit releases them, so a commit is due once they take half the share, and a cursor
/// larger than the share is refused. A cursor waits for no push: its share is its own.
pub(crate) const CURSOR_SHARE: u64 = 64;

/// What commits record of tables, each table's schema and names, from the schema change that
/// makes a record until the commit recording it lands: a 32nd of the budget.
///
/// A table whose records take more than the share is refused at its schema change, with
/// `table_exceeds_budget`, before the destination or any commit sees it; and the columns a
/// connector is told a schema may hold are what the share admits.
pub(crate) const TABLE_SHARE: u64 = 32;

/// Bytes: what a commit records of a column, its type and its two names, at most for names of
/// up to a hundred and fifty bytes: what the columns a schema may hold are derived from.
pub(crate) const COLUMN_RECORD: u64 = 512;

/// What reads keep beside their events for as long as they last, as a remote read's decoder
/// keeps its dictionaries and its schema: a quarter of the budget for all reads together.
///
/// It is divided into a part for each partition read at once, or into as many parts of twice the
/// protocol's least dictionary as it holds where those are fewer, 4 MiB at the defaults. A read
/// may keep a part, and one that would keep more fails with `limit_exceeded` at the frame that
/// would pass it; followed unbounded reads keep theirs together in the parts no other read holds.
pub(crate) const READ_SHARE: u64 = 4;

/// What decoding a connector's answers holds, from before a message is decoded until it is: a
/// 16th of the budget, 16 MiB of the default 256 MiB.
///
/// A remote connector's catalogs, plans, opened state and other answers decode into many times
/// the bytes they take on the wire; each is charged at what its scan counts before it is decoded.
/// The catalog, state and control message limits a connector is told are what the share holds
/// of one such message decoded, so one that keeps to them is refused nothing; answers wait for
/// no push.
pub(crate) const CONTROL_SHARE: u64 = 16;

/// The most one request for what lowering holds may take: a quarter of the budget.
///
/// One row that takes more to lower fails its write with `row_exceeds_budget`. As much of the
/// budget is never taken by pushes waiting to be lowered, so such a request always fits once
/// the pieces before it are written.
pub(crate) const REQUEST_SHARE: u64 = 4;

/// The most lowering one piece of a unit holds, but for a row that alone takes more: a 16th of
/// the budget.
pub(crate) const PIECE_SHARE: u64 = 16;

/// Bytes: the most lowering one piece, or one row, may take whatever the budget, and so the most
/// text one column of a piece holds: as far as a text array's 32-bit offsets reach.
///
/// A row that alone takes more fails its write with `row_exceeds_budget`, before it is lowered.
pub(crate) const MAX_PIECE_BYTES: u64 = (1 << 31) - 1;

/// Bytes: the fewest a piece holds whatever the budget, so a small budget still lowers rows in
/// useful batches.
pub(crate) const MIN_PIECE: u64 = 64 << 10;

/// The shortest deadline of a wait for room in the memory budget, after which the attempt fails
/// with what held the budget: an hour, twice the default deadline of a connector's call.
///
/// A wait ends sooner once what is in flight is written or a commit lands; one that lasts this
/// long waits for bytes nothing will release.
pub(crate) const BUDGET_WAIT: Duration = Duration::from_secs(3600);

/// Times a schema's message that a read keeps for it: the message, and the schema decoded from
/// it, which takes up to four times as much where its fields are many and their names short.
pub(crate) const SCHEMA_KEPT: u64 = 5;

/// Bytes: what the records of a child table of a few columns take in state, its schema and
/// names, as the child tables a stream may have are derived from the stored state limit.
pub(crate) const TABLE_RECORDS: u64 = 4 << 10;

/// Cells: the most one shred of JSON pushes builds, a cell being a row under a column holding
/// values, at every level of a nested column, a list's items being the rows of its items' level.
///
/// Every row takes a cell in every column of its level, so sparse, wide records would build far
/// more than their text; past this, the pushes fail with `limit_exceeded` before anything is
/// built.
pub(crate) const MAX_CELLS: u64 = 1 << 25;

/// Bytes a JSON push is admitted for, for the batches it becomes, for each byte of its records:
/// its charge less the text itself, which is what each chunk's speculative build may take.
pub(crate) const JSON_ADMITTED: u64 = rdlt_connector::cost::JSON_CHARGE - 1;

/// Bytes: the most of a record's content an error quotes, a key or a number, shown as text a
/// connector sent is.
pub(crate) const QUOTED_BYTES: usize = 128;

/// Source plans of an attempt's streams awaited at once, at most: far fewer than the calls a
/// served connection holds open (ADR 0042), and each answer is charged to the share of answers
/// being decoded before it is decoded, so more at once would only wait there.
pub(crate) const PLANS_AT_ONCE: NonZeroUsize = NonZeroUsize::new(16).expect("sixteen is not zero");

/// Calls the engine has in flight at once into a source beside its reads, and into a destination
/// beside its open writers: the coordinator's plan or acknowledgement, and a session's schema
/// change, commit or close, each in turn; a writer's opening is that writer's own call.
pub(crate) const OTHER_CALLS: usize = 1;

/// The fewest calls a connector carries at once that a run can use: a read or a writer, and the
/// engine's [`OTHER_CALLS`] beside it.
///
/// An attempt refuses a connector carrying fewer as it starts.
pub(crate) const LEAST_CALLS: usize = OTHER_CALLS + 1;

/// The reads, or the open writers, a connector carrying `calls` at once leaves the engine: all
/// but its [`OTHER_CALLS`], for a connector carrying at least [`LEAST_CALLS`].
pub(crate) fn carried(calls: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::new(calls.get().saturating_sub(OTHER_CALLS)).unwrap_or(NonZeroUsize::MIN)
}

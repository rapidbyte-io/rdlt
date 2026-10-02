//! Limits the engine enforces on what it holds: the shares its memory budget is divided into,
//! and how long a request waits for one.
//!
//! Each share is a fraction of the budget, given by its denominator, and is its holders' alone:
//! bytes of one share are never reserved from another, so the shares together never pass the
//! budget.

use std::time::Duration;

/// The cursors of the seals waiting for a commit: a 64th of the budget, 4 MiB of the default
/// 256 MiB, which is one cursor of the longest a connector may send.
///
/// Only a commit releases them, so a commit is due once they take half the share, and a cursor
/// larger than the share is refused. A cursor waits for no push: its share is its own.
pub(crate) const CURSOR_SHARE: u64 = 64;

/// The log's seal and commit frames, from before they are encoded until they are appended: a
/// 16th of the budget, four times the cursors' share, since a seal's frame records two cursors,
/// each twice over in base64.
pub(crate) const LOG_SHARE: u64 = 16;

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
/// Each read may keep the share divided by the partitions read at once, 4 MiB at the defaults,
/// and a read that would keep more fails with `limit_exceeded` at the frame that would pass it.
pub(crate) const READ_SHARE: u64 = 4;

/// What decoding a connector's answers holds, from before a message is decoded until it is: a
/// 16th of the budget, 16 MiB of the default 256 MiB.
///
/// A remote connector's catalogs, plans, opened state and other answers decode into many times
/// the bytes they take on the wire; each is charged at what its scan counts before it is decoded,
/// and waits for no push.
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

/// Bytes: the fewest a piece holds whatever the budget, so a small budget still lowers rows in
/// useful batches.
pub(crate) const MIN_PIECE: u64 = 64 << 10;

/// How long a request waits for room in the memory budget by default, before the attempt fails
/// with what held the budget: an hour, longer than any call of a destination may take by default,
/// the thirty minutes of a commit.
///
/// A wait ends sooner once what is in flight is written or a commit lands; one that lasts this
/// long waits for bytes nothing will release.
pub(crate) const BUDGET_WAIT: Duration = Duration::from_secs(3600);

/// Times a schema's message that a read keeps for it: the message, and the schema decoded from
/// it, which takes up to four times as much where its fields are many and their names short.
pub(crate) const SCHEMA_KEPT: u64 = 5;

/// Bytes a frame of the log takes for each byte of a cursor or state value it records: the value
/// is base64 text in its record, and the record base64 text in the frame.
pub(crate) const RECORDED: u64 = 2;

/// The code of the error for a configuration whose memory admits less than the protocol's
/// least frame.
pub(crate) const MEMORY_BELOW_MINIMUM: &str = "memory_below_minimum";

/// The code of the error for a followed unbounded read beyond what a run's partitions leave room
/// for: each holds a slot as long as the run, and one slot must stay for every other read.
pub(crate) const PARTITIONS_TOO_FEW: &str = "partitions_too_few";

/// The code of the error for a table whose records take more than the tables' share.
pub(crate) const TABLE_EXCEEDS_BUDGET: &str = "table_exceeds_budget";

/// The code of the error a wait on the memory budget ends with at its deadline.
pub(crate) const BUDGET_WAIT_EXCEEDED: &str = "memory_budget_wait_exceeded";

/// The code of the error for a push that keeps more alive than pushes may take of the budget.
pub(crate) const PUSH_EXCEEDS_BUDGET: &str = "push_exceeds_budget";

/// The code of the error for one row that takes more to lower than a request may take.
pub(crate) const ROW_EXCEEDS_BUDGET: &str = "row_exceeds_budget";

/// The code of the error for a seal's or a commit's frame that takes more than the log's share.
pub(crate) const LOG_FRAME_EXCEEDS_BUDGET: &str = "log_frame_exceeds_budget";

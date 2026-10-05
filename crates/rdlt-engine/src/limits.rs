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

/// The code of the error for a change that makes a table wider than the schema columns a
/// connector may send, its nested fields counted.
pub(crate) const TABLE_COLUMNS_EXCEEDED: &str = "table_columns_exceeded";

/// The code of the error for a child table beyond those a normalized stream may add.
pub(crate) const CHILD_TABLES_EXCEEDED: &str = "child_tables_exceeded";

/// Bytes: what the records of a child table of a few columns take in state, its schema and
/// names, as the child tables a stream may have are derived from the stored state limit.
pub(crate) const TABLE_RECORDS: u64 = 4 << 10;

/// The code of the error for a commit that would leave more state, or send a larger request,
/// than a message carrying state may take.
pub(crate) const STATE_BYTES_EXCEEDED: &str = "state_bytes_exceeded";

/// The code of the error for a change to a table whose schema version is the last one counts.
pub(crate) const SCHEMA_VERSION_EXHAUSTED: &str = "schema_version_exhausted";

/// The code of the error a wait on the memory budget ends with at its deadline.
pub(crate) const BUDGET_WAIT_EXCEEDED: &str = "memory_budget_wait_exceeded";

/// The code of the error for a push that keeps more alive than pushes may take of the budget.
pub(crate) const PUSH_EXCEEDS_BUDGET: &str = "push_exceeds_budget";

/// The code of the error for JSON pushes whose batches take more beyond what the pushes were
/// admitted for than a request may take.
pub(crate) const JSON_EXCEEDS_BUDGET: &str = "json_exceeds_budget";

/// The code of the error for one row that takes more to lower than a request may take.
pub(crate) const ROW_EXCEEDS_BUDGET: &str = "row_exceeds_budget";

/// The code of the error for a seal's or a commit's frame that takes more than the log's share.
pub(crate) const LOG_FRAME_EXCEEDS_BUDGET: &str = "log_frame_exceeds_budget";

/// Cells: the most one shred of JSON pushes builds, a cell being a row under a column holding
/// values, at every level of a nested column, a list's items being the rows of its items' level.
///
/// Every row takes a cell in every column of its level, so sparse, wide records would build far
/// more than their text; past this, the pushes fail with `limit_exceeded` before anything is
/// built.
pub(crate) const MAX_CELLS: u64 = 1 << 25;

/// Bytes: the most of a record's content an error quotes, a key or a number, shown as text a
/// connector sent is.
pub(crate) const QUOTED_BYTES: usize = 128;
/// The code of the error for a directory or file of a local log that is not its user's alone:
/// another user owns it, or its group or others may reach it, or it is a link, or of another
/// kind than the log keeps there.
pub(crate) const WAL_NOT_PRIVATE: &str = "wal_not_private";

/// The code of the error for a name a local log's store never writes, in a directory it keeps a
/// log's files in: such a file is refused, never read.
pub(crate) const WAL_STRAY: &str = "wal_stray";

/// The code of the error for a load whose log a replay took over: another attempt of the
/// pipeline runs, and this one stops before it answers its commit.
pub(crate) const WAL_FENCED: &str = "wal_fenced";

/// The code of the error for an attempt whose replay found another load of the pipeline still
/// writing its log: retryable, since the attempt waits for that load to end.
pub(crate) const WAL_RUNNING: &str = "wal_running";

/// The code of the error for an attempt whose log store is not the store its destination names
/// for the pipeline's logs: not retryable, as the engine's configuration decides the store.
pub(crate) const WAL_STORE_OTHER: &str = "wal_store_other";

/// The code of the error for a log this engine cannot read, which an operator removes: a chunk
/// missing, damaged, of another format, or holding a commit not all of which is there.
pub(crate) const WAL_UNREADABLE: &str = "wal_unreadable";

/// The code of the error for a log holding a chunk of another pipeline's or load's log.
pub(crate) const WAL_FOREIGN: &str = "wal_foreign";

/// The code of the error for a logged batch that takes more than one request for lowering may
/// take of the memory budget: the log was written under more memory than replays it.
pub(crate) const REPLAY_EXCEEDS_BUDGET: &str = "replay_exceeds_budget";

/// The code of the error for a write-ahead log store that stages more in memory than half the
/// budget's share for the log, which the log's seal and commit frames need beside it.
pub(crate) const WAL_STAGING_EXCEEDS_BUDGET: &str = "wal_staging_exceeds_budget";

/// Bytes: what a load's write-ahead log holds on disk at most by default, 4 GiB: four commits'
/// worth at the default policy's byte threshold.
pub(crate) const LOG_BYTES: u64 = 4 << 30;

/// Parts of what a load's write-ahead log may hold that each of these takes, 8: the room a batch
/// keeps for a carry, which gathers many small chunks into one, and the least it keeps for a
/// commit.
pub(crate) const LOG_PARTS: u64 = 8;

/// The code of the error for a batch whose frame would take its load's write-ahead log past
/// what it may hold on disk: its source sent that much without a checkpoint a commit could take.
pub(crate) const LOG_BYTES_EXCEEDED: &str = "log_bytes_exceeded";

/// The code of the error for a write-ahead log whose disk is full: retryable, since the failed
/// write gives back what its chunk staged, and the next attempt deletes what a crashed load
/// staged before it needs room of its own: its log's directory and its fences.
pub(crate) const WAL_STORAGE_FULL: &str = "wal_storage_full";

/// The code of the error for a write-ahead log kept in an object store that does not do what the
/// log needs of it: refuse a second create of one name, list an object once it is written,
/// take an upload of several parts, or answer that a deleted object is not found.
#[cfg(feature = "object-store")]
pub(crate) const WAL_STORAGE_UNSUPPORTED: &str = "wal_storage_unsupported";

/// The code of the error for a write-ahead log's object store that refused the engine's
/// credentials or what they may do: not retryable, as its operator grants what they may.
#[cfg(feature = "object-store")]
pub(crate) const WAL_STORAGE_DENIED: &str = "wal_storage_denied";

/// The code of the error for a write-ahead log's object store whose client refused a request
/// for good, as a certificate no trusted root signs or a request the store answers as
/// malformed: not retryable, as its configuration decides it.
#[cfg(feature = "object-store")]
pub(crate) const WAL_STORAGE_REFUSED: &str = "wal_storage_refused";

/// The code of the error for a write-ahead log's object store that failed or did not answer
/// every attempt of a request: retryable, as the next attempt of the run may find it well.
#[cfg(feature = "object-store")]
pub(crate) const WAL_STORAGE_UNAVAILABLE: &str = "wal_storage_unavailable";

/// The code of the error for an object store's log prefix that is empty, too long, or holds an
/// empty, `.` or `..` segment or a character beyond `[A-Za-z0-9._-]` and `/` between segments.
#[cfg(feature = "object-store")]
pub(crate) const WAL_PREFIX_INVALID: &str = "wal_prefix_invalid";

/// Bytes: the longest prefix an object store's logs are kept under.
#[cfg(feature = "object-store")]
pub(crate) const WAL_PREFIX_BYTES: usize = 512;

/// Bytes: what an object-store log's staged chunk holds in memory before it uploads a part of
/// it, by default 8 MiB: a chunk this long or shorter is published by one request, a longer one
/// is uploaded in parts of this length, each at least the 5 MiB S3 takes, and published by a
/// request naming them, so a staging never holds much more than a part.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_PART_BYTES: usize = 8 << 20;

/// The most parts an upload may take, S3's limit: a chunk holds at most this many parts.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_PARTS: u64 = 10_000;

/// How many times an object store's request is tried before its failure is reported, by default.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_ATTEMPTS: u32 = 5;

/// The longest wait before the first retry of an object store's request, by default; each
/// retry after waits a random time up to twice the longest its predecessor might, up to
/// [`OBJECT_BACKOFF_MOST`].
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_BACKOFF: Duration = Duration::from_millis(100);

/// The longest wait before any retry of an object store's request, by default.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_BACKOFF_MOST: Duration = Duration::from_secs(5);

/// How long an object store's request that moves no data may take before it is given up, by
/// default; one that moves data is given [`OBJECT_REQUEST_PER_MIB`] more for each MiB.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_REQUEST: Duration = Duration::from_secs(30);

/// How much longer an object store's request may take for each MiB it moves, by default: a
/// transfer no slower than a MiB a second ends within its deadline.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_REQUEST_PER_MIB: Duration = Duration::from_secs(1);

/// Objects a listing of an object-store log's directory holds at most: a directory holding more
/// is refused as `wal_unreadable`, which an operator clears, rather than held in memory.
///
/// Each chunk's end names the live chunks before it, two bytes at least each, so a log keeps at
/// most about the square root of its bound in chunks, each a head and at most a body: 65,536 at
/// the default bound of 4 GiB.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_LISTED: usize = 65_536;

/// Rounds of creates of one fresh name racing an object store's probe makes, each of which must
/// take exactly one.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_PROBE_ROUNDS: usize = 3;

/// Creates racing each round of an object store's probe.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_PROBE_RACERS: usize = 4;

/// Chunks whose kind, inline or uploaded in parts, an object-store log remembers, so a chunk's
/// reads after its first ask for no more than its bytes.
#[cfg(feature = "object-store")]
pub(crate) const OBJECT_HEADS: usize = 4_096;

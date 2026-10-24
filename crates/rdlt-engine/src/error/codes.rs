//! Every code an engine [`Error`](crate::Error) may carry: the catalogue, and the module that
//! alone makes a [`Code`].
//!
//! A code names one failure for as long as the engine has it, and each constant is named after
//! its text. A connector's own code is never one of these: an error keeps it apart, as
//! [`Error::connector_code`](crate::Error::connector_code).

use std::fmt;

use serde::Serialize;

/// A stable machine code for an engine error, one of this module's constants.
///
/// ```
/// use rdlt_engine::codes;
///
/// assert_eq!(codes::STATE_INVALID.as_str(), "state_invalid");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct Code(&'static str);

impl Code {
    /// The code's text, as reports show it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// The code of the error for a batch policy whose counts or sizes are not more than zero.
pub const BATCH_POLICY_INVALID: Code = Code("batch_policy_invalid");

/// The code of the error for a source's batch holding a column of no table schema.
pub const BATCH_SCHEMA_INVALID: Code = Code("batch_schema_invalid");

/// The code of the error for identifier rules of a destination's that the engine refuses.
pub const CAPABILITIES_INVALID: Code = Code("capabilities_invalid");

/// The code of the error for a change stream merged by key into a destination that does not
/// merge change streams.
pub const CHANGE_MERGE_UNSUPPORTED: Code = Code("change_merge_unsupported");

/// The code of the error for a history stream's change time that is no time, or one that
/// microseconds since the epoch cannot hold, or a source column that cannot be one.
pub const CHANGE_TIME_INVALID: Code = Code("change_time_invalid");

/// The code of the error for a batch of a history stream without its change time column.
pub const CHANGE_TIME_MISSING: Code = Code("change_time_missing");

/// The code of the error for a change of a history stream with no change time, where nothing else
/// says when its version begins.
pub const CHANGE_TIME_NULL: Code = Code("change_time_null");

/// The code of the error for a child table beyond those a normalized stream may add.
pub const CHILD_TABLES_EXCEEDED: Code = Code("child_tables_exceeded");

/// The code of the error refusing a source column that asks for a metadata column's name.
pub const COLUMN_NAME_RESERVED: Code = Code("column_name_reserved");

/// The code of the error for a commit policy that sets no threshold, or one of zero.
pub const COMMIT_POLICY_INVALID: Code = Code("commit_policy_invalid");

/// The code of the error for an engine configuration with a count or size that is not more than
/// zero, or a retry policy whose first delay is longer than its longest.
pub const CONFIG_INVALID: Code = Code("config_invalid");

/// The code of the error for a call into a connector that took longer than the engine's wait for
/// it; a connector's own deadline, or its host's, is the connector's code of the same text.
pub const DEADLINE_EXCEEDED: Code = Code(rdlt_connector::DEADLINE_EXCEEDED);

/// The code of the error for a change stream whose deletes or truncates remove rows in a way the
/// destination cannot.
pub const DELETE_MODE_UNSUPPORTED: Code = Code("delete_mode_unsupported");

/// The code of the error for a reset of a stream's tables at a destination that cannot drop
/// tables.
pub const DROP_UNSUPPORTED: Code = Code("drop_unsupported");

/// The code of the error for a full read of a stream whose source cannot read again what it
/// acknowledged.
pub const FULL_READ_UNREPLAYABLE: Code = Code("full_read_unreplayable");

/// The code of the error for growth limits of zero.
pub const GROWTH_LIMITS_INVALID: Code = Code("growth_limits_invalid");

/// The code of the error for a history stream set to normalize, which it cannot yet.
pub const HISTORY_NORMALIZE_UNSUPPORTED: Code = Code("history_normalize_unsupported");

/// The code of the error for a history stream into a destination that stores neither timestamps
/// nor 64-bit integers, one of which holds when its versions begin and end.
pub const HISTORY_VALIDITY_UNSUPPORTED: Code = Code("history_validity_unsupported");

/// The code of the error for a table or column the destination's identifier rules leave no free
/// identifier for.
pub const IDENTIFIER_EXHAUSTED: Code = Code("identifier_exhausted");

/// The code of the error for a JSON record that names one key twice.
pub const JSON_DUPLICATE_KEY: Code = Code("json_duplicate_key");

/// The code of the error for JSON pushes whose batches take more beyond what the pushes were
/// admitted for than a request may take of the memory budget.
pub const JSON_EXCEEDS_BUDGET: Code = Code("json_exceeds_budget");

/// The code of the error for JSON text that is not JSON.
pub const JSON_INVALID: Code = Code("json_invalid");

/// The code of the error for a JSON record that is not an object.
pub const JSON_NOT_OBJECT: Code = Code("json_not_object");

/// The code of the error for JSON beyond a limit the engine reads it within, such as its nesting
/// or its cells; the wire's refusals of a value beyond a limit share the text.
pub const LIMIT_EXCEEDED: Code = Code(rdlt_wire::limits::LIMIT_EXCEEDED);

/// The code of the error for a batch whose frame would take its load's write-ahead log past what
/// it may hold on disk: its source sent that much without a checkpoint a commit could take.
pub const LOG_BYTES_EXCEEDED: Code = Code("log_bytes_exceeded");

/// The code of the error for a seal's, commit's or table's frame that takes more than the log's
/// share of the memory budget.
pub const LOG_FRAME_EXCEEDS_BUDGET: Code = Code("log_frame_exceeds_budget");

/// The code of the error for a configuration whose memory admits less than the least limits the
/// protocol lets a peer set.
pub const MEMORY_BELOW_MINIMUM: Code = Code("memory_below_minimum");

/// The code of the error a wait on the memory budget ends with at its deadline.
pub const MEMORY_BUDGET_WAIT_EXCEEDED: Code = Code("memory_budget_wait_exceeded");

/// The code of the error for a merge key column whose type would change to one in which its
/// values no longer match its stored rows.
pub const MERGE_KEY_CHANGED: Code = Code("merge_key_changed");

/// The code of the error for a merge key stored as JSON for a normalized stream's table.
pub const MERGE_KEY_JSON: Code = Code("merge_key_json");

/// The code of the error for a merging stream with no key, or a batch without a key column.
pub const MERGE_KEY_MISSING: Code = Code("merge_key_missing");

/// The code of the error for a merge key holding a NaN, which equals no value.
pub const MERGE_KEY_NAN: Code = Code("merge_key_nan");

/// The code of the error for a normalized stream's merge key holding objects or arrays.
pub const MERGE_KEY_NESTED: Code = Code("merge_key_nested");

/// The code of the error for a merge key holding a null.
pub const MERGE_KEY_NULL: Code = Code("merge_key_null");

/// The code of the error for a change that flags a key column unchanged.
pub const MERGE_KEY_UNCHANGED: Code = Code("merge_key_unchanged");

/// The code of the error for a reset that names no streams.
pub const NO_STREAMS: Code = Code("no_streams");

/// The code of the error for a change stream set to normalize, which it cannot yet.
pub const NORMALIZE_CHANGES_UNSUPPORTED: Code = Code("normalize_changes_unsupported");

/// The code of the error for updates that leave columns unchanged, which the destination cannot
/// keep.
pub const PARTIAL_UPDATES_UNSUPPORTED: Code = Code("partial_updates_unsupported");

/// The code of the error for a followed unbounded read beyond what a run's partitions leave room
/// for: each holds a slot as long as the run, and one slot must stay for every other read.
pub const PARTITIONS_TOO_FEW: Code = Code("partitions_too_few");

/// The code of the error for a source planning a phase of a stream not read as changes.
pub const PHASE_UNEXPECTED: Code = Code("phase_unexpected");

/// The code of the error for a plan or a catalog naming a nested column in a stream's settings,
/// hints or key.
pub const PLAN_COLUMN_NESTED: Code = Code("plan_column_nested");

/// The code of the error for a delete or truncate mode on a stream that applies neither.
pub const PLAN_DELETES_UNUSED: Code = Code("plan_deletes_unused");

/// The code of the error for a plan that selects one stream twice.
pub const PLAN_DUPLICATE_STREAM: Code = Code("plan_duplicate_stream");

/// The code of the error for a plan that selects no streams.
pub const PLAN_EMPTY: Code = Code("plan_empty");

/// The code of the error for a column hinted as null.
pub const PLAN_HINT_INVALID: Code = Code("plan_hint_invalid");

/// The code of the error for a source's partition plan the engine refuses, or streams that read
/// more partitions at once than the limit.
pub const PLAN_INVALID: Code = Code("plan_invalid");

/// The code of the error for a merge key that names no column.
pub const PLAN_KEY_EMPTY: Code = Code("plan_key_empty");

/// The code of the error for a key on a stream that matches no rows by key.
pub const PLAN_KEY_UNUSED: Code = Code("plan_key_unused");

/// The code of the error for a write mode the stream's read mode cannot feed.
pub const PLAN_MODE_INVALID: Code = Code("plan_mode_invalid");

/// The code of the error for a column set to normalize, which only pipelines and streams are.
pub const PLAN_NESTED_NORMALIZE_COLUMN: Code = Code("plan_nested_normalize_column");

/// The code of the error for two selected streams that would load one table.
pub const PLAN_TABLE_COLLISION: Code = Code("plan_table_collision");

/// The code of the error for a push the stream's read mode does not take.
pub const PUSH_UNEXPECTED: Code = Code("push_unexpected");

/// The code of the error for a stream its source cannot read in the plan's read mode.
pub const READ_MODE_UNSUPPORTED: Code = Code("read_mode_unsupported");

/// The code of the error for a commit the destination answered with the receipt of another.
pub const RECEIPT_MISMATCH: Code = Code("receipt_mismatch");

/// The code of the error for receipts that count more rows or bytes than a total holds.
pub const RECEIPT_OVERFLOW: Code = Code("receipt_overflow");

/// The code of the error for a logged batch that takes more than one request for lowering may
/// take of the memory budget: the log was written under more memory than replays it.
pub const REPLAY_EXCEEDS_BUDGET: Code = Code("replay_exceeds_budget");

/// The code of the error for a reset of a stream whose source cannot read it again from its
/// beginning, or whose log holds rows that never landed.
pub const RESET_UNREPLAYABLE: Code = Code("reset_unreplayable");

/// The code of the error for reading a partition again from its earliest after a retention loss,
/// on a stream that cannot.
pub const RETENTION_RESET_UNSUPPORTED: Code = Code("retention_reset_unsupported");

/// The code of the error for one row that takes more to lower than a request may take of the
/// memory budget.
pub const ROW_EXCEEDS_BUDGET: Code = Code("row_exceeds_budget");

/// The code of the error for a schema change the destination cannot apply, which the stream's
/// policy refuses a variant column for.
pub const SCHEMA_CHANGE_UNSUPPORTED: Code = Code("schema_change_unsupported");

/// The code of the error for a change to a frozen schema.
pub const SCHEMA_FROZEN: Code = Code("schema_frozen");

/// The code of the error for a change to a table whose schema version is the last one counts.
pub const SCHEMA_VERSION_EXHAUSTED: Code = Code("schema_version_exhausted");

/// The code of the error for a bug in the shredder.
pub const SHRED_INTERNAL: Code = Code("shred_internal");

/// The code of the error for a commit that would leave more state, or send a larger request,
/// than a message carrying state may take.
pub const STATE_BYTES_EXCEEDED: Code = Code("state_bytes_exceeded");

/// The code of the error for pipeline state the destination answered with that the engine
/// cannot read.
pub const STATE_INVALID: Code = Code("state_invalid");

/// The code of the error for a reset of a stream that another recorded stream is displayed as.
pub const STREAM_AMBIGUOUS: Code = Code("stream_ambiguous");

/// The code of the error for a stream the source's catalog lacks, or a reset of one the pipeline
/// recorded nothing of.
pub const STREAM_NOT_FOUND: Code = Code("stream_not_found");

/// The code of the error for a history stream whose table's versions began at another change
/// time.
pub const TABLE_CHANGE_TIME_MISMATCH: Code = Code("table_change_time_mismatch");

/// The code of the error for a change that makes a table wider than the schema columns a
/// connector may send, its nested fields counted.
pub const TABLE_COLUMNS_EXCEEDED: Code = Code("table_columns_exceeded");

/// The code of the error for a table whose records take more than the tables' share of the memory
/// budget.
pub const TABLE_EXCEEDS_BUDGET: Code = Code("table_exceeds_budget");

/// The code of the error for a stream whose table keeps history where the stream does not, or
/// the reverse.
pub const TABLE_HISTORY_MISMATCH: Code = Code("table_history_mismatch");

/// The code of the error for a merge into a table whose normalized roots a load that did not
/// merge identified by their positions, whose children the merge would leave behind.
pub const TABLE_IDENTITY_MISMATCH: Code = Code("table_identity_mismatch");

/// The code of the error for a merging stream whose table's rows were merged by another key.
pub const TABLE_KEY_MISMATCH: Code = Code("table_key_mismatch");

/// The code of the error for an array of a normalized stream that no table can be named after.
pub const TABLE_PATH_INVALID: Code = Code("table_path_invalid");

/// The code of the error for a change stream ordered by its source's positions into a table of
/// rows the engine merged.
pub const TABLE_SEQUENCES_MISMATCH: Code = Code("table_sequences_mismatch");

/// The code of the error for a value of a batch that its column cannot hold.
pub const VALUE_UNREPRESENTABLE: Code = Code("value_unrepresentable");

/// The code of the error for a load whose log a replay took over: another attempt of the pipeline
/// runs, and this one stops before it answers its commit.
pub const WAL_FENCED: Code = Code("wal_fenced");

/// The code of the error for a log holding a chunk of another pipeline's or load's log.
pub const WAL_FOREIGN: Code = Code("wal_foreign");

/// The code of the error for a directory or file of a local log that is not its user's alone:
/// another user owns it, or its group or others may reach it, or it is a link, or of another kind
/// than the log keeps there.
pub const WAL_NOT_PRIVATE: Code = Code("wal_not_private");

/// The code of the error for a stream whose source cannot read again what it acknowledged, where
/// the engine has nowhere to keep the write-ahead log its loads need.
pub const WAL_REQUIRED: Code = Code("wal_required");

/// The code of the error for an attempt whose replay found another load of the pipeline still
/// writing its log: retryable, since the attempt waits for that load to end.
pub const WAL_RUNNING: Code = Code("wal_running");

/// The code of the error for a write-ahead log whose disk is full: retryable, since the failed
/// write gives back what its chunk staged, and the next attempt deletes what a crashed load
/// staged before it needs room of its own.
pub const WAL_STORAGE_FULL: Code = Code("wal_storage_full");

/// The code of the error for a pipeline that keeps a write-ahead log, where the engine has nowhere
/// to keep one.
pub const WAL_STORE_MISSING: Code = Code("wal_store_missing");

/// The code of the error for an attempt whose log store is not the store its destination names
/// for the pipeline's logs: not retryable, as the engine's configuration decides the store.
pub const WAL_STORE_OTHER: Code = Code("wal_store_other");

/// The code of the error for a name a local log's store never writes, in a directory it keeps a
/// log's files in: such a file is refused, never read.
pub const WAL_STRAY: Code = Code("wal_stray");

/// The code of the error for a log this engine cannot read, which an operator removes: a chunk
/// missing, damaged, of another format, or holding a commit not all of which is there.
pub const WAL_UNREADABLE: Code = Code("wal_unreadable");

/// The code of the error for a stream its destination cannot write in the plan's write mode.
pub const WRITE_MODE_UNSUPPORTED: Code = Code("write_mode_unsupported");

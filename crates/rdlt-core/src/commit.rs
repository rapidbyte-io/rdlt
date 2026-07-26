//! The commit protocol vocabulary.

use serde::{Deserialize, Serialize};

use crate::ids::LoadId;
use crate::state::StateDoc;

/// Counters for one commit unit. Feeds `RunReport` accounting — no silent failures.
///
/// Field-identical to [`crate::report::TableReport`] by design, but a different role:
/// `CommitCounters` totals one commit unit across all its tables, whereas a
/// `TableReport` totals one table across the whole run. `From<CommitCounters>` (defined
/// alongside `TableReport`) projects the former into the latter's shape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitCounters {
    /// Rows in this commit unit.
    pub rows: u64,
    /// In-memory bytes of its batches.
    pub bytes: u64,
    /// Whole rows a Discard* policy dropped within it.
    pub discarded_rows: u64,
    /// Individual values a Discard* policy nulled within it.
    pub discarded_values: u64,
}

/// Everything a destination must publish atomically with the data of one commit unit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitMeta {
    /// The run publishing this unit.
    pub load_id: LoadId,
    /// Monotonic within a run; `(load_id, commit_seq)` is the idempotency key.
    pub commit_seq: u64,
    /// The complete new pipeline state (cursors, schema hashes) — stored opaquely by
    /// the destination, in the same transaction as the data.
    pub state: StateDoc,
    /// This unit's totals, for the run report.
    pub counters: CommitCounters,
}

/// Destination acknowledgment. Re-committing the same `(load_id, commit_seq)` MUST
/// return the prior receipt without re-publishing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitReceipt {
    /// The run whose commit is acknowledged.
    pub load_id: LoadId,
    /// The acknowledged commit sequence number.
    pub commit_seq: u64,
}

/// How the engine groups checkpoint spans into commit units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitPolicy {
    /// Commit after every N source checkpoints.
    EveryCheckpoints(u32),
    /// Commit when the accumulated uncommitted batch bytes reach this size.
    EveryBytes(u64),
    /// Commit at least every N seconds of wall clock.
    EverySeconds(u32),
}

impl Default for CommitPolicy {
    /// Commit at every checkpoint: the safest cadence, since the window of work
    /// a crash can force to be re-extracted is one checkpoint wide.
    fn default() -> Self {
        CommitPolicy::EveryCheckpoints(1)
    }
}

/// Per-stream write disposition.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum WriteMode {
    /// New rows are appended.
    #[default]
    Append,
    /// The table contents are replaced by this run's rows.
    Replace,
    /// Upsert by key: an updated record replaces its prior version *and its entire
    /// subtree of child rows* (delete by `_rdlt_root_id`, insert the new subtree).
    ///
    /// Merge-key precedence: `key` and a source's `StreamSpec::primary_key` are
    /// two names for one identity, not independent knobs. For a structured
    /// stream the engine requires them to AGREE (same columns, as a set) and
    /// rejects a mismatch typed at plan time; for a shredded stream `key`
    /// drives `_rdlt_id`. Neither silently overrides the other.
    Merge {
        /// The columns identifying a row.
        key: Vec<String>,
    },
}

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
///
/// Thresholds, ANY of which triggers a commit — whichever is reached
/// first, the way a delivery buffer is usually specified ("100 MB or
/// every 15 minutes"). They are not alternatives to choose between:
/// setting both a size and an interval is the ordinary case, because
/// size bounds how much a crash can cost while the interval bounds
/// how long data can sit unwritten on a quiet stream.
///
/// At least one threshold must be set — see [`CommitPolicy::check`].
/// A policy with none would never commit until the run ended, which
/// is a data-loss window rather than a cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct CommitPolicy {
    /// Commit after this many source checkpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_checkpoints: Option<u32>,
    /// Commit once the uncommitted batch reaches this many bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_bytes: Option<u64>,
    /// Commit at least this often, in seconds of wall clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_seconds: Option<u32>,
}

impl Default for CommitPolicy {
    /// Commit at every checkpoint: the safest cadence, since the window of work
    /// a crash can force to be re-extracted is one checkpoint wide.
    fn default() -> Self {
        Self {
            every_checkpoints: Some(1),
            every_bytes: None,
            every_seconds: None,
        }
    }
}

impl CommitPolicy {
    /// Commit every `n` checkpoints, and nothing else.
    pub fn every_checkpoints(n: u32) -> Self {
        Self {
            every_checkpoints: Some(n),
            ..Self::empty()
        }
    }

    /// Commit every `bytes` of accumulated batch, and nothing else.
    pub fn every_bytes(bytes: u64) -> Self {
        Self {
            every_bytes: Some(bytes),
            ..Self::empty()
        }
    }

    /// Commit every `secs` seconds, and nothing else.
    pub fn every_seconds(secs: u32) -> Self {
        Self {
            every_seconds: Some(secs),
            ..Self::empty()
        }
    }

    /// No thresholds. Only useful as a base for the builders below —
    /// [`check`](Self::check) refuses it.
    fn empty() -> Self {
        Self {
            every_checkpoints: None,
            every_bytes: None,
            every_seconds: None,
        }
    }

    /// Add a byte threshold, keeping any already set.
    #[must_use]
    pub fn or_every_bytes(mut self, bytes: u64) -> Self {
        self.every_bytes = Some(bytes);
        self
    }

    /// Add a time threshold, keeping any already set.
    #[must_use]
    pub fn or_every_seconds(mut self, secs: u32) -> Self {
        self.every_seconds = Some(secs);
        self
    }

    /// Add a checkpoint threshold, keeping any already set.
    #[must_use]
    pub fn or_every_checkpoints(mut self, n: u32) -> Self {
        self.every_checkpoints = Some(n);
        self
    }

    /// Every threshold that would fire, given what has accumulated.
    ///
    /// `true` when ANY is reached — the thresholds are a disjunction,
    /// not a conjunction.
    #[must_use]
    pub fn triggers(&self, checkpoints: u32, bytes: u64, elapsed_secs: u64) -> bool {
        // `max(1)` on the checkpoint count keeps `Some(0)` from
        // meaning "commit before anything happened"; a zero threshold
        // is the same as one.
        self.every_checkpoints
            .is_some_and(|n| checkpoints >= n.max(1))
            || self.every_bytes.is_some_and(|b| bytes >= b)
            || self
                .every_seconds
                .is_some_and(|s| elapsed_secs >= u64::from(s))
    }

    /// Refuse a policy that can never fire.
    ///
    /// A policy with no threshold set does not mean "commit rarely" —
    /// it means the run holds everything uncommitted until it ends, so
    /// a crash costs the whole run. That is a mistake worth naming
    /// rather than honouring.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.every_checkpoints.is_none()
            && self.every_bytes.is_none()
            && self.every_seconds.is_none()
        {
            return Err("commit_policy sets no threshold — give it at least one of \
                 `every_checkpoints`, `every_bytes` or `every_seconds`, or omit \
                 it entirely for the default (every checkpoint)");
        }
        Ok(())
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

#[cfg(test)]
mod commit_policy_tests {
    use super::CommitPolicy;

    /// The thresholds are a DISJUNCTION: whichever is reached first
    /// ends the commit unit. This is the property the type exists for
    /// — "100 MB or every 15 minutes" is one policy, not two.
    #[test]
    fn any_threshold_alone_triggers() {
        let policy = CommitPolicy::every_bytes(100).or_every_seconds(900);

        // Bytes reached, time nowhere near.
        assert!(policy.triggers(0, 100, 0));
        // Time reached, bytes nowhere near.
        assert!(policy.triggers(0, 0, 900));
        // Neither.
        assert!(!policy.triggers(0, 99, 899));
    }

    /// An unset threshold never fires — it is absent, not zero.
    #[test]
    fn an_unset_threshold_is_not_a_zero_threshold() {
        let bytes_only = CommitPolicy::every_bytes(1_000);
        // A checkpoint count that would trip `every_checkpoints: 1`
        // must not trip a policy that never asked for one.
        assert!(!bytes_only.triggers(99, 0, 0));
        assert!(!bytes_only.triggers(0, 0, 86_400));
        assert!(bytes_only.triggers(0, 1_000, 0));
    }

    /// `every_checkpoints: 0` means every checkpoint, not "before
    /// anything happened" — a zero threshold that fired on an empty
    /// span would commit in a loop.
    #[test]
    fn a_zero_checkpoint_threshold_still_needs_one_checkpoint() {
        let policy = CommitPolicy::every_checkpoints(0);
        assert!(!policy.triggers(0, 0, 0));
        assert!(policy.triggers(1, 0, 0));
    }

    /// The default is the safe one: commit at every checkpoint, so a
    /// crash costs at most one checkpoint of re-extraction.
    #[test]
    fn the_default_commits_every_checkpoint() {
        let policy = CommitPolicy::default();
        assert_eq!(policy.every_checkpoints, Some(1));
        assert!(policy.triggers(1, 0, 0));
        policy.check().expect("the default is a usable policy");
    }

    /// A policy with NO threshold is refused rather than honoured: it
    /// would hold everything uncommitted until the run ended, so a
    /// crash costs the whole run.
    #[test]
    fn a_policy_with_no_threshold_is_refused() {
        let empty: CommitPolicy =
            serde_json::from_str("{}").expect("an empty document deserializes");
        assert!(!empty.triggers(u32::MAX, u64::MAX, u64::MAX));
        let err = empty.check().expect_err("must be refused");
        assert!(err.contains("no threshold"), "{err}");
    }

    /// The wire form is the one a pipeline document writes.
    #[test]
    fn the_document_form_reads_as_written() {
        let policy: CommitPolicy =
            serde_yaml::from_str("every_bytes: 104857600\nevery_seconds: 900\n").expect("parses");
        assert_eq!(policy.every_bytes, Some(104_857_600));
        assert_eq!(policy.every_seconds, Some(900));
        assert_eq!(policy.every_checkpoints, None);
        policy.check().expect("valid");
    }
}

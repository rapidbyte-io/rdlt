//! What a run reports: its attempts and what they committed, counted only from receipts.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use rdlt_connector::{CommitSeq, LoadId, PipelineId, Receipt, StreamName};
use serde::Serialize;

use crate::error::ErrorReport;

/// How a run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Every stream was read to its end and committed.
    Succeeded,
    /// The run stopped on request after committing what was sealed.
    Stopped,
    /// An attempt failed and no retry was left or allowed.
    Failed,
    /// The run was cancelled; uncommitted work is discarded at the next open.
    Cancelled,
}

/// The outcome of a run: every attempt and what it committed.
///
/// Row and byte totals come only from commit receipts, so they match what the destination
/// publishes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Report {
    /// The pipeline.
    pub pipeline: PipelineId,
    /// How the run ended.
    pub status: RunStatus,
    /// The latest attempts, in order: at most [`REPORTED_ATTEMPTS`], so a run that retries for
    /// ever keeps a bounded report.
    pub attempts: Vec<AttemptReport>,
    /// Every attempt the run made, those the report no longer lists included.
    pub attempted: u64,
    /// Wall-clock time from the run's start to its end.
    pub elapsed: Duration,
    /// Rows committed, from receipts.
    pub rows: u64,
    /// Bytes committed, from receipts.
    pub bytes: u64,
    /// Commits made.
    pub commits: u64,
    /// The most bytes of in-flight batches the run held at once.
    pub peak_memory: u64,
    /// What each stream committed, by stream name.
    pub streams: BTreeMap<String, StreamReport>,
}

/// How many of a run's latest attempts its report lists.
pub const REPORTED_ATTEMPTS: usize = 128;

/// One attempt of a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AttemptReport {
    /// The attempt's load.
    pub load_id: LoadId,
    /// When the attempt started.
    pub started_at: SystemTime,
    /// When the attempt ended.
    pub ended_at: SystemTime,
    /// Commits the attempt made.
    pub commits: u64,
    /// Rows the attempt committed, from receipts.
    pub rows: u64,
    /// Bytes the attempt committed, from receipts.
    pub bytes: u64,
    /// Why the attempt failed, if it did.
    pub error: Option<ErrorReport>,
}

/// What one stream committed during a run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StreamReport {
    /// Rows committed, to the stream's table and, for a normalized stream, its child tables.
    pub rows: u64,
    /// Their bytes in memory.
    pub bytes: u64,
    /// Commits that published the stream's rows.
    pub commits: u64,
    /// Replace generations swapped in.
    pub generations_swapped: u64,
    /// Rows the schema policy dropped from committed segments.
    pub discarded_rows: u64,
    /// Values the schema policy nulled in committed segments.
    pub discarded_values: u64,
    /// Deletes a change stream ignores, dropped from committed segments.
    pub deletes_ignored: u64,
    /// Truncates a change stream ignores, dropped from committed segments.
    pub truncates_ignored: u64,
    /// How many records the stream's reads were last behind its source's newest, over its
    /// partitions, as its source measures it; none where it never said.
    pub behind: Option<u64>,
    /// Reads of the stream's partitions that started again from their earliest, where the
    /// source's retention had dropped where they would resume.
    pub retention_resets: u64,
}

/// How an attempt that did not fail ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AttemptEnd {
    /// Every partition was read to its end.
    Exhausted,
    /// Reading stopped on request.
    Stopped,
}

/// One commit and what each stream contributed to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommitRecord {
    pub(crate) receipt: Receipt,
    pub(crate) streams: BTreeMap<StreamName, StreamReport>,
}

/// What an attempt committed, folded as each commit lands: an attempt that commits for ever
/// keeps its totals, not every commit.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Committed {
    pub(crate) commits: u64,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) streams: BTreeMap<StreamName, StreamReport>,
}

impl Committed {
    /// Folds `commit` in.
    pub(crate) fn add(&mut self, commit: CommitRecord) {
        self.commits += 1;
        self.rows += commit.receipt.rows;
        self.bytes += commit.receipt.bytes;
        for (stream, counts) in commit.streams {
            self.streams.entry(stream).or_default().absorb(&counts);
        }
    }
}

impl StreamReport {
    /// Adds what `other` counts.
    fn absorb(&mut self, other: &Self) {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.commits += other.commits;
        self.generations_swapped += other.generations_swapped;
        self.discarded_rows += other.discarded_rows;
        self.discarded_values += other.discarded_values;
        self.deletes_ignored += other.deletes_ignored;
        self.truncates_ignored += other.truncates_ignored;
    }
}

/// What an attempt did, recorded as it happens so a failed attempt still reports its commits.
#[derive(Debug, Default)]
pub(crate) struct AttemptLog {
    pub(crate) committed: Committed,
    /// Whether a commit of the attempt moved the load on: published a row, or recorded anything
    /// but a partition where it already stood; or the attempt landed rows an earlier one logged.
    pub(crate) progressed: bool,
    pub(crate) end: Option<AttemptEnd>,
    /// The commit in flight, whose response has not arrived.
    pub(crate) pending: Option<CommitRecord>,
    /// The receipt state recorded when the attempt opened, naming the last commit that landed.
    pub(crate) opened: Option<(LoadId, CommitSeq)>,
    /// How many records each stream's reads were last behind their source's newest.
    pub(crate) behind: BTreeMap<StreamName, u64>,
    /// Each stream's reads that started again from their earliest after a retention loss.
    pub(crate) retention_resets: BTreeMap<StreamName, u64>,
}

/// A finished attempt.
#[derive(Debug)]
pub(crate) struct AttemptRecord {
    pub(crate) load_id: LoadId,
    pub(crate) started_at: SystemTime,
    pub(crate) ended_at: SystemTime,
    pub(crate) log: AttemptLog,
    pub(crate) error: Option<ErrorReport>,
}

impl Report {
    /// A report of `pipeline` that has folded no attempt yet.
    pub(crate) fn new(pipeline: PipelineId) -> Self {
        Self {
            pipeline,
            status: RunStatus::Succeeded,
            attempts: Vec::new(),
            attempted: 0,
            elapsed: Duration::ZERO,
            rows: 0,
            bytes: 0,
            commits: 0,
            peak_memory: 0,
            streams: BTreeMap::new(),
        }
    }

    /// Folds `attempt` in, listing it among the latest [`REPORTED_ATTEMPTS`].
    pub(crate) fn absorb(&mut self, attempt: AttemptRecord) {
        for (stream, behind) in &attempt.log.behind {
            self.streams.entry(stream.to_string()).or_default().behind = Some(*behind);
        }
        for (stream, resets) in &attempt.log.retention_resets {
            self.streams
                .entry(stream.to_string())
                .or_default()
                .retention_resets += resets;
        }
        let committed = attempt.log.committed;
        for (stream, counts) in &committed.streams {
            let total = self.streams.entry(stream.to_string()).or_default();
            total.absorb(counts);
        }
        self.rows += committed.rows;
        self.bytes += committed.bytes;
        self.commits += committed.commits;
        self.attempted += 1;
        self.attempts.push(AttemptReport {
            load_id: attempt.load_id,
            started_at: attempt.started_at,
            ended_at: attempt.ended_at,
            commits: committed.commits,
            rows: committed.rows,
            bytes: committed.bytes,
            error: attempt.error,
        });
        if self.attempts.len() > REPORTED_ATTEMPTS {
            self.attempts.remove(0);
        }
    }

    /// Credits `commit` to the folded attempt whose load its receipt names, once a later attempt
    /// found it landed.
    pub(crate) fn credit(&mut self, commit: CommitRecord) {
        let load = commit.receipt.load_id;
        let mut committed = Committed::default();
        committed.add(commit);
        for (stream, counts) in &committed.streams {
            self.streams
                .entry(stream.to_string())
                .or_default()
                .absorb(counts);
        }
        self.rows += committed.rows;
        self.bytes += committed.bytes;
        self.commits += committed.commits;
        if let Some(listed) = self
            .attempts
            .iter_mut()
            .find(|attempt| attempt.load_id == load)
        {
            listed.commits += committed.commits;
            listed.rows += committed.rows;
            listed.bytes += committed.bytes;
        }
    }

    /// Folds `attempts` into the run's report.
    #[cfg(test)]
    pub(crate) fn fold(
        pipeline: PipelineId,
        status: RunStatus,
        attempts: Vec<AttemptRecord>,
        elapsed: Duration,
        peak_memory: u64,
    ) -> Self {
        let mut report = Self::new(pipeline);
        for attempt in attempts {
            report.absorb(attempt);
        }
        report.status = status;
        report.elapsed = elapsed;
        report.peak_memory = peak_memory;
        report
    }
}

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
    /// Every attempt, in order.
    pub attempts: Vec<AttemptReport>,
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
    /// Rows committed.
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

/// What an attempt did, recorded as it happens so a failed attempt still reports its commits.
#[derive(Debug, Default)]
pub(crate) struct AttemptLog {
    pub(crate) commits: Vec<CommitRecord>,
    pub(crate) end: Option<AttemptEnd>,
    /// The commit in flight, whose response has not arrived.
    pub(crate) pending: Option<CommitRecord>,
    /// The receipt state recorded when the attempt opened, naming the last commit that landed.
    pub(crate) opened: Option<(LoadId, CommitSeq)>,
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
    /// Folds `attempts` into the run's report.
    pub(crate) fn fold(
        pipeline: PipelineId,
        status: RunStatus,
        attempts: Vec<AttemptRecord>,
        elapsed: Duration,
        peak_memory: u64,
    ) -> Self {
        let mut report = Self {
            pipeline,
            status,
            attempts: Vec::with_capacity(attempts.len()),
            elapsed,
            rows: 0,
            bytes: 0,
            commits: 0,
            peak_memory,
            streams: BTreeMap::new(),
        };
        for attempt in attempts {
            let mut summary = AttemptReport {
                load_id: attempt.load_id,
                started_at: attempt.started_at,
                ended_at: attempt.ended_at,
                commits: 0,
                rows: 0,
                bytes: 0,
                error: attempt.error,
            };
            for commit in attempt.log.commits {
                summary.commits += 1;
                summary.rows += commit.receipt.rows;
                summary.bytes += commit.receipt.bytes;
                for (stream, counts) in commit.streams {
                    let total = report.streams.entry(stream.to_string()).or_default();
                    total.rows += counts.rows;
                    total.bytes += counts.bytes;
                    total.commits += counts.commits;
                    total.generations_swapped += counts.generations_swapped;
                    total.discarded_rows += counts.discarded_rows;
                    total.discarded_values += counts.discarded_values;
                }
            }
            report.rows += summary.rows;
            report.bytes += summary.bytes;
            report.commits += summary.commits;
            report.attempts.push(summary);
        }
        report
    }
}

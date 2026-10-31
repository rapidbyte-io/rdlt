//! What a run tells its environment as it happens: each attempt it starts and retries, each
//! commit that lands, and each fact an operator reads, through [`Env::observe`].

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;
use rdlt_connector::PipelineId;
use serde::Serialize;

use crate::env::Env;
use crate::error::ErrorReport;
use crate::limits::REPORTED_LISTED;
use crate::report::{Acks, CommitPhases, RunStatus, StreamReport, Trigger};

/// One thing a run did, told as it happens.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Event {
    /// When it happened, on the run's environment's wall clock.
    pub at: SystemTime,
    /// The pipeline the run loads.
    pub pipeline: PipelineId,
    /// The run's attempt it happened in, the first being 1: for what happens after an attempt
    /// ended, the attempt that ended.
    pub attempt: u64,
    /// What happened.
    pub kind: EventKind,
}

/// What happened.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// The attempt started: it opens the destination next.
    AttemptStarted,
    /// A commit landed, and its cursors were reported to the source.
    Committed {
        /// The commit's place among its attempt's commits, the first being 1.
        seq: u64,
        /// What made it due.
        trigger: Trigger,
        /// Rows it published, as its receipt counts them.
        rows: u64,
        /// Bytes it published, as its receipt counts them.
        bytes: u64,
        /// The time from when it began, its barrier included, to the end of its report.
        took: Duration,
        /// The time each of its phases took.
        phases: CommitPhases,
        /// What it took of each stream, by stream name: rows, discards and the rest a stream's
        /// report counts, of this commit alone; a stream's lag, resets and forgotten partitions
        /// are the run's report's, and none here.
        streams: BTreeMap<String, StreamReport>,
        /// Whether its report to the source failed, and the owed cursors it gave up.
        acks: Acks,
    },
    /// A commit of an earlier attempt, whose answer was lost, was found landed: its rows and
    /// bytes count from now.
    Credited {
        /// Rows it published.
        rows: u64,
        /// Bytes it published.
        bytes: u64,
    },
    /// The destination applied a change to a table's schema.
    SchemaChanged {
        /// The table, by its name in the destination.
        table: String,
        /// The schema version the change made.
        version: u32,
        /// What changed; a table created lists its columns as added.
        changes: Listed<ColumnChange>,
    },
    /// A partition's source had dropped where its read would resume, so it read again from its
    /// source's earliest.
    RetentionReset {
        /// The stream.
        stream: String,
        /// The partition.
        partition: String,
    },
    /// A commit that landed deleted the done markers of partitions its plans no longer name, to
    /// keep stored state within its limit: a plan that names one again reads it from its
    /// beginning.
    Forgotten {
        /// The stream.
        stream: String,
        /// The partitions, the earliest recorded first.
        partitions: Listed<String>,
    },
    /// What `scope` names failed, and is tried again after `retry_in`.
    Retrying {
        /// What failed.
        scope: Scope,
        /// Why.
        error: ErrorReport,
        /// How long the run waits before it tries again.
        retry_in: Duration,
    },
    /// The run ended.
    RunEnded {
        /// How.
        status: RunStatus,
    },
}

/// What a retry tries again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// The attempt: the next opens the destination again and resumes from what it holds.
    Attempt,
    /// The commit in flight: the destination answered it transiently, and the same commit is
    /// sent again in place, by the same key.
    Commit,
}

/// A list an event bounds: its first [`REPORTED_LISTED`] entries, and how many more it had.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Listed<T> {
    /// The first entries.
    pub entries: Vec<T>,
    /// How many entries followed them.
    pub unlisted: u64,
}

impl<T> Listed<T> {
    /// The first [`REPORTED_LISTED`] of `items`, the rest counted.
    pub(crate) fn first(items: impl IntoIterator<Item = T>) -> Self {
        let mut items = items.into_iter();
        let entries: Vec<T> = items.by_ref().take(REPORTED_LISTED).collect();
        let unlisted = u64::try_from(items.count()).unwrap_or(u64::MAX);
        Self { entries, unlisted }
    }

    /// The list of what `shown` makes of each entry, the rest counted as they were: the entries
    /// left out are never shown.
    pub(crate) fn map<U>(self, shown: impl FnMut(T) -> U) -> Listed<U> {
        Listed {
            entries: self.entries.into_iter().map(shown).collect(),
            unlisted: self.unlisted,
        }
    }
}

/// A change to a table's columns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnChange {
    /// A column was added.
    Added {
        /// Its name in the destination.
        column: String,
        /// How the destination stores it.
        logical_type: String,
        /// Whether it is a variant: a sibling of a source column, holding its values of another
        /// type the destination cannot change the column to.
        variant: bool,
    },
    /// A column was widened to a type that holds more.
    Widened {
        /// Its name in the destination.
        column: String,
        /// How the destination stored it.
        from: String,
        /// How the destination stores it now.
        to: String,
    },
}

/// A commit that landed whose report to the source has not ended: told of once it ends, or by the
/// run where its attempt ends first, so every commit that lands is told of once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Unreported {
    pub(crate) seq: u64,
    pub(crate) trigger: Trigger,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    /// When the commit began, its barrier included.
    pub(crate) began: Instant,
    /// What its phases took but its report.
    pub(crate) phases: CommitPhases,
    pub(crate) streams: BTreeMap<String, StreamReport>,
    /// When its report began.
    pub(crate) reporting: Instant,
}

impl Unreported {
    /// The commit's event, its report having ended at `at` after `ack`, with `acks`.
    pub(crate) fn reported(self, acks: Acks, ack: Duration, at: Instant) -> EventKind {
        let phases = CommitPhases { ack, ..self.phases };
        EventKind::Committed {
            seq: self.seq,
            trigger: self.trigger,
            rows: self.rows,
            bytes: self.bytes,
            took: at.saturating_duration_since(self.began),
            phases,
            streams: self.streams,
            acks,
        }
    }

    /// The commit's event, its attempt having ended at `at` while its report ran.
    pub(crate) fn cut_short(self, at: Instant) -> EventKind {
        let ack = at.saturating_duration_since(self.reporting);
        self.reported(Acks::default(), ack, at)
    }
}

/// Whether a run told its end, after which it tells nothing more.
///
/// Each event of the run is told holding it, so none follows the end, though a task the end left
/// running still runs, and the run's events are told one at a time.
#[derive(Debug, Default)]
pub(crate) struct Telling {
    ended: Mutex<bool>,
}

/// Tells a run's environment what happens in one of its attempts.
#[derive(Clone)]
pub(crate) struct Announcer {
    env: Arc<dyn Env>,
    pipeline: PipelineId,
    attempt: u64,
    /// The run's telling, which every announcer of the run shares.
    telling: Arc<Telling>,
}

impl Announcer {
    /// Tells `env` what happens in attempt `attempt` of a run of `pipeline`, whose telling is
    /// `telling`.
    pub(crate) fn new(
        env: Arc<dyn Env>,
        pipeline: PipelineId,
        attempt: u64,
        telling: Arc<Telling>,
    ) -> Self {
        Self {
            env,
            pipeline,
            attempt,
            telling,
        }
    }

    /// Whether its environment is told anything.
    pub(crate) fn observes(&self) -> bool {
        self.env.observes()
    }

    /// Tells the environment that what `kind` makes happened, now, where it is told anything and
    /// the run has not told its end; the caller holds none of the locks of the engine's state,
    /// since an observer may take its time.
    pub(crate) fn announce(&self, kind: impl FnOnce() -> EventKind) {
        if !self.observes() {
            return;
        }
        let ended = self.telling.ended.lock();
        if !*ended {
            self.tell(kind());
        }
    }

    /// Tells the environment that the run ended as `status`, unless it was told so: nothing is
    /// told after.
    pub(crate) fn end(&self, status: RunStatus) {
        let mut ended = self.telling.ended.lock();
        if !std::mem::replace(&mut *ended, true) && self.observes() {
            self.tell(EventKind::RunEnded { status });
        }
    }

    /// Tells the environment that `kind` happened, now.
    fn tell(&self, kind: EventKind) {
        let event = Event {
            at: self.env.now(),
            pipeline: self.pipeline.clone(),
            attempt: self.attempt,
            kind,
        };
        self.env.observe(&event);
    }

    /// The environment it tells.
    pub(crate) fn env(&self) -> &Arc<dyn Env> {
        &self.env
    }
}

impl fmt::Debug for Announcer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Announcer")
            .field("pipeline", &self.pipeline)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

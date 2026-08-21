//! The canonical fold of the event stream into live numbers.
//!
//! ONE implementation, shared by every consumer: the CLI's live
//! display, an embedder's metrics endpoint, and any bridge to a
//! telemetry system all read the same fold, so a rate computed wrong
//! is fixed once for all of them.
//!
//! The fold is ADVISORY, like the events that feed it: the numbers
//! here are live approximations for humans and dashboards. The
//! exactly-once numbers are the [`crate::report::Run`]'s, and a consumer
//! showing final totals must take them from there — a lagging event
//! subscriber loses the oldest events rather than being allowed to
//! slow the pipeline, so the live fold may have missed events the
//! report did not.
//!
//! Time is a parameter internally (`apply_at`, `snapshot_at`), which is
//! what makes rates testable; the public `apply`/`snapshot` pass
//! `Instant::now()`.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::event::PipelineEvent;
use crate::id::{StreamName, TableName};

/// How far back the sliding rate window reaches.
const RATE_WINDOW: Duration = Duration::from_secs(5);

/// A stream's read-side liveness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamState {
    /// `StreamStarted` seen, `StreamFinished` not yet.
    Reading,
    /// `StreamFinished` seen.
    Finished,
}

/// Read-side totals for one stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct Stream {
    /// Rows decoded from the source payloads.
    pub rows_read: u64,
    /// Source payload bytes (raw for JSON sources, Arrow footprint for
    /// structured ones).
    pub bytes_read: u64,
}

/// Write-side totals for one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct Table {
    /// Rows written to the destination (not necessarily committed yet).
    pub rows_written: u64,
    /// In-memory bytes of the batches written.
    pub bytes_written: u64,
    /// Encoded bytes of closed output parts — absent (zero alongside
    /// `parts_closed == 0`) for destinations that write no files.
    pub output_bytes: u64,
    /// Output parts closed.
    pub parts_closed: u64,
    /// Whole rows dropped by a Discard* policy.
    pub discarded_rows: u64,
    /// Individual values nulled by a Discard* policy.
    pub discarded_values: u64,
}

/// A point-in-time picture of the fold. Plain serializable data — this
/// is what a metrics endpoint returns and what a renderer draws.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct Snapshot {
    /// Per-stream read totals.
    pub streams: BTreeMap<StreamName, Stream>,
    /// Per-stream state, kept beside rather than inside the totals so
    /// the totals stay `Copy`.
    pub stream_states: BTreeMap<StreamName, StreamState>,
    /// Each stream's destination ROOT table, as announced by
    /// `StreamStarted` — the join a renderer needs, published rather
    /// than re-derived (the normalization rules are the
    /// destination's).
    pub stream_tables: BTreeMap<StreamName, TableName>,
    /// Per-table write totals.
    pub tables: BTreeMap<TableName, Table>,
    /// Total rows read across streams.
    pub rows_read: u64,
    /// Total rows written across tables.
    pub rows_written: u64,
    /// Total in-memory bytes written.
    pub bytes_written: u64,
    /// Total encoded output bytes (file-writing destinations only).
    pub output_bytes: u64,
    /// Rows written per second over the sliding window; `None` before
    /// anything happened in the window.
    pub rows_per_sec: Option<f64>,
    /// In-memory bytes written per second over the sliding window.
    pub bytes_per_sec: Option<f64>,
    /// Rows written per second averaged over the whole run so far.
    pub rows_per_sec_avg: Option<f64>,
    /// Commits so far.
    pub commits: u64,
    /// The last committed sequence number.
    pub last_commit_seq: Option<u64>,
    /// How long ago the last commit finished.
    pub since_last_commit: Option<Duration>,
    /// Engine retries of transient failures.
    pub retries: u64,
    /// Schema migrations applied.
    pub schema_migrations: u64,
    /// Whether a commit is currently in flight.
    pub committing: bool,
}

/// The fold itself: feed it every event, ask it for snapshots.
#[derive(Debug)]
pub struct Metrics {
    started: Instant,
    streams: BTreeMap<StreamName, Stream>,
    stream_states: BTreeMap<StreamName, StreamState>,
    stream_tables: BTreeMap<StreamName, TableName>,
    tables: BTreeMap<TableName, Table>,
    commits: u64,
    last_commit_seq: Option<u64>,
    last_commit_at: Option<Instant>,
    commit_in_flight: bool,
    retries: u64,
    schema_migrations: u64,
    /// (when, rows, bytes) per `BatchLoaded`, pruned to the window.
    window: std::collections::VecDeque<(Instant, u64, u64)>,
}

impl Metrics {
    /// A fold whose run started NOW.
    pub fn new() -> Self {
        Self::started_at(Instant::now())
    }

    /// A fold whose run started at `started` — the testable form.
    fn started_at(started: Instant) -> Self {
        Self {
            started,
            streams: BTreeMap::new(),
            stream_states: BTreeMap::new(),
            stream_tables: BTreeMap::new(),
            tables: BTreeMap::new(),
            commits: 0,
            last_commit_seq: None,
            last_commit_at: None,
            commit_in_flight: false,
            retries: 0,
            schema_migrations: 0,
            window: std::collections::VecDeque::new(),
        }
    }

    /// Fold one event in, stamped NOW.
    pub fn apply(&mut self, event: &PipelineEvent) {
        self.apply_at(event, Instant::now());
    }

    /// Fold one event in at an explicit instant — the testable form.
    fn apply_at(&mut self, event: &PipelineEvent, at: Instant) {
        match event {
            PipelineEvent::StreamStarted { stream, table } => {
                self.streams.entry(stream.clone()).or_default();
                self.stream_states
                    .insert(stream.clone(), StreamState::Reading);
                self.stream_tables.insert(stream.clone(), table.clone());
            }
            PipelineEvent::StreamFinished { stream } => {
                self.stream_states
                    .insert(stream.clone(), StreamState::Finished);
            }
            PipelineEvent::BatchRead {
                stream,
                rows,
                bytes,
            } => {
                let entry = self.streams.entry(stream.clone()).or_default();
                entry.rows_read += rows;
                entry.bytes_read += bytes;
            }
            PipelineEvent::BatchLoaded { table, rows, bytes } => {
                let entry = self.tables.entry(table.clone()).or_default();
                entry.rows_written += rows;
                entry.bytes_written += bytes;
                self.window.push_back((at, *rows, *bytes));
                self.prune(at);
            }
            PipelineEvent::PartClosed {
                table,
                encoded_bytes,
                ..
            } => {
                let entry = self.tables.entry(table.clone()).or_default();
                entry.output_bytes += encoded_bytes;
                entry.parts_closed += 1;
            }
            PipelineEvent::Discarded {
                table,
                rows,
                values,
                ..
            } => {
                let entry = self.tables.entry(table.clone()).or_default();
                entry.discarded_rows += rows;
                entry.discarded_values += values;
            }
            PipelineEvent::SchemaEvolved { .. } => self.schema_migrations += 1,
            PipelineEvent::CommitStarted { .. } => self.commit_in_flight = true,
            PipelineEvent::Committed { commit_seq, .. } => {
                self.commits += 1;
                self.last_commit_seq = Some(*commit_seq);
                self.last_commit_at = Some(at);
                self.commit_in_flight = false;
            }
            PipelineEvent::Retried { .. } => self.retries += 1,
            // Deliberately exhaustive IN-CRATE: a new event variant must
            // decide here what the fold does with it, at compile time —
            // foreign consumers see `#[non_exhaustive]` instead.
            PipelineEvent::RunStarted { .. } | PipelineEvent::Heartbeat { .. } => {}
        }
    }

    fn prune(&mut self, now: Instant) {
        while let Some((when, ..)) = self.window.front() {
            if now.duration_since(*when) > RATE_WINDOW {
                self.window.pop_front();
            } else {
                break;
            }
        }
    }

    /// The picture as of NOW.
    pub fn snapshot(&mut self) -> Snapshot {
        self.snapshot_at(Instant::now())
    }

    /// The picture as of an explicit instant — the testable form.
    fn snapshot_at(&mut self, now: Instant) -> Snapshot {
        self.prune(now);
        let rows_written: u64 = self.tables.values().map(|t| t.rows_written).sum();
        let bytes_written: u64 = self.tables.values().map(|t| t.bytes_written).sum();
        let (window_rows, window_bytes) = self
            .window
            .iter()
            .fold((0u64, 0u64), |(r, b), (_, rows, bytes)| {
                (r + rows, b + bytes)
            });
        // The window's SPAN is measured from its oldest entry, capped at
        // the window size — dividing by the full window while it is
        // still filling would understate every early rate.
        let span = self
            .window
            .front()
            .map(|(oldest, ..)| now.duration_since(*oldest).min(RATE_WINDOW));
        let rate = |count: u64| -> Option<f64> {
            let span = span?.as_secs_f64();
            (span > f64::EPSILON).then(|| count as f64 / span)
        };
        let elapsed = now.duration_since(self.started).as_secs_f64();
        Snapshot {
            streams: self.streams.clone(),
            stream_states: self.stream_states.clone(),
            stream_tables: self.stream_tables.clone(),
            tables: self.tables.clone(),
            rows_read: self.streams.values().map(|s| s.rows_read).sum(),
            rows_written,
            bytes_written,
            output_bytes: self.tables.values().map(|t| t.output_bytes).sum(),
            rows_per_sec: rate(window_rows),
            bytes_per_sec: rate(window_bytes),
            rows_per_sec_avg: (elapsed > f64::EPSILON && rows_written > 0)
                .then(|| rows_written as f64 / elapsed),
            commits: self.commits,
            last_commit_seq: self.last_commit_seq,
            since_last_commit: self.last_commit_at.map(|at| now.duration_since(at)),
            retries: self.retries,
            schema_migrations: self.schema_migrations,
            committing: self.commit_in_flight,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::PartCloseReason;

    fn batch_loaded(table: &str, rows: u64, bytes: u64) -> PipelineEvent {
        PipelineEvent::BatchLoaded {
            table: TableName::new(table),
            rows,
            bytes,
        }
    }

    /// Totals are plain sums; read and write sides stay separate.
    #[test]
    fn totals_accumulate_per_side() {
        let start = Instant::now();
        let mut metrics = Metrics::started_at(start);
        metrics.apply_at(
            &PipelineEvent::BatchRead {
                stream: StreamName::new("events"),
                rows: 100,
                bytes: 1_000,
            },
            start,
        );
        metrics.apply_at(&batch_loaded("events", 60, 600), start);
        metrics.apply_at(&batch_loaded("events", 40, 400), start);
        let snap = metrics.snapshot_at(start);
        assert_eq!(snap.rows_read, 100);
        assert_eq!(snap.rows_written, 100);
        assert_eq!(snap.bytes_written, 1_000);
        assert_eq!(snap.streams[&StreamName::new("events")].rows_read, 100);
        assert_eq!(snap.tables[&TableName::new("events")].rows_written, 100);
    }

    /// The sliding rate is computed over the WINDOW SPAN, not the full
    /// window size — a fold two seconds old must not divide by five.
    #[test]
    fn the_rate_window_slides_and_uses_its_real_span() {
        let start = Instant::now();
        let mut metrics = Metrics::started_at(start);
        metrics.apply_at(&batch_loaded("t", 1_000, 10_000), start);
        metrics.apply_at(
            &batch_loaded("t", 1_000, 10_000),
            start + Duration::from_secs(2),
        );
        let snap = metrics.snapshot_at(start + Duration::from_secs(2));
        // 2,000 rows over a 2-second span.
        let rate = snap.rows_per_sec.expect("rows in window");
        assert!((rate - 1_000.0).abs() < 1.0, "{rate}");

        // At six seconds the first batch (age 6s) left the window; the
        // second (age 4s) remains, and the span is measured from IT.
        let snap = metrics.snapshot_at(start + Duration::from_secs(6));
        let rate = snap.rows_per_sec.expect("one batch still in window");
        assert!((rate - 250.0).abs() < 1.0, "1000 rows / 4s span: {rate}");

        // Once everything ages out there is no rate, not a zero rate.
        let snap = metrics.snapshot_at(start + Duration::from_secs(60));
        assert_eq!(snap.rows_per_sec, None);
        // The cumulative average survives the window emptying.
        let avg = snap.rows_per_sec_avg.expect("rows were written");
        assert!((avg - 2_000.0 / 60.0).abs() < 0.5, "{avg}");
    }

    /// Commit latency bookkeeping: CommitStarted marks in-flight,
    /// Committed clears it and stamps recency.
    #[test]
    fn commits_track_inflight_and_recency() {
        let start = Instant::now();
        let mut metrics = Metrics::started_at(start);
        metrics.apply_at(&PipelineEvent::CommitStarted { commit_seq: 1 }, start);
        assert!(metrics.snapshot_at(start).committing);
        metrics.apply_at(
            &PipelineEvent::Committed {
                commit_seq: 1,
                cursors: BTreeMap::new(),
            },
            start + Duration::from_millis(250),
        );
        let snap = metrics.snapshot_at(start + Duration::from_secs(1));
        assert!(!snap.committing);
        assert_eq!(snap.commits, 1);
        assert_eq!(snap.last_commit_seq, Some(1));
        assert_eq!(snap.since_last_commit, Some(Duration::from_millis(750)));
    }

    /// Output bytes come ONLY from PartClosed — in-memory bytes never
    /// leak into the output column.
    #[test]
    fn output_bytes_are_encoded_bytes_only() {
        let start = Instant::now();
        let mut metrics = Metrics::started_at(start);
        metrics.apply_at(&batch_loaded("t", 1_000, 1_000_000), start);
        metrics.apply_at(
            &PipelineEvent::PartClosed {
                table: TableName::new("t"),
                encoded_bytes: 90_000,
                reason: PartCloseReason::Target,
            },
            start,
        );
        let snap = metrics.snapshot_at(start);
        assert_eq!(snap.bytes_written, 1_000_000);
        assert_eq!(snap.output_bytes, 90_000);
        assert_eq!(snap.tables[&TableName::new("t")].parts_closed, 1);
    }
}

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
//!
//! The events are whatever their author sent — a connector's counters,
//! or a file another process wrote — so the fold holds its own bounds:
//! every counter SATURATES rather than wrapping or panicking (a live
//! picture pinned at the ceiling is honest; a wrapped one is not), the
//! per-key maps retain at most [`MAX_TRACKED_KEYS`] entries each (the
//! totals keep counting past it; the overflow is counted in
//! [`Snapshot::untracked_keys`]), a key longer than [`MAX_KEY_BYTES`]
//! is folded into the totals but never retained, and the rate window
//! holds at most [`MAX_WINDOW_ENTRIES`] samples.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::event::PipelineEvent;
use crate::id::{StreamName, TableName};

/// How far back the sliding rate window reaches.
const RATE_WINDOW: Duration = Duration::from_secs(5);

/// Samples the rate window retains: a backlog of `BatchLoaded` events
/// replayed in one instant would otherwise grow it without bound.
/// Well above what five seconds of honest loading produces.
pub const MAX_WINDOW_ENTRIES: usize = 65_536;

/// Distinct streams, and distinct tables, the fold tracks per key.
/// Past it a new key counts toward the totals and toward
/// [`Snapshot::untracked_keys`] but gets no entry of its own — a live
/// picture of thousands of rows is unreadable anyway, and the ceiling
/// is what keeps an event author from growing the fold, and every
/// redraw that clones it, without limit.
pub const MAX_TRACKED_KEYS: usize = 4096;

/// The longest stream or table name the fold retains as a key — the
/// wire's identifier ceiling. A longer one is not an identifier the
/// engine would have produced; it is folded into the totals and
/// counted untracked.
pub const MAX_KEY_BYTES: usize = 1024;

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
    /// Events whose stream or table got no entry of its own — past
    /// [`MAX_TRACKED_KEYS`] distinct keys, or a key over
    /// [`MAX_KEY_BYTES`]. Their rows and bytes are in the totals.
    pub untracked_keys: u64,
}

/// The fold itself: feed it every event, ask it for snapshots.
#[derive(Debug)]
pub struct Metrics {
    started: Instant,
    streams: BTreeMap<StreamName, Stream>,
    stream_states: BTreeMap<StreamName, StreamState>,
    stream_tables: BTreeMap<StreamName, TableName>,
    tables: BTreeMap<TableName, Table>,
    /// The totals, kept apart from the per-key maps so a key the maps
    /// do not retain still counts.
    totals: Totals,
    untracked_keys: u64,
    commits: u64,
    last_commit_seq: Option<u64>,
    last_commit_at: Option<Instant>,
    commit_in_flight: bool,
    retries: u64,
    schema_migrations: u64,
    /// (when, rows, bytes) per `BatchLoaded`, pruned to the window.
    window: std::collections::VecDeque<(Instant, u64, u64)>,
}

/// Run-wide sums, independent of which keys the maps retain.
#[derive(Debug, Default)]
struct Totals {
    rows_read: u64,
    rows_written: u64,
    bytes_written: u64,
    output_bytes: u64,
}

/// The two key kinds the fold retains, by their text.
trait Identifier {
    fn text(&self) -> &str;
}
impl Identifier for StreamName {
    fn text(&self) -> &str {
        self.as_str()
    }
}
impl Identifier for TableName {
    fn text(&self) -> &str {
        self.as_str()
    }
}

/// The per-key entry for `key`, if the fold retains one: an existing
/// entry always, a new one only under [`MAX_TRACKED_KEYS`] and
/// [`MAX_KEY_BYTES`]; `None` counts the event as untracked.
fn tracked<'m, K: Ord + Clone + Identifier, V: Default>(
    map: &'m mut BTreeMap<K, V>,
    key: &K,
    untracked: &mut u64,
) -> Option<&'m mut V> {
    if !map.contains_key(key) && (map.len() >= MAX_TRACKED_KEYS || key.text().len() > MAX_KEY_BYTES)
    {
        *untracked = untracked.saturating_add(1);
        return None;
    }
    Some(map.entry(key.clone()).or_default())
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
            totals: Totals::default(),
            untracked_keys: 0,
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
                if tracked(&mut self.streams, stream, &mut self.untracked_keys).is_none() {
                    return;
                }
                self.stream_states
                    .insert(stream.clone(), StreamState::Reading);
                self.stream_tables.insert(stream.clone(), table.clone());
            }
            PipelineEvent::StreamFinished { stream } => {
                // Only a stream the fold knows changes state: a name
                // never started gets no entry from its finish.
                if self.streams.contains_key(stream) {
                    self.stream_states
                        .insert(stream.clone(), StreamState::Finished);
                }
            }
            PipelineEvent::BatchRead {
                stream,
                rows,
                bytes,
            } => {
                self.totals.rows_read = self.totals.rows_read.saturating_add(*rows);
                if let Some(entry) = tracked(&mut self.streams, stream, &mut self.untracked_keys) {
                    entry.rows_read = entry.rows_read.saturating_add(*rows);
                    entry.bytes_read = entry.bytes_read.saturating_add(*bytes);
                }
            }
            PipelineEvent::BatchLoaded { table, rows, bytes } => {
                self.totals.rows_written = self.totals.rows_written.saturating_add(*rows);
                self.totals.bytes_written = self.totals.bytes_written.saturating_add(*bytes);
                if let Some(entry) = tracked(&mut self.tables, table, &mut self.untracked_keys) {
                    entry.rows_written = entry.rows_written.saturating_add(*rows);
                    entry.bytes_written = entry.bytes_written.saturating_add(*bytes);
                }
                if self.window.len() >= MAX_WINDOW_ENTRIES {
                    self.window.pop_front();
                }
                self.window.push_back((at, *rows, *bytes));
                self.prune(at);
            }
            PipelineEvent::PartClosed {
                table,
                encoded_bytes,
                ..
            } => {
                self.totals.output_bytes = self.totals.output_bytes.saturating_add(*encoded_bytes);
                if let Some(entry) = tracked(&mut self.tables, table, &mut self.untracked_keys) {
                    entry.output_bytes = entry.output_bytes.saturating_add(*encoded_bytes);
                    entry.parts_closed = entry.parts_closed.saturating_add(1);
                }
            }
            PipelineEvent::Discarded {
                table,
                rows,
                values,
                ..
            } => {
                if let Some(entry) = tracked(&mut self.tables, table, &mut self.untracked_keys) {
                    entry.discarded_rows = entry.discarded_rows.saturating_add(*rows);
                    entry.discarded_values = entry.discarded_values.saturating_add(*values);
                }
            }
            PipelineEvent::SchemaEvolved { .. } => {
                self.schema_migrations = self.schema_migrations.saturating_add(1);
            }
            PipelineEvent::CommitStarted { .. } => self.commit_in_flight = true,
            PipelineEvent::Committed { commit_seq, .. } => {
                self.commits = self.commits.saturating_add(1);
                self.last_commit_seq = Some(*commit_seq);
                self.last_commit_at = Some(at);
                self.commit_in_flight = false;
            }
            PipelineEvent::Retried { .. } => self.retries = self.retries.saturating_add(1),
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
        let rows_written = self.totals.rows_written;
        let bytes_written = self.totals.bytes_written;
        let (window_rows, window_bytes) = self
            .window
            .iter()
            .fold((0u64, 0u64), |(r, b), (_, rows, bytes)| {
                (r.saturating_add(*rows), b.saturating_add(*bytes))
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
            rows_read: self.totals.rows_read,
            rows_written,
            bytes_written,
            output_bytes: self.totals.output_bytes,
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
            untracked_keys: self.untracked_keys,
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

    /// Counters saturate: a connector's `encoded_bytes` of `u64::MAX`
    /// followed by one more is the ceiling, not a wrap and not a panic,
    /// and the total agrees with the entry.
    #[test]
    fn counters_saturate_at_the_ceiling() {
        let start = Instant::now();
        let mut metrics = Metrics::started_at(start);
        for encoded_bytes in [u64::MAX, 1] {
            metrics.apply_at(
                &PipelineEvent::PartClosed {
                    table: TableName::new("t"),
                    encoded_bytes,
                    reason: PartCloseReason::Target,
                },
                start,
            );
        }
        metrics.apply_at(&batch_loaded("t", u64::MAX, u64::MAX), start);
        metrics.apply_at(&batch_loaded("t", 1, 1), start);
        let snap = metrics.snapshot_at(start);
        assert_eq!(snap.output_bytes, u64::MAX);
        assert_eq!(snap.tables[&TableName::new("t")].output_bytes, u64::MAX);
        assert_eq!(
            (snap.rows_written, snap.bytes_written),
            (u64::MAX, u64::MAX)
        );
        assert!(snap.rows_per_sec_avg.is_none() || snap.rows_per_sec_avg.unwrap().is_finite());
    }

    /// The per-key maps stop growing at the ceiling and refuse
    /// over-long keys, while the totals keep counting every event and
    /// the overflow is counted where a renderer can say so. The window
    /// holds its own ceiling of samples.
    #[test]
    fn retained_keys_are_bounded_and_the_totals_are_not() {
        let start = Instant::now();
        let mut metrics = Metrics::started_at(start);
        for n in 0..MAX_TRACKED_KEYS + 10 {
            metrics.apply_at(&batch_loaded(&format!("t{n}"), 1, 1), start);
        }
        let long = "x".repeat(MAX_KEY_BYTES + 1);
        metrics.apply_at(&batch_loaded(&long, 1, 1), start);
        // An already-tracked key keeps accumulating past the ceiling.
        metrics.apply_at(&batch_loaded("t0", 1, 1), start);
        for _ in 0..MAX_WINDOW_ENTRIES {
            metrics.apply_at(&batch_loaded("t0", 0, 0), start);
        }
        let snap = metrics.snapshot_at(start);
        assert_eq!(snap.tables.len(), MAX_TRACKED_KEYS);
        assert_eq!(snap.untracked_keys, 11);
        assert_eq!(snap.rows_written, MAX_TRACKED_KEYS as u64 + 10 + 1 + 1);
        assert_eq!(snap.tables[&TableName::new("t0")].rows_written, 2);
        assert_eq!(metrics.window.len(), MAX_WINDOW_ENTRIES);
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

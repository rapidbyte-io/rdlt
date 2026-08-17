//! Typed observability events.
//!
//! Causal-order guarantees: a table's `SchemaEvolved` precedes the first `BatchLoaded`
//! at the new version; `Committed` follows everything it covers; a stream's
//! `BatchRead` precedes the `BatchLoaded` carrying those rows;
//! `CommitStarted` precedes its `Committed`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cursor::Cursor;
use crate::id::{LoadId, StreamName, TableName};
use crate::report::ResumedFrom;
use crate::schema;

/// What a run reports as it happens.
///
/// Subscribed to by a host that wants progress rather than only a final report.
/// Events are advisory: dropping them changes nothing about the load, and a
/// consumer that lags LOSES THE OLDEST EVENTS rather than being allowed to slow
/// the pipeline — a still-connected consumer must not infer a complete feed.
/// The run report is the complete record either way.
///
/// `#[non_exhaustive]`: new events can be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
#[non_exhaustive]
pub enum PipelineEvent {
    /// The run is identified and about to start its streams. The FIRST
    /// event of every attempt that gets far enough to be identified —
    /// the feed's answer to "which load is this, and did it resume".
    /// WAL-replay recovery happens before this event and deliberately
    /// emits nothing (replayed work belongs to the crashed load;
    /// `resumed_from: wal` is its record), and an attempt that fails
    /// during discovery or recovery emits only the `Retried` that
    /// announces its successor.
    RunStarted {
        /// This run's identity — the same id the report will carry.
        load_id: LoadId,
        /// How the run started relative to previous state.
        resumed_from: ResumedFrom,
    },
    /// A stream began reading.
    StreamStarted {
        /// The stream.
        stream: StreamName,
        /// The destination ROOT table its rows land in — the engine's
        /// normalization applied, so a consumer never re-derives it
        /// (the rules depend on the destination). Nested payloads may
        /// shred into further child tables beyond this one.
        table: TableName,
    },
    /// A batch reached the destination. Not yet committed — and therefore not
    /// yet visible to readers of that table.
    BatchLoaded {
        /// The table written.
        table: TableName,
        /// Rows in this batch.
        rows: u64,
        /// In-memory size of this batch.
        bytes: u64,
    },
    /// A table's schema changed. Always precedes the first `BatchLoaded` at the
    /// new version, so a consumer tracking schemas never sees rows in a shape it
    /// has not been told about.
    SchemaEvolved {
        /// What changed.
        delta: schema::Delta,
    },
    /// Work was published atomically with pipeline state. Follows every event it
    /// covers, and its cursors are durable: a crash after this replays from here.
    Committed {
        /// Monotonic sequence number within this run.
        commit_seq: u64,
        /// The committed cursor per stream.
        cursors: BTreeMap<StreamName, Cursor>,
    },
    /// A transient connector failure was retried by the engine. One
    /// ordering asymmetry, stated rather than implied: `Retried`
    /// announces the UPCOMING attempt, so it precedes that attempt's
    /// own `RunStarted`.
    Retried {
        /// The stream involved, where the failure was attributable to one.
        stream: Option<StreamName>,
        /// The RETRY ordinal, 1-based: the first retry announces 1
        /// (which is the run's second attempt).
        attempt: u32,
    },
    /// Rows or values were discarded under a Discard* policy — counted, never silent.
    Discarded {
        /// The table whose data was discarded.
        table: TableName,
        /// Whole rows dropped.
        rows: u64,
        /// Individual values nulled, with their rows kept.
        values: u64,
        /// Why, in human-readable form.
        reason: String,
    },
    /// A stream finished reading, successfully.
    StreamFinished {
        /// The stream.
        stream: StreamName,
    },
    /// A batch arrived FROM the source — before batching, merging, or
    /// discard policies touch it. `BatchLoaded` is the destination side
    /// of the same rows; the two diverging is how coalescing and
    /// discards become visible.
    BatchRead {
        /// The stream that delivered it.
        stream: StreamName,
        /// Rows decoded from the source payload.
        rows: u64,
        /// Payload size: raw bytes for a JSON source, the Arrow
        /// in-memory footprint for a structured one.
        bytes: u64,
    },
    /// A commit began. Paired with the `Committed` that follows it, it
    /// makes commit LATENCY observable — `Committed` alone only gives
    /// intervals.
    CommitStarted {
        /// The sequence number the following `Committed` will carry.
        commit_seq: u64,
    },
    /// A destination closed one output part (a file it will publish).
    /// Emitted by destinations that materialise files; SQL destinations
    /// never emit it. `encoded_bytes` is the part's ON-THE-WIRE size —
    /// the only honest basis for output throughput, since a batch's
    /// in-memory footprint differs from its encoded form by multiples.
    PartClosed {
        /// The table the part belongs to.
        table: TableName,
        /// Encoded size of the closed part.
        encoded_bytes: u64,
        /// What closed it.
        reason: PartCloseReason,
    },
    /// A liveness tick on a coarse timer while the run is active. Lets
    /// a consumer distinguish "the wire is quiet" from "the process is
    /// stalled": events may legitimately stop for a while, heartbeats
    /// may not.
    Heartbeat {
        /// Milliseconds since the run started.
        elapsed_ms: u64,
    },
}

/// Why a destination closed an output part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PartCloseReason {
    /// The part reached its size target.
    Target,
    /// The part was open longer than the configured time bound.
    Time,
    /// The memory ceiling across open parts closed it early.
    Budget,
    /// A commit closed it — no part spans a commit.
    Commit,
    /// A schema change closed it — a file holds one schema.
    Schema,
}

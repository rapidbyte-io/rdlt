//! Typed observability events.
//!
//! Causal-order guarantees: a table's `SchemaEvolved` precedes the first `BatchLoaded`
//! at the new version; `Committed` follows everything it covers.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cursor::Cursor;
use crate::ids::{StreamName, TableName};
use crate::schema::SchemaDelta;

/// What a run reports as it happens.
///
/// Subscribed to by a host that wants progress rather than only a final report.
/// Events are advisory: dropping them changes nothing about the load, and a
/// consumer that lags is disconnected rather than allowed to slow the pipeline.
///
/// `#[non_exhaustive]`: new events can be added without a breaking change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
#[non_exhaustive]
pub enum PipelineEvent {
    /// A stream began reading.
    StreamStarted {
        /// The stream.
        stream: StreamName,
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
        delta: SchemaDelta,
    },
    /// Work was published atomically with pipeline state. Follows every event it
    /// covers, and its cursors are durable: a crash after this replays from here.
    Committed {
        /// Monotonic sequence number within this run.
        commit_seq: u64,
        /// The committed cursor per stream.
        cursors: BTreeMap<StreamName, Cursor>,
    },
    /// A transient connector failure was retried by the engine.
    Retried {
        /// The stream involved, where the failure was attributable to one.
        stream: Option<StreamName>,
        /// Which attempt this is, 1-based.
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
}

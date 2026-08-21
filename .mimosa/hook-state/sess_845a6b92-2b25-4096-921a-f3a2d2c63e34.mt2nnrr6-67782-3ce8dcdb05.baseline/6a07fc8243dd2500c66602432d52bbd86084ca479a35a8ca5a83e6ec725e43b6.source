//! The machine-readable run outcome.
//!
//! Accounting invariant: totals here equal destination-visible reality; every retry,
//! widening, and discard appears. Serde-stable — platforms persist reports across
//! engine upgrades.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::commit;
use crate::cursor::Cursor;
use crate::id::{LoadId, PipelineId, StreamName, TableName};
use crate::schema;

/// Version of the report's serialized shape. Present so a platform persisting
/// reports across engine upgrades can tell which layout it is reading.
pub const REPORT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
/// One table's totals for the run.
pub struct Table {
    /// Rows committed to this table.
    pub rows: u64,
    /// In-memory bytes of the batches written to it.
    pub bytes: u64,
    /// Whole rows dropped by a Discard* policy. Non-zero means the destination
    /// holds LESS than the source offered, deliberately.
    pub discarded_rows: u64,
    /// Individual values nulled by a Discard* policy, with their rows kept.
    pub discarded_values: u64,
    /// Encoded bytes of the output parts this table's rows landed in —
    /// zero for destinations that write no files. `#[serde(default)]`
    /// so reports written before the field existed still deserialize.
    #[serde(default)]
    pub output_bytes: u64,
}

/// A commit unit's totals projected into report shape. The two types are
/// field-identical but hold different aggregation levels (see
/// [`commit::Counters`]); this binds them so the shared field set has one
/// authority.
impl From<commit::Counters> for Table {
    fn from(counters: commit::Counters) -> Self {
        Self {
            rows: counters.rows,
            bytes: counters.bytes,
            discarded_rows: counters.discarded_rows,
            discarded_values: counters.discarded_values,
            output_bytes: 0,
        }
    }
}

/// How this run started relative to previous state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResumedFrom {
    /// No prior state existed.
    Fresh,
    /// Prior cursors recovered from destination state; source resumed.
    Cursor,
    /// Local WAL replayed (no re-extraction) before continuing.
    Wal {
        /// How many batches replay re-applied before the run continued.
        replayed_batches: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
/// What a run did, in machine-readable form.
///
/// The accounting invariant: these totals equal destination-visible reality.
/// Every retry, widening and discard appears here — if the run did less than was
/// asked, this says so rather than reporting a clean success.
pub struct Run {
    /// [`REPORT_FORMAT_VERSION`] at the time of writing.
    pub format_version: u32,
    /// The pipeline this run belongs to.
    pub pipeline: PipelineId,
    /// This run's identifier.
    pub load_id: LoadId,
    /// Per-table totals.
    pub tables: BTreeMap<TableName, Table>,
    /// Every schema migration applied during the run, in order.
    pub schema_migrations: Vec<schema::Delta>,
    /// Engine-driven retries of transient connector failures.
    pub retries: u64,
    /// Final committed cursor per stream.
    ///
    /// Cursor VALUES are source-defined and may embed sensitive
    /// material (an offset is harmless; a resume token or signed
    /// continuation URL is not) — so a serialized report, like the
    /// CLI's `--report` file and stdout, is to be handled as
    /// sensitively as the state store itself.
    pub cursors: BTreeMap<StreamName, Cursor>,
    /// How this run started relative to previous state.
    pub resumed_from: ResumedFrom,
    /// How many commits the run published. At least one, even for a no-op run,
    /// so a fresh pipeline's state document exists afterwards.
    pub commits: u64,
    /// Wall-clock duration of the run.
    pub elapsed_ms: u64,
    /// Read-side totals per stream. Distinct from `tables`: a stream's
    /// rows are counted as the SOURCE delivered them, before discard
    /// policies and merges. `#[serde(default)]` so reports written
    /// before the field existed still deserialize.
    #[serde(default)]
    pub streams: BTreeMap<StreamName, Stream>,
    /// Rows per second averaged over the run — `rows / elapsed`,
    /// precomputed so consumers do not each re-derive it (and get the
    /// division-by-near-zero edge wrong independently).
    #[serde(default)]
    pub rows_per_sec_avg: Option<f64>,
}

/// One stream's read-side totals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Stream {
    /// Rows decoded from the source's payloads.
    pub rows_read: u64,
    /// Payload bytes: raw for a JSON source, the Arrow in-memory
    /// footprint for a structured one.
    pub bytes_read: u64,
}

impl Run {
    /// `#[non_exhaustive]` blocks struct literals outside this crate; the engine
    /// constructs through here and mutates the pub fields.
    pub fn new(pipeline: PipelineId, load_id: LoadId) -> Self {
        Self {
            format_version: REPORT_FORMAT_VERSION,
            pipeline,
            load_id,
            tables: BTreeMap::new(),
            schema_migrations: Vec::new(),
            retries: 0,
            cursors: BTreeMap::new(),
            resumed_from: ResumedFrom::Fresh,
            commits: 0,
            elapsed_ms: 0,
            streams: BTreeMap::new(),
            rows_per_sec_avg: None,
        }
    }

    /// The mutable entry for a table, created zeroed on first mention.
    pub fn table_mut(&mut self, table: &TableName) -> &mut Table {
        self.tables.entry(table.clone()).or_default()
    }

    /// Rows committed across every table. Excludes discards — this is what the
    /// destination holds, not what the source offered.
    pub fn total_rows(&self) -> u64 {
        self.tables.values().map(|t| t.rows).sum()
    }
}

#[cfg(test)]
mod projection_tests {
    //! `From<commit::Counters> for Table` is a field-for-field projection; a
    //! projection returning `Default::default()` (all zeros) would let a host
    //! report a unit that moved nothing.
    use super::*;

    #[test]
    fn commit_counters_project_field_for_field() {
        let counters = commit::Counters {
            rows: 7,
            bytes: 1_234,
            discarded_rows: 2,
            discarded_values: 5,
        };
        let report: Table = counters.into();
        assert_eq!(report.rows, 7);
        assert_eq!(report.bytes, 1_234);
        assert_eq!(report.discarded_rows, 2);
        assert_eq!(report.discarded_values, 5);

        // Distinct values per field on purpose: equal ones would let a
        // transposed projection pass.
        assert_ne!(report.rows, report.bytes);
        assert_ne!(report.discarded_rows, report.discarded_values);
    }
}

//! The machine-readable run outcome (data-model.md §7).
//!
//! Accounting invariant (spec FR-012/SC-008): totals here equal destination-visible
//! reality; every retry, widening, and discard appears. Serde-stable — platforms
//! persist reports across engine upgrades.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cursor::Cursor;
use crate::ids::{LoadId, PipelineId, StreamName, TableName};
use crate::schema::SchemaDelta;

pub const REPORT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TableReport {
    pub rows: u64,
    pub bytes: u64,
    pub discarded_rows: u64,
    pub discarded_values: u64,
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
    Wal { replayed_batches: u64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RunReport {
    pub format_version: u32,
    pub pipeline: PipelineId,
    pub load_id: LoadId,
    pub tables: BTreeMap<TableName, TableReport>,
    /// Every schema migration applied during the run, in order.
    pub schema_migrations: Vec<SchemaDelta>,
    /// Engine-driven retries of transient connector failures.
    pub retries: u64,
    /// Final committed cursor per stream.
    pub cursors: BTreeMap<StreamName, Cursor>,
    pub resumed_from: ResumedFrom,
    pub commits: u64,
    pub elapsed_ms: u64,
}

impl RunReport {
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
        }
    }

    pub fn table_mut(&mut self, table: &TableName) -> &mut TableReport {
        self.tables.entry(table.clone()).or_default()
    }

    pub fn total_rows(&self) -> u64 {
        self.tables.values().map(|t| t.rows).sum()
    }
}

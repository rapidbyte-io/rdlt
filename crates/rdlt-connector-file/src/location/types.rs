//! The value types shared across the source, format, and destination sides:
//! what one file looks like on disk this run (`FileMeta`), what to do with it
//! (`FileTask`), and the persisted per-file progress (`FileProgress`). They live
//! HERE, under `location/`, because both the source (which plans reads) and the
//! shared read path consume them — a type owned by `source/` would force the
//! lower `location/` layer to import upward from `source/`.
//!
//! Unit polymorphism: `size_units`/`done_units` count bytes for plain jsonl and
//! row groups for parquet — the incremental unit the stream's format defines.
//! The `_units` suffix is the reminder that these are not always byte counts.

use serde::{Deserialize, Serialize};

/// Persisted per-file progress. Serialized as one entry of the cursor document,
/// so its wire keys are frozen (WR1): the Rust field renames below preserve the
/// on-disk spelling exactly (`done`, `size`, `eol`).
///
/// Complete ⇔ `done_units == size_units`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileProgress {
    /// Bytes consumed (jsonl) / row groups consumed (parquet).
    #[serde(rename = "done")]
    pub done_units: u64,
    /// Total size (bytes / row groups) when last read.
    #[serde(rename = "size")]
    pub size_units: u64,
    /// Whether the consumed range ended at a record boundary (a newline for jsonl;
    /// always true for parquet's row-group unit). A range that swallowed an
    /// unterminated final line must not be resumed past if the file later grows —
    /// `done_units` would point mid-record.
    #[serde(rename = "eol", default = "default_true")]
    pub ended_at_record_boundary: bool,
    /// File mtime (ms since epoch) observed when this progress was recorded. A
    /// same-size file whose mtime moved was rewritten in place — size alone cannot
    /// see that, so this is the loud-failure tripwire for it.
    #[serde(default)]
    pub mtime_ms: Option<u64>,
    /// Object-store content identity: the etag observed when this progress was
    /// recorded — the object-side rewrite tripwire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// blake3 of the last `min(done_units, TAIL_WINDOW)` consumed bytes (jsonl only —
    /// the resume-offset integrity check).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_hash: Option<String>,
    /// blake3 over the FOOTER DESCRIPTION of the consumed row-group prefix
    /// (parquet only — the row-group analogue of `tail_hash`).
    ///
    /// Additive at `CURSOR_FORMAT_VERSION` 1, exactly as `etag` and `tail_hash`
    /// were: `skip_serializing_if` keeps the emitted document byte-identical for
    /// the formats that do not set it, and neither this struct nor `FileCursor`
    /// denies unknown fields, so documents written either side of this change
    /// stay readable both ways. A parquet entry carries no integrity value until
    /// the next checkpoint rewrites it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_groups_hash: Option<String>,
}

fn default_true() -> bool {
    true
}

/// One matched file as observed on disk this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub path: String,
    /// Bytes (jsonl) / row groups (parquet).
    pub size_units: u64,
    pub mtime_ms: Option<u64>,
    /// Object-store content identity (None for local files).
    pub etag: Option<String>,
}

/// What to do with one matched file this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTask {
    pub path: String,
    /// Where to start (bytes / row groups); equals prior `done_units` (0 for new files).
    pub start: u64,
    /// The mtime observed at planning time, recorded with this file's progress.
    pub mtime_ms: Option<u64>,
    /// The object etag observed at planning time (object-store files).
    pub etag: Option<String>,
    /// Snapshot size (bytes / row groups) from the listing.
    pub size_units: u64,
    /// Read from THIS local path instead of `path` (object-store parquet
    /// is fetched to a temp file first; the cursor stays keyed by `path`).
    pub read_path: Option<String>,
    /// Resume-offset integrity: what the recorded progress expects to find
    /// before `start`. Present only for a resumed file whose progress recorded
    /// one; a fresh read and a first-upgrade resume both carry `None`.
    pub resume_check: Option<ResumeCheck>,
}

/// What a resumed read must verify before it trusts its start offset. One
/// variant per cursor unit, because the two units describe a consumed prefix in
/// different terms: bytes for a record stream, row groups for parquet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeCheck {
    /// Re-read the last `window` bytes before the offset and match `hash`.
    TailBytes { window: u64, hash: String },
    /// Re-describe row groups `0..groups` from the footer and match `hash`.
    RowGroupPrefix { groups: u64, hash: String },
}

impl FileTask {
    /// A task carrying the listing identity of `meta` (path, snapshot size,
    /// mtime, etag), starting at `start` with an optional tail check. The one
    /// place a `FileTask` is stamped from a `FileMeta` — every planner arm goes
    /// through here so no field is ever dropped on the floor.
    pub fn from_meta(meta: &FileMeta, start: u64, resume_check: Option<ResumeCheck>) -> Self {
        Self {
            path: meta.path.clone(),
            start,
            mtime_ms: meta.mtime_ms,
            etag: meta.etag.clone(),
            size_units: meta.size_units,
            read_path: None,
            resume_check,
        }
    }

    /// A fresh read from the start of the file (new files, and whole-file
    /// formats that re-read from zero on any incompleteness).
    pub fn fresh(meta: &FileMeta) -> Self {
        Self::from_meta(meta, 0, None)
    }
}

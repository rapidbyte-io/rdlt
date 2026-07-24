//! Write-ahead log: parquet segments + append-only JSONL manifest.
//!
//! The WAL is a *replayable buffer*, never the source of truth. Commit ordering:
//! (1) fsync pending segments + manifest, (2) destination commit, (3) append
//! `Committed` + GC segment files. A crash between (1) and (3) leaves an
//! uncommitted-looking span whose replay re-commits with the SAME
//! `(load_id, commit_seq)` — idempotence absorbs the ambiguity.

pub(crate) mod resume;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::arrow::ArrowWriter;
use rdlt_connector::RecordBatch;
use rdlt_core::{
    Cursor, LoadId, PipelineId, RdltError, SchemaDelta, StreamName, TableName, TableSchema,
    WriteMode,
};
use serde::{Deserialize, Serialize};

use crate::load::LoadItem;
use rdlt_core::crash_point;

/// Manifest format version. Bumped on any
/// record-shape change; a newer-than-supported manifest degrades to re-extraction
/// rather than being misread.
pub(crate) const WAL_FORMAT_VERSION: u32 = 1;

fn default_wal_version() -> u32 {
    1
}

/// One manifest line. Order on disk IS the replay order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "rec", rename_all = "snake_case")]
pub(crate) enum WalRecord {
    /// First record of each run; identifies the load for recovery commits and
    /// carries the manifest format version.
    Run {
        #[serde(default = "default_wal_version")]
        format_version: u32,
        load_id: LoadId,
        pipeline: PipelineId,
    },
    Delta {
        schema: TableSchema,
        delta: SchemaDelta,
        mode: WriteMode,
    },
    Segment {
        table: TableName,
        file: String,
        rows: u64,
    },
    Checkpoint {
        stream: StreamName,
        cursor: Cursor,
    },
    Committed {
        commit_seq: u64,
    },
}

#[derive(Debug)]
pub(crate) struct Wal {
    dir: PathBuf,
    manifest: File,
    load_id: LoadId,
    segment_seq: u64,
    /// Segment files written since the last fsync barrier.
    pending_sync: Vec<PathBuf>,
    /// Segment files of the current uncommitted span (GC'd after receipt).
    pending_gc: Vec<PathBuf>,
}

fn wal_err(context: &str, e: impl std::fmt::Display) -> RdltError {
    RdltError::wal(format!("{context}: {e}"))
}

impl Wal {
    /// Open (creating if needed) the WAL for a new run and append its `Run` header.
    pub(crate) fn open(
        dir: PathBuf,
        pipeline: &PipelineId,
        load_id: &LoadId,
    ) -> Result<Self, RdltError> {
        std::fs::create_dir_all(&dir).map_err(|e| wal_err("creating wal dir", e))?;
        let manifest = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("manifest.jsonl"))
            .map_err(|e| wal_err("opening manifest", e))?;
        let mut wal = Self {
            dir,
            manifest,
            load_id: load_id.clone(),
            segment_seq: 0,
            pending_sync: Vec::new(),
            pending_gc: Vec::new(),
        };
        wal.append(&WalRecord::Run {
            format_version: WAL_FORMAT_VERSION,
            load_id: load_id.clone(),
            pipeline: pipeline.clone(),
        })?;
        Ok(wal)
    }

    /// Record one load item ahead of applying it to the destination.
    pub(crate) fn record(&mut self, item: &LoadItem) -> Result<(), RdltError> {
        match item {
            LoadItem::Delta {
                schema,
                delta,
                mode,
            } => self.append(&WalRecord::Delta {
                schema: schema.clone(),
                delta: delta.clone(),
                mode: mode.clone(),
            }),
            LoadItem::Checkpoint { stream, cursor } => self.append(&WalRecord::Checkpoint {
                stream: stream.clone(),
                cursor: cursor.clone(),
            }),
            LoadItem::Batch { table, batch } => {
                crash_point!(
                    "wal.segment.write",
                    Err(wal_err(
                        "write segment",
                        std::io::Error::other("injected crash"),
                    ))
                );
                let file = format!("{}-{:06}.parquet", self.load_id, self.segment_seq);
                self.segment_seq += 1;
                let path = self.dir.join(&file);
                write_segment(&path, batch)?;
                self.pending_sync.push(path.clone());
                self.pending_gc.push(path);
                self.append(&WalRecord::Segment {
                    table: table.clone(),
                    file,
                    rows: batch.num_rows() as u64,
                })
            }
            // Report-only accounting; a replay regenerates nothing from it.
            LoadItem::Discarded { .. } => Ok(()),
        }
    }

    /// Step (1) of the commit protocol: make the whole span durable.
    pub(crate) fn sync_for_commit(&mut self) -> Result<(), RdltError> {
        crash_point!(
            "wal.segment.fsync",
            Err(wal_err(
                "fsync segment",
                std::io::Error::other("injected crash"),
            ))
        );
        for path in self.pending_sync.drain(..) {
            File::open(&path)
                .and_then(|f| f.sync_all())
                .map_err(|e| wal_err("fsync segment", e))?;
        }
        self.manifest
            .flush()
            .map_err(|e| wal_err("flush manifest", e))?;
        crash_point!(
            "wal.manifest.fsync",
            Err(wal_err(
                "fsync manifest",
                std::io::Error::other("injected crash"),
            ))
        );
        self.manifest
            .sync_all()
            .map_err(|e| wal_err("fsync manifest", e))?;
        Ok(())
    }

    /// Step (3): the destination acknowledged `commit_seq` — mark and reclaim.
    pub(crate) fn mark_committed(&mut self, commit_seq: u64) -> Result<(), RdltError> {
        self.append(&WalRecord::Committed { commit_seq })?;
        for path in self.pending_gc.drain(..) {
            // Best-effort: a survivor just gets replay-skipped via the Committed record.
            let _ = std::fs::remove_file(path);
        }
        Ok(())
    }

    fn append(&mut self, record: &WalRecord) -> Result<(), RdltError> {
        crash_point!(
            "wal.manifest.append",
            Err(wal_err(
                "append manifest",
                std::io::Error::other("injected crash"),
            ))
        );
        let mut line = serde_json::to_vec(record).map_err(|e| wal_err("encode record", e))?;
        line.push(b'\n');
        self.manifest
            .write_all(&line)
            .map_err(|e| wal_err("append manifest", e))
    }
}

fn write_segment(path: &Path, batch: &RecordBatch) -> Result<(), RdltError> {
    let file = File::create(path).map_err(|e| wal_err("create segment", e))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&batch.schema()), None)
        .map_err(|e| wal_err("open segment writer", e))?;
    writer
        .write(batch)
        .map_err(|e| wal_err("write segment", e))?;
    writer.close().map_err(|e| wal_err("close segment", e))?;
    Ok(())
}

/// Remove the whole WAL directory (clean finish, or fresh start after full recovery).
pub(crate) fn clear(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

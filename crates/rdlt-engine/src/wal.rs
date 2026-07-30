//! Write-ahead log: Arrow IPC file segments + append-only JSONL manifest.
//!
//! The WAL is a *replayable buffer*, never the source of truth. Commit ordering:
//! (1) fsync pending segments + manifest, (2) destination commit, (3) append
//! `Committed` + GC segment files. A crash between (1) and (3) leaves an
//! uncommitted-looking span whose replay re-commits with the SAME
//! `(load_id, commit_seq)` — idempotence absorbs the ambiguity.
//!
//! Segments carry no dictionary, no statistics and no compression. A columnar
//! analytics container spends its encoding effort making data queryable, and
//! nothing ever queries a segment: it is written, replayed at most once, and
//! unlinked. The container is chosen for cheap round-tripping and for refusing
//! a truncated file, not for what a reader could do with it.

pub(crate) mod resume;

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rdlt_connector::RecordBatch;
use rdlt_core::{
    Cursor, LoadId, PipelineId, RdltError, SchemaDelta, StreamName, TableName, TableSchema,
    WriteMode, crash_point,
};
use serde::{Deserialize, Serialize};

use crate::load::LoadItem;

/// WAL format version, covering the manifest record shapes AND the segment
/// container together — they are one format, because a manifest line is only
/// meaningful if the segment it names can be decoded.
///
/// v1: parquet segments. v2: Arrow IPC file segments (`.arrow`).
///
/// A manifest at any other version is REFUSED, in both directions. Refusing a
/// newer one is obvious; refusing an older one matters just as much here,
/// because a v1 manifest names parquet segments this build cannot read — and
/// discovering that at segment-open time would report "unreadable segment"
/// where the truth is "different format". Recovery degrades to source
/// re-extraction either way: slower, never wrong.
pub(crate) const WAL_FORMAT_VERSION: u32 = 2;

/// The serde fallback for a manifest whose `Run` header predates the versioned
/// header field. Pinned to `1` FOREVER — such a manifest is by definition a v1
/// one, so defaulting it to the current version would claim parquet segments
/// are Arrow IPC and hand corrupt input to the reader. Deliberately not
/// [`WAL_FORMAT_VERSION`]: that constant moves, this one describes history.
fn initial_wal_version() -> u32 {
    1
}

/// One manifest line. Order on disk IS the replay order.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "rec", rename_all = "snake_case")]
pub(crate) enum WalRecord {
    /// First record of each run; identifies the load for recovery commits and
    /// carries the manifest format version.
    Run {
        #[serde(default = "initial_wal_version")]
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
    ///
    /// The segment is written, then its manifest line appended. That order is
    /// the durability rule: replay follows manifest lines, so a segment must
    /// exist before anything names it. A crash between the two leaves an
    /// unreferenced file, which replay ignores and `clear` removes.
    ///
    /// The write happens ON THIS TASK, deliberately. Moving it to the blocking
    /// pool measured 6.7 ms per batch SLOWER on 8 MiB batches: the encode reads
    /// a batch that was just produced on this thread, and handing it to another
    /// core costs more in cache misses than the freed runtime thread is worth
    /// while the pipeline is serial and nothing else can use it.
    ///
    /// Deliberately NOT pipelined against the destination write either: the
    /// manifest's order on disk IS the replay order.
    pub(crate) async fn record(&mut self, item: &LoadItem) -> Result<(), RdltError> {
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
                let file = format!("{}-{:06}.arrow", self.load_id, self.segment_seq);
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
    ///
    /// The fsyncs DO go to the blocking pool: unlike the segment encode they are
    /// pure kernel wait with no working set to keep warm, so nothing is lost by
    /// moving them and a runtime thread is freed for the duration.
    ///
    /// TWO blocking hops, not one, and the split is forced rather than chosen:
    /// `crash_point!` expands to a fail point whose closure form RETURNS from
    /// the enclosing function, so moving one inside a `spawn_blocking` closure
    /// would change what it returns from — and under the panic action would
    /// move the panic onto a pool thread. `wal.manifest.fsync` sits between the
    /// segment fsyncs and the manifest fsync, so it stays on this side and the
    /// two fsync groups go over separately.
    /// Mutation note: replacing this body with `Ok(())` is UNKILLABLE by any
    /// test this suite can run, and that is a property of fsync rather than a
    /// gap in the pins.
    ///
    /// What this method buys is durability across POWER LOSS: without the
    /// fsyncs the data is still in the page cache, so every read — including a
    /// full crash-recovery replay after `kill -9` — returns exactly the same
    /// bytes. The difference appears only when the kernel dies with the cache
    /// unwritten, which no in-process test can produce. The crash sweep covers
    /// process death, which is a strictly weaker fault.
    ///
    /// Recorded rather than papered over: a test asserting "commit succeeded"
    /// here would pass with the fsyncs removed and would falsely claim the
    /// durability barrier is covered. Verifying it needs a different KIND of
    /// instrument (a fault-injecting filesystem, or hardware), not another
    /// assertion.
    pub(crate) async fn sync_for_commit(&mut self) -> Result<(), RdltError> {
        crash_point!(
            "wal.segment.fsync",
            Err(wal_err(
                "fsync segment",
                std::io::Error::other("injected crash"),
            ))
        );
        let pending = std::mem::take(&mut self.pending_sync);
        tokio::task::spawn_blocking(move || {
            for path in pending {
                File::open(&path)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| wal_err("fsync segment", e))?;
            }
            Ok::<(), RdltError>(())
        })
        .await
        .map_err(|e| wal_err("segment fsync task", e))??;
        // A no-op on `File` (nothing is buffered in userspace), kept because it
        // states where the userspace boundary is.
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
        let handle = self
            .manifest
            .try_clone()
            .map_err(|e| wal_err("fsync manifest", e))?;
        tokio::task::spawn_blocking(move || {
            handle.sync_all().map_err(|e| wal_err("fsync manifest", e))
        })
        .await
        .map_err(|e| wal_err("manifest fsync task", e))??;
        Ok(())
    }

    /// Step (3): the destination acknowledged `commit_seq` — mark and reclaim.
    pub(crate) async fn mark_committed(&mut self, commit_seq: u64) -> Result<(), RdltError> {
        self.append(&WalRecord::Committed { commit_seq })?;
        let reclaim = std::mem::take(&mut self.pending_gc);
        // Best-effort: a survivor just gets replay-skipped via the Committed
        // record, so unlinking never blocks the commit's completion.
        let _ = tokio::task::spawn_blocking(move || {
            for path in reclaim {
                let _ = std::fs::remove_file(path);
            }
        })
        .await;
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

/// Write one segment in the Arrow IPC **file** format.
///
/// The file format is chosen over the streaming one for a durability property,
/// not for speed: its footer is validated on open, so a segment truncated at a
/// block boundary by a power loss is REFUSED. A truncated IPC *stream*, by
/// contrast, decodes the messages it has and reports clean end-of-input — which
/// would replay a short span and silently drop the rest.
///
/// No dictionary construction, no statistics, no compression: a segment lives
/// only until its commit is acknowledged, so encoding effort spent making it
/// queryable is pure loss.
///
/// Buffered, because the writer does NOT emit only a few large writes: each
/// segment costs roughly sixteen `write_all` calls — continuation markers,
/// flatbuffer metadata, padding and the footer alongside the body buffers —
/// so several hundred per load, most of them tiny. Measured at 1.1% of wall on
/// the 1M-row relational cell: small, but free.
pub(crate) fn write_segment(path: &Path, batch: &RecordBatch) -> Result<(), RdltError> {
    let file = File::create(path).map_err(|e| wal_err("create segment", e))?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new_buffered(file, batch.schema_ref())
        .map_err(|e| wal_err("open segment writer", e))?;
    writer
        .write(batch)
        .map_err(|e| wal_err("write segment", e))?;
    writer.finish().map_err(|e| wal_err("close segment", e))?;
    Ok(())
}

/// Remove the whole WAL directory (clean finish, or fresh start after full recovery).
pub(crate) fn clear(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}
// The claim "segment sequence numbers must be strictly monotonic" used to live
// in a doc comment on a resume.rs test that only ever asserted the header
// version. That clause is deleted there and the property is actually tested
// here, against a real Wal writing real segments.

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_core::TableName;
    use std::sync::Arc;

    fn batch_of(rows: i64) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
        )
        .expect("batch")
    }

    fn manifest_records(dir: &Path) -> Vec<WalRecord> {
        let text = std::fs::read_to_string(dir.join("manifest.jsonl")).expect("read manifest");
        text.lines()
            .map(|line| serde_json::from_str(line).expect("manifest line"))
            .collect()
    }

    fn segment_rows(path: &Path) -> usize {
        let file = std::fs::File::open(path).expect("open segment");
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).expect("arrow ipc");
        reader
            .map(|b| b.expect("decode batch").num_rows())
            .sum::<usize>()
    }

    /// Each recorded batch gets its OWN segment file, and the sequence advances
    /// by one so no name is ever reused. Under `segment_seq += 1` → `*=` the
    /// counter stays at zero, both batches write `l-000000.arrow`, and the
    /// second silently OVERWRITES the first — replay would then load the same
    /// rows twice and lose the others. Nothing about the counter is asserted
    /// directly: the pin is the two files and their distinct contents.
    #[tokio::test]
    async fn each_recorded_batch_gets_its_own_sequential_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut wal = Wal::open(
            dir.path().to_path_buf(),
            &PipelineId::new("p"),
            &LoadId::new("l"),
        )
        .expect("open wal");

        wal.record(&LoadItem::Batch {
            table: TableName::new("t"),
            batch: batch_of(3),
        })
        .await
        .expect("record first");
        wal.record(&LoadItem::Batch {
            table: TableName::new("t"),
            batch: batch_of(5),
        })
        .await
        .expect("record second");

        // Two distinct files, named in sequence.
        let first = dir.path().join("l-000000.arrow");
        let second = dir.path().join("l-000001.arrow");
        assert!(first.exists(), "first segment must exist: {first:?}");
        assert!(
            second.exists(),
            "the second segment must be a NEW file, not a reuse of the first"
        );

        // Each carries its own rows — proof the first was not overwritten.
        assert_eq!(segment_rows(&first), 3);
        assert_eq!(segment_rows(&second), 5);

        // And the manifest names them in write order, with matching row counts.
        let records = manifest_records(dir.path());
        let segments: Vec<(String, u64)> = records
            .iter()
            .filter_map(|r| match r {
                WalRecord::Segment { file, rows, .. } => Some((file.clone(), *rows)),
                _ => None,
            })
            .collect();
        assert_eq!(
            segments,
            vec![
                ("l-000000.arrow".to_owned(), 3),
                ("l-000001.arrow".to_owned(), 5)
            ],
            "manifest order IS replay order"
        );
        assert!(
            matches!(records.first(), Some(WalRecord::Run { .. })),
            "the Run header is always the first line"
        );
    }

    /// `initial_wal_version` describes HISTORY: a manifest with no version field
    /// is by definition a v1 one. Defaulting it to the current version would
    /// claim its parquet segments are Arrow IPC and hand corrupt bytes to the
    /// reader, so this must stay 1 even as `WAL_FORMAT_VERSION` moves.
    #[test]
    fn a_headerless_manifest_version_defaults_to_one_forever() {
        assert_eq!(initial_wal_version(), 1);
        let decoded: WalRecord =
            serde_json::from_str(r#"{"rec":"run","load_id":"l","pipeline":"p"}"#)
                .expect("a pre-versioning header still decodes");
        match decoded {
            WalRecord::Run { format_version, .. } => assert_eq!(
                format_version, 1,
                "absent version means v1, never the current version"
            ),
            other => panic!("expected a Run header, got {other:?}"),
        }
    }
}

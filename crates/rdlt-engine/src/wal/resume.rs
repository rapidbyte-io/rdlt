//! WAL resume: single forward scan of the manifest, replay of the uncommitted span,
//! degradation to re-extraction on any damage (slower, never wrong).

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use rdlt_connector::LoadSession;
use rdlt_core::{CommitMeta, LoadId, RdltError, StateDoc};

use super::WalRecord;

/// Open one WAL segment for streaming decode; the error text names the
/// failure so degradation to re-extraction is diagnosable from logs.
///
/// The Arrow IPC file reader validates the footer before yielding anything, so
/// a segment truncated by a power loss fails HERE rather than decoding a
/// prefix and reporting a clean end — which is the property the file format
/// was chosen for.
fn open_segment(
    dir: &Path,
    file: &str,
) -> Result<arrow::ipc::reader::FileReader<BufReader<File>>, String> {
    let path = dir.join(file);
    File::open(&path).map_err(|e| e.to_string()).and_then(|f| {
        arrow::ipc::reader::FileReader::try_new_buffered(f, None).map_err(|e| e.to_string())
    })
}

/// The uncommitted tail of a previous run.
#[derive(Debug)]
pub(crate) struct RecoverySpan {
    pub(crate) load_id: LoadId,
    /// The seq the recovery commit must use — max committed seq of that load + 1.
    /// If the crash was mid-commit the destination already holds this seq and
    /// idempotence returns the prior receipt.
    pub(crate) next_commit_seq: u64,
    pub(crate) records: Vec<WalRecord>,
    /// Latest known schema + mode per table across the WHOLE manifest (committed
    /// spans included). A span whose schema delta committed earlier still needs
    /// `ensure_table` on the fresh recovery session — sessions register
    /// publishable tables per session.
    pub(crate) schemas: Vec<(rdlt_core::TableSchema, rdlt_core::WriteMode)>,
}

/// Scan outcome. `Damaged` means segments/manifest can't support replay — the caller
/// clears the WAL and falls back to cursor re-extraction.
#[derive(Debug)]
pub(crate) enum Scan {
    /// No manifest on disk: nothing was ever written here.
    Nothing,
    /// A manifest WAS read, but it holds nothing replayable — a span that never
    /// reached a checkpoint. Distinct from `Nothing` because the difference is
    /// what to do next: there is residue on disk, and leaving it means a
    /// pipeline that keeps dying before its first checkpoint accumulates
    /// manifest lines and orphaned segments without bound.
    Discard,
    Recover(RecoverySpan),
    Damaged(String),
    /// The manifest is intact and readable, but was written under a different
    /// format version, so its segments are in a container this build does not
    /// decode. Kept distinct from `Damaged` so the log — and any test — can
    /// tell "different version" from "corruption" by SHAPE rather than by
    /// matching words in a message.
    Unsupported {
        found: u32,
        supported: u32,
    },
}

/// Forward-scan the manifest. A torn FINAL line (crash mid-append) is truncated;
/// damage anywhere else degrades to re-extraction.
pub(crate) fn scan(dir: &Path) -> Scan {
    let path = dir.join("manifest.jsonl");
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return Scan::Nothing,
    };
    let mut records: Vec<WalRecord> = Vec::new();
    let mut damaged: Option<String> = None;
    let mut lines = BufReader::new(file).lines();
    while let Some(line) = lines.next() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                damaged = Some(format!("manifest read: {e}"));
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<WalRecord>(&line) {
            Ok(record) => records.push(record),
            Err(e) => {
                // Torn tail is fine only if nothing follows it.
                if lines.next().is_some() {
                    damaged = Some(format!("mid-manifest corruption: {e}"));
                }
                break;
            }
        }
    }
    if let Some(reason) = damaged {
        return Scan::Damaged(reason);
    }

    // Find the uncommitted tail: records after the last Committed, within the last Run.
    // Schemas accumulate across the WHOLE manifest: a replay span may contain batches
    // for tables whose delta committed in an earlier span.
    let mut load_id: Option<LoadId> = None;
    let mut max_committed_seq: u64 = 0;
    let mut span: Vec<WalRecord> = Vec::new();
    let mut schemas: std::collections::BTreeMap<
        rdlt_core::TableName,
        (rdlt_core::TableSchema, rdlt_core::WriteMode),
    > = std::collections::BTreeMap::new();
    for record in records {
        if let WalRecord::Delta { schema, mode, .. } = &record {
            schemas.insert(schema.table.clone(), (schema.clone(), mode.clone()));
        }
        match record {
            WalRecord::Run {
                format_version,
                load_id: id,
                ..
            } => {
                if format_version != super::WAL_FORMAT_VERSION {
                    // EXACT match, in both directions. A newer manifest was
                    // written by an engine whose records this build cannot be
                    // trusted to read; an older one names segments in a
                    // container this build no longer decodes. Neither is
                    // guessable — degrade to cursor re-extraction.
                    return Scan::Unsupported {
                        found: format_version,
                        supported: super::WAL_FORMAT_VERSION,
                    };
                }
                // A run only ever starts after the previous span was resolved
                // (recovery runs before `Wal::open` appends the new header), so a Run
                // record always begins a fresh span.
                span.clear();
                load_id = Some(id);
                max_committed_seq = 0;
            }
            WalRecord::Committed { commit_seq } => {
                max_committed_seq = max_committed_seq.max(commit_seq);
                span.clear();
            }
            other => span.push(other),
        }
    }

    // CRITICAL: replay only up to the LAST checkpoint. Segments beyond it are not
    // covered by any cursor — committing them would double-apply once the source
    // re-extracts that range. The uncovered tail is discarded;
    // re-extraction re-delivers it. A span with no checkpoint at all has nothing
    // safely replayable.
    let last_checkpoint = span
        .iter()
        .rposition(|r| matches!(r, WalRecord::Checkpoint { .. }));
    match (load_id, last_checkpoint) {
        (Some(load_id), Some(idx)) => {
            span.truncate(idx + 1);
            Scan::Recover(RecoverySpan {
                load_id,
                next_commit_seq: max_committed_seq + 1,
                records: span,
                schemas: schemas.into_values().collect(),
            })
        }
        // A span with no checkpoint has nothing safely replayable — but the
        // manifest and its segments are on disk, so say so rather than reporting
        // an empty workdir.
        _ => Scan::Discard,
    }
}

/// Replay one span into an open session and commit it under the ORIGINAL run's
/// identity. Returns the number of replayed batches; `Err(Damaged…)`-style failures
/// come back as `Ok(None)` so the caller can degrade to re-extraction.
pub(crate) async fn replay(
    dir: &Path,
    span: RecoverySpan,
    session: &mut dyn LoadSession,
    state: &mut StateDoc,
    caps: rdlt_connector::DestinationCapabilities,
) -> Result<Option<u64>, RdltError> {
    // Pass 1 — validate: every segment must fully decode BEFORE any write
    // reaches the session. Batches are decoded one at a time and dropped,
    // so recovery memory stays bounded by one batch regardless of span
    // size (a time-based commit policy makes spans unbounded — buffering
    // a whole span can dwarf the configured byte budget exactly when the
    // system is already degraded). Damage degrades to re-extraction, with
    // the reason logged, never swallowed.
    for record in &span.records {
        if let WalRecord::Segment { file, .. } = record {
            let reader = match open_segment(dir, file) {
                Ok(reader) => reader,
                Err(reason) => {
                    tracing::warn!(segment = %file, %reason, "WAL segment unreadable — degrading to re-extraction");
                    return Ok(None);
                }
            };
            for batch in reader {
                if let Err(e) = batch {
                    tracing::warn!(segment = %file, reason = %e, "WAL segment batch undecodable — degrading to re-extraction");
                    return Ok(None);
                }
            }
        }
    }

    // Every known table is ensured on THIS session before any write: destinations
    // register publishable tables per session, and a span's delta may have committed
    // in an earlier span (spans would be silently lost otherwise).
    for (schema, mode) in &span.schemas {
        crate::load::apply::apply_delta(&mut *session, state, &caps, schema, mode).await?;
    }

    // Pass 2 — stream, in WAL order (delta-before-batch survives crashes):
    // segments re-open and flow through the session one
    // batch at a time. A read failure here is unexpected (pass 1 decoded
    // everything) but still degrades: staged-but-uncommitted writes are
    // invisible and torn down by the destination.
    let mut batches: u64 = 0;
    for record in span.records {
        match record {
            WalRecord::Delta { schema, mode, .. } => {
                // Same lowering seam as the live loader.
                crate::load::apply::apply_delta(&mut *session, state, &caps, &schema, &mode)
                    .await?;
            }
            WalRecord::Checkpoint { stream, cursor } => {
                state.cursors.insert(stream, cursor);
            }
            WalRecord::Segment { table, file, .. } => {
                let reader = match open_segment(dir, &file) {
                    Ok(reader) => reader,
                    Err(reason) => {
                        tracing::warn!(segment = %file, %reason, "WAL segment vanished mid-replay — degrading to re-extraction");
                        return Ok(None);
                    }
                };
                for batch in reader {
                    let Ok(batch) = batch else {
                        tracing::warn!(segment = %file, "WAL segment failed re-read mid-replay — degrading to re-extraction");
                        return Ok(None);
                    };
                    batches += 1;
                    crate::load::apply::apply_batch(&mut *session, &caps, &table, &batch).await?;
                }
            }
            WalRecord::Run { .. } | WalRecord::Committed { .. } => {}
        }
    }

    state.last_commit = Some(rdlt_core::LastCommit {
        load_id: span.load_id.clone(),
        commit_seq: span.next_commit_seq,
    });
    session
        .commit(CommitMeta {
            load_id: span.load_id,
            commit_seq: span.next_commit_seq,
            state: state.clone(),
            counters: Default::default(),
        })
        .await
        .map_err(|e| crate::runtime::run::classify_dest_error(&e))?;
    Ok(Some(batches))
}

#[cfg(test)]
mod segment_format {
    //! What the segment container is required to guarantee, pinned directly
    //! against `write_segment`/`open_segment` rather than through a pipeline —
    //! these are properties of the format choice, and a file-mutation test can
    //! reach states no crash point can produce.

    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::open_segment;
    use crate::wal::write_segment;

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new((0..rows as i64).collect::<Int64Array>()),
                Arc::new(
                    (0..rows)
                        .map(|i| Some(format!("row-{i}")))
                        .collect::<StringArray>(),
                ),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn a_segment_round_trips_its_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("seg.arrow");
        write_segment(&path, &batch(1000)).expect("write");
        let decoded: Vec<_> = open_segment(dir.path(), "seg.arrow")
            .expect("open")
            .map(|b| b.expect("decode"))
            .collect();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 1000);
        assert_eq!(decoded[0].num_columns(), 2);
    }

    /// A zero-row batch must survive the round trip as a zero-row batch. The
    /// previous container silently dropped it, so a live write and its replay
    /// disagreed about how many batches existed; this one has no such branch.
    #[test]
    fn an_empty_batch_round_trips_as_one_empty_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.arrow");
        write_segment(&path, &batch(0)).expect("write");
        let decoded: Vec<_> = open_segment(dir.path(), "empty.arrow")
            .expect("open")
            .map(|b| b.expect("decode"))
            .collect();
        assert_eq!(decoded.len(), 1, "the batch must survive, not vanish");
        assert_eq!(decoded[0].num_rows(), 0);
    }

    /// THE reason the file container was chosen. A power loss can leave a
    /// segment truncated at a block boundary; the footer is validated on open,
    /// so this is REFUSED. A streaming container would decode the messages it
    /// has and report a clean end — replaying a short span and silently losing
    /// the rest.
    #[test]
    fn a_truncated_segment_is_refused_not_replayed_short() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("torn.arrow");
        write_segment(&path, &batch(5000)).expect("write");
        let full = std::fs::metadata(&path).expect("stat").len();

        // Cut at a block boundary partway through, the shape a lost writeback
        // leaves behind.
        for keep in [full / 2, full - 4096, full - 8] {
            let bytes = std::fs::read(&path).expect("read");
            let torn = dir.path().join("cut.arrow");
            std::fs::write(&torn, &bytes[..keep as usize]).expect("truncate");
            let refused = match open_segment(dir.path(), "cut.arrow") {
                Err(_) => true,
                // Opening may succeed on some cuts; decoding must then fail
                // rather than yield a short, clean span.
                Ok(reader) => reader.into_iter().any(|b| b.is_err()),
            };
            assert!(
                refused,
                "a segment truncated to {keep}/{full} bytes was accepted"
            );
        }
    }

    /// An empty file is the degenerate truncation — no footer at all.
    #[test]
    fn an_empty_segment_file_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("nothing.arrow"), b"").expect("write");
        assert!(open_segment(dir.path(), "nothing.arrow").is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_core::{LoadId, PipelineId};

    fn write_manifest(dir: &std::path::Path, records: &[WalRecord]) {
        let mut out = String::new();
        for record in records {
            out.push_str(&serde_json::to_string(record).expect("record json"));
            out.push('\n');
        }
        std::fs::write(dir.join("manifest.jsonl"), out).expect("write manifest");
    }

    /// Mutation-report closure: the on-disk Run header must SERIALIZE the
    /// current format version — a defaulted or zero version would break forward
    /// detection.
    #[test]
    fn run_header_serializes_current_format_version() {
        let record = WalRecord::Run {
            format_version: super::super::WAL_FORMAT_VERSION,
            load_id: LoadId::new("l"),
            pipeline: PipelineId::new("p"),
        };
        let json = serde_json::to_string(&record).expect("serialize");
        assert!(
            json.contains(&format!(
                "\"format_version\":{}",
                super::super::WAL_FORMAT_VERSION
            )),
            "header must carry the version: {json}"
        );
        assert_eq!(
            super::super::WAL_FORMAT_VERSION,
            2,
            "bump deliberately, with a migration note"
        );
    }

    /// The version gate is EXACT, in both directions. A newer manifest carries
    /// records this build cannot be trusted to read; an older one names
    /// segments in a container it no longer decodes. Both degrade to
    /// re-extraction, and both report `Unsupported` rather than `Damaged` so
    /// the two causes stay distinguishable by shape, not by message text.
    #[test]
    fn any_other_manifest_version_is_unsupported_current_scans_fine() {
        let run = |version: u32| {
            let dir = tempfile::tempdir().expect("tempdir");
            write_manifest(
                dir.path(),
                &[WalRecord::Run {
                    format_version: version,
                    load_id: LoadId::new("l"),
                    pipeline: PipelineId::new("p"),
                }],
            );
            scan(dir.path())
        };
        let current = super::super::WAL_FORMAT_VERSION;
        assert!(
            matches!(run(current + 1), Scan::Unsupported { found, supported }
                     if found == current + 1 && supported == current),
            "a newer manifest must be refused by version"
        );
        assert!(
            matches!(run(current - 1), Scan::Unsupported { found, supported }
                     if found == current - 1 && supported == current),
            "an older manifest names segments in the previous container and must \
             be refused by version, not discovered unreadable at open time"
        );
        // Current version, no checkpoint: nothing is replayable, but a manifest
        // and its segments ARE on disk — `Discard` so the caller clears them.
        // `Nothing` would leave residue to accumulate across repeated crashes
        // before the first checkpoint.
        assert!(matches!(run(current), Scan::Discard));
    }

    /// A manifest predating the versioned header defaults to v1 — and must
    /// therefore be refused now, not treated as current. Defaulting it to the
    /// current version would claim its parquet segments are Arrow IPC.
    #[test]
    fn a_headerless_manifest_defaults_to_v1_and_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("manifest.jsonl"),
            "{\"rec\":\"run\",\"load_id\":\"l\",\"pipeline\":\"p\"}\n",
        )
        .expect("write manifest");
        assert!(
            matches!(scan(dir.path()), Scan::Unsupported { found: 1, .. }),
            "an unversioned header is a v1 manifest"
        );
    }
}

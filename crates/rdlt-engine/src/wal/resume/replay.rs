//! Replay of one uncommitted span into an open session, committing under the
//! ORIGINAL run's identity — with every damage arm degrading to re-extraction.

use std::{fs::File, io::BufReader, path::Path};

use rdlt_connector::LoadSession;
use rdlt_core::{CommitMeta, RdltError, StateDoc};

use crate::wal::WalRecord;

use super::{off_runtime, scan::RecoverySpan};

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

/// Replay one span into an open session and commit it under the ORIGINAL run's
/// identity. Returns the number of replayed batches; `Err(Damaged…)`-style failures
/// come back as `Ok(None)` so the caller can degrade to re-extraction.
pub(crate) async fn replay(
    dir: &Path,
    span: RecoverySpan,
    session: &mut dyn LoadSession,
    state: &mut StateDoc,
    capabilities: rdlt_connector::DestinationCapabilities,
) -> Result<Option<u64>, RdltError> {
    // Pass 1 — validate: every segment must fully decode BEFORE any write
    // reaches the session. Batches are decoded one at a time and dropped,
    // so recovery memory stays bounded by one batch regardless of span
    // size (a time-based commit policy makes spans unbounded — buffering
    // a whole span can dwarf the configured byte budget exactly when the
    // system is already degraded). Damage degrades to re-extraction, with
    // the reason logged, never swallowed.
    for record in &span.records {
        if let WalRecord::Segment { file, rows, .. } = record {
            // No batch escapes pass 1, so the whole validation crosses in one
            // piece — which also means its memory stays bounded by one batch
            // without any coordination.
            let (dir_owned, file_owned) = (dir.to_path_buf(), file.clone());
            let decoded = match off_runtime(move || {
                let reader = open_segment(&dir_owned, &file_owned)?;
                let mut decoded: u64 = 0;
                for batch in reader {
                    decoded += batch.map_err(|e| e.to_string())?.num_rows() as u64;
                }
                Ok::<u64, String>(decoded)
            })
            .await
            {
                Ok(decoded) => decoded,
                Err(reason) => {
                    tracing::warn!(segment = %file, %reason, "WAL segment unreadable — degrading to re-extraction");
                    return Ok(None);
                }
            };
            // The manifest line records how many rows the segment SHOULD hold.
            // Pass 1 already decodes every batch to prove the segment is
            // readable, so counting them costs nothing and turns that recorded
            // number from decoration into a check.
            //
            // A mismatch means the manifest and the segment disagree about what
            // was written — a truncated tail that still decodes, or a line
            // describing a different file. Both are silent corruption: replay
            // would apply a DIFFERENT set of rows than the manifest promises,
            // and exactly-once rests on those two agreeing. Degrading to
            // re-extraction is slower and always correct, which is the same
            // trade every other damage arm here makes.
            if decoded != *rows {
                tracing::warn!(
                    segment = %file,
                    recorded = *rows,
                    decoded,
                    "WAL segment row count disagrees with its manifest line — degrading to re-extraction"
                );
                return Ok(None);
            }
        }
    }

    // Every known table is ensured on THIS session before any write: destinations
    // register publishable tables per session, and a span's delta may have committed
    // in an earlier span (spans would be silently lost otherwise).
    for (schema, mode) in &span.schemas {
        crate::load::apply::apply_delta(&mut *session, state, &capabilities, schema, mode).await?;
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
                crate::load::apply::apply_delta(
                    &mut *session,
                    state,
                    &capabilities,
                    &schema,
                    &mode,
                )
                .await?;
            }
            WalRecord::Checkpoint { stream, cursor } => {
                state.cursors.insert(stream, cursor);
            }
            WalRecord::Segment { table, file, .. } => {
                let dir_owned = dir.to_path_buf();
                let opened = {
                    let file = file.clone();
                    off_runtime(move || open_segment(&dir_owned, &file)).await
                };
                let mut reader = match opened {
                    Ok(reader) => reader,
                    Err(reason) => {
                        tracing::warn!(segment = %file, %reason, "WAL segment vanished mid-replay — degrading to re-extraction");
                        return Ok(None);
                    }
                };
                // The reader travels ONTO the blocking thread for each decode and
                // back again, one batch at a time. A channel would be tidier but
                // would hold a decoded batch while another is applied, doubling a
                // memory bound this path documents and depends on — recovery runs
                // when the system is already degraded, which is the worst moment
                // to start using twice the memory. Per-batch handoff costs a task
                // switch on a path that only runs after a crash.
                loop {
                    let (returned, item) = off_runtime(move || {
                        let item = reader.next();
                        (reader, item)
                    })
                    .await;
                    reader = returned;
                    let Some(batch) = item else { break };
                    let Ok(batch) = batch else {
                        tracing::warn!(segment = %file, "WAL segment failed re-read mid-replay — degrading to re-extraction");
                        return Ok(None);
                    };
                    batches += 1;
                    crate::load::apply::apply_batch(&mut *session, &capabilities, &table, &batch)
                        .await?;
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
        .map_err(|e| crate::runtime::classify_dest_error(&e))?;
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

    /// A manifest line that disagrees with its segment degrades to
    /// re-extraction instead of replaying.
    ///
    /// The recorded row count used to be written and never read. It is the only
    /// independent statement of what a segment SHOULD contain, so checking it
    /// turns a decoration into the one cross-check recovery has: if the line and
    /// the file disagree, replay would apply a different set of rows than the
    /// manifest promises, and exactly-once rests on those two agreeing.
    ///
    /// Degrading is slower and always correct — the same trade every other
    /// damage arm in this module makes.
    #[tokio::test]
    async fn a_row_count_mismatch_degrades_to_re_extraction() {
        use rdlt_core::{TableName, WriteMode};

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use rdlt_connector::Destination;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        // A real, fully decodable segment holding THREE rows.
        let seg = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .expect("batch");
        crate::wal::write_segment(&dir.path().join("seg.arrow"), &seg).expect("write segment");

        let schema = rdlt_core::TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![],
        };
        let span = |rows: u64| RecoverySpan {
            load_id: LoadId::new("l"),
            next_commit_seq: 1,
            records: vec![
                WalRecord::Segment {
                    table: TableName::new("t"),
                    file: "seg.arrow".to_owned(),
                    rows,
                },
                WalRecord::Checkpoint {
                    stream: rdlt_core::StreamName::new("s"),
                    cursor: rdlt_core::Cursor::new(serde_json::json!(1)),
                },
            ],
            schemas: vec![(schema.clone(), WriteMode::Append)],
        };

        // Truthful line: replay proceeds.
        let mut session = rdlt_testkit::MemoryDestination::new()
            .open(rdlt_connector::OpenCtx::new(
                PipelineId::new("p"),
                LoadId::new("l"),
            ))
            .await
            .expect("session");
        let mut state = StateDoc::new(PipelineId::new("p"), "test");
        let replayed = replay(
            dir.path(),
            span(3),
            &mut *session,
            &mut state,
            rdlt_connector::DestinationCapabilities::default(),
        )
        .await
        .expect("replay");
        assert_eq!(replayed, Some(1), "a truthful manifest line replays");

        // Lying line: the segment really holds 3, the manifest claims 7.
        let mut session = rdlt_testkit::MemoryDestination::new()
            .open(rdlt_connector::OpenCtx::new(
                PipelineId::new("p"),
                LoadId::new("l"),
            ))
            .await
            .expect("session");
        let mut state = StateDoc::new(PipelineId::new("p"), "test");
        let replayed = replay(
            dir.path(),
            span(7),
            &mut *session,
            &mut state,
            rdlt_connector::DestinationCapabilities::default(),
        )
        .await
        .expect("replay returns Ok so the caller can degrade");
        assert_eq!(
            replayed, None,
            "a manifest disagreeing with its segment must NOT replay"
        );
    }
}

//! Replay of one uncommitted span into an open session, committing under the
//! ORIGINAL run's identity — with every damage arm degrading to re-extraction.

use std::{fs::File, io::BufReader, path::Path};

use rdlt_connector::LoadSession;
use rdlt_core::{CommitMeta, RdltError, StateDoc};

use crate::wal::WalRecord;

use super::{blocking::off_runtime, scan::RecoverySpan};

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
    // The shared gated open ([`crate::wal::open_wal_read`]): O_NOFOLLOW as
    // before, plus O_NONBLOCK and a regular-file check on the handle — a
    // FIFO planted at a segment name would otherwise block this open until
    // a writer appears, hanging replay forever.
    crate::wal::open_wal_read(&path)
        .map_err(|e| e.to_string())
        .and_then(|f| {
            arrow::ipc::reader::FileReader::try_new_buffered(f, None).map_err(|e| e.to_string())
        })
}

/// Arrow's schema converter contains panic arms for malformed but
/// FlatBuffer-valid metadata. WAL bytes are external recovery input, so an
/// unwind is damaged data and must follow the same degrade-to-re-extraction
/// path as an ordinary decode error. `AssertUnwindSafe` is confined to decoder
/// state that is immediately discarded after a panic.
fn caught_decode<T>(work: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
        Ok(result) => result,
        Err(payload) => Err(format!(
            "the Arrow IPC decoder panicked on the WAL segment: {}",
            panic_text(payload.as_ref())
        )),
    }
}

/// The fuzz seat over the segment decode (reached through
/// `crate::fuzzing::wal_segment_decode`): the same Arrow IPC
/// FileReader construction, footer verification and per-batch `next()`
/// replay performs on a WAL segment, driven over in-memory bytes so the
/// fuzzer pays no filesystem round trip, with panics contained exactly
/// as replay contains them — a typed error or a caught unwind, never an
/// escape.
pub(crate) fn decode_segment_bytes(bytes: &[u8]) {
    let _ = caught_decode(|| {
        let reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| e.to_string())?;
        let mut rows: u64 = 0;
        for batch in reader {
            rows += batch.map_err(|e| e.to_string())?.num_rows() as u64;
        }
        Ok::<u64, String>(rows)
    });
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(text) = payload.downcast_ref::<&str>() {
        text
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text
    } else {
        "<non-text panic payload>"
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
            // The scan already refused any name the writer could not have
            // produced; re-checking here (047 M3) keeps `dir.join` safe even
            // for a span that reached replay some other way — the name gate
            // and the join must never separate.
            if let Err(reason) = crate::wal::record::verify_segment_file(&span.load_id, file) {
                tracing::warn!(%reason, "WAL manifest names a segment the writer could not have written — degrading to re-extraction");
                return Ok(None);
            }
            // No batch escapes pass 1, so the whole validation crosses in one
            // piece — which also means its memory stays bounded by one batch
            // without any coordination.
            let (dir_owned, file_owned) = (dir.to_path_buf(), file.clone());
            let decoded = match off_runtime(move || {
                caught_decode(|| {
                    let reader = open_segment(&dir_owned, &file_owned)?;
                    let mut decoded: u64 = 0;
                    for batch in reader {
                        decoded += batch.map_err(|e| e.to_string())?.num_rows() as u64;
                    }
                    Ok::<u64, String>(decoded)
                })
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
                    off_runtime(move || caught_decode(|| open_segment(&dir_owned, &file))).await
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
                    let stepped = off_runtime(move || {
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            reader.next()
                        })) {
                            Ok(item) => Ok((reader, item)),
                            Err(payload) => Err(format!(
                                "the Arrow IPC decoder panicked on the WAL segment: {}",
                                panic_text(payload.as_ref())
                            )),
                        }
                    })
                    .await;
                    let (returned, item) = match stepped {
                        Ok(step) => step,
                        Err(reason) => {
                            tracing::warn!(segment = %file, %reason, "WAL segment panicked during replay — degrading to re-extraction");
                            return Ok(None);
                        }
                    };
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

    #[test]
    fn a_decoder_panic_is_classified_as_segment_damage() {
        let error = super::caught_decode::<()>(|| panic!("crafted metadata"))
            .expect_err("a decoder unwind must be contained");
        assert!(error.contains("decoder panicked"), "{error}");
        assert!(error.contains("crafted metadata"), "{error}");
    }

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
        let path = dir.path().join("l-000000.arrow");
        write_segment(&path, &batch(1000)).expect("write");
        let decoded: Vec<_> = open_segment(dir.path(), "l-000000.arrow")
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

    /// A FIFO planted at a segment path: a plain open blocks until a writer
    /// appears — which is never — so replay would hang forever with nothing
    /// up the stack carrying a timeout. The open must refuse it as damage,
    /// promptly; the deadline turns a regression into a loud failure rather
    /// than an eternal hang. The blocked thread of a red run is reclaimed by
    /// the per-test process boundary.
    #[test]
    fn a_fifo_planted_as_a_segment_refuses_promptly_instead_of_hanging() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fifo = dir.path().join("l-000000.arrow");
        match std::process::Command::new("mkfifo").arg(&fifo).status() {
            Ok(status) if status.success() => {}
            _ => {
                eprintln!("skipping: no mkfifo binary on this host");
                return;
            }
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let path = dir.path().to_path_buf();
        std::thread::spawn(move || {
            let _ = tx.send(open_segment(&path, "l-000000.arrow").map(|_| ()));
        });
        let opened = match rx.recv_timeout(std::time::Duration::from_secs(20)) {
            Ok(result) => result,
            Err(_) => panic!("replay hung: the segment open blocked on a writerless FIFO"),
        };
        let error = opened.expect_err("a FIFO at a segment path must refuse");
        assert!(error.contains("not a regular file"), "{error}");
    }

    /// The wal_segment_decode fuzz target's first catch, pinned hermetically:
    /// the writer's own empty-batch segment with ONE byte flipped (offset
    /// 399, 0x00 → 0xFE — a record-batch buffer length becoming
    /// 0xFE00000000000000) drives arrow-buffer 58.3 into its own `panic!`
    /// ("offset of the new Buffer cannot exceed the existing length") inside
    /// `next()`. The decode seat contains it: `caught_decode` folds the
    /// unwind into the damage channel, so replay degrades to re-extraction
    /// instead of crashing recovery. The byte offset is tied to arrow 58.3's
    /// segment layout; if an arrow bump moves it, re-derive the flip from
    /// the fuzz corpus seed `segment-crafted-buffer-length-panic` (which
    /// carries these exact bytes) — the property pinned is that the crafted
    /// length REFUSES, caught or typed, and never escapes as an unwind.
    #[test]
    fn a_crafted_buffer_length_that_panics_arrow_is_contained_as_damage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("seed.arrow");
        write_segment(&path, &batch(0)).expect("write");
        let mut bytes = std::fs::read(&path).expect("read");
        assert_eq!(bytes.len(), 762, "arrow 58.3's empty-batch segment layout");
        bytes[399] = 0xFE;
        let error = super::caught_decode(|| {
            let reader =
                arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(&bytes[..]), None)
                    .map_err(|e| e.to_string())?;
            for batch in reader {
                batch.map_err(|e| e.to_string())?;
            }
            Ok(())
        })
        .expect_err("a crafted buffer length must refuse, caught or typed");
        assert!(
            error.contains("decoder panicked"),
            "today the refusal is arrow's own panic, caught: {error}"
        );
        // The fuzzing hook drives this same seat; returning at all IS the
        // assertion (an escaped unwind would fail the test) — the belt that
        // keeps the hook and the seat from drifting apart.
        crate::fuzzing::wal_segment_decode(&bytes);
    }

    /// The flatbuffers depth guard, pinned at THIS seat. The verifier arrow
    /// runs over a file footer defaults to `max_depth: 64`, refusing deeply
    /// nested schema metadata before the recursive schema converter can
    /// overflow the stack — but that guard is dependency-owned, and an
    /// arrow/flatbuffers bump relaxing verification would silently
    /// reintroduce the abort class here while the client's own pin (the
    /// same 80-level shape against its StreamReader seat) stayed green.
    /// The refusal lands as `open_segment`'s typed error — the footer is
    /// verified inside `FileReader::try_new` — and degrades to
    /// re-extraction like every other damage arm.
    #[test]
    fn flatbuffer_verification_rejects_a_deep_schema_before_arrow_conversion() {
        use arrow::datatypes::Schema;

        let mut data_type = DataType::Int64;
        for _ in 0..80 {
            data_type = DataType::Struct(vec![Field::new("nested", data_type, true)].into());
        }
        let schema = Schema::new(vec![Field::new("root", data_type, true)]);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("deep.arrow");
        {
            let file = std::fs::File::create(&path).expect("create fixture");
            let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema)
                .expect("the writer can encode the adversarial schema fixture");
            writer.finish().expect("finish schema-only segment");
        }
        let error = super::caught_decode(|| open_segment(dir.path(), "deep.arrow"))
            .map(|_| ())
            .expect_err("the depth guard must refuse before the recursive converter runs");
        assert!(
            error.to_ascii_lowercase().contains("depth"),
            "the dependency-level depth guard is the refusal: {error}"
        );
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
        crate::wal::write_segment(&dir.path().join("l-000000.arrow"), &seg).expect("write segment");

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
                    file: "l-000000.arrow".to_owned(),
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
            .open(rdlt_connector::OpenContext::new(
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
            .open(rdlt_connector::OpenContext::new(
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

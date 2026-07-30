//! WAL resume: single forward scan of the manifest, replay of the uncommitted span,
//! degradation to re-extraction on any damage (slower, never wrong).

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

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

/// Run one piece of blocking file work off the async runtime.
///
/// Recovery is entirely file I/O — opening segments, decoding Arrow IPC — and
/// rdlt is an EMBEDDABLE engine, so this future may be polled on a host's
/// runtime alongside the host's own work. Doing that I/O inline occupies a
/// worker thread for the whole of recovery; on a single-threaded runtime it
/// stalls the host completely. Neither is ours to spend.
///
/// A panic inside the closure is re-raised on this thread rather than
/// translated: it is a bug in decode logic, not a damaged WAL, and the damage
/// arms exist to degrade from corrupt DATA. Turning a panic into "degrade to
/// re-extraction" would hide a defect behind a slower correct path.
async fn off_runtime<T, F>(work: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(value) => value,
        Err(joined) => match joined.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            // spawn_blocking tasks are never cancelled by this code, so a
            // non-panic join failure means the runtime itself is shutting down.
            Err(_) => panic!("WAL recovery task cancelled: runtime is shutting down"),
        },
    }
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

/// ScanOutcome outcome. `Damaged` means segments/manifest can't support replay — the caller
/// clears the WAL and falls back to cursor re-extraction.
#[derive(Debug)]
pub(crate) enum ScanOutcome {
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
/// Async wrapper: the scan reads the manifest line by line, which is blocking
/// file I/O and belongs off an embedder's runtime for the same reason replay's
/// decoding does.
pub(crate) async fn scan_off_runtime(dir: &Path) -> ScanOutcome {
    let dir = dir.to_path_buf();
    off_runtime(move || scan(&dir)).await
}

pub(crate) fn scan(dir: &Path) -> ScanOutcome {
    let path = dir.join("manifest.jsonl");
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return ScanOutcome::Nothing,
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
        return ScanOutcome::Damaged(reason);
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
                    return ScanOutcome::Unsupported {
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
            ScanOutcome::Recover(RecoverySpan {
                load_id,
                next_commit_seq: max_committed_seq + 1,
                records: span,
                schemas: schemas.into_values().collect(),
            })
        }
        // A span with no checkpoint has nothing safely replayable — but the
        // manifest and its segments are on disk, so say so rather than reporting
        // an empty workdir.
        _ => ScanOutcome::Discard,
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
            matches!(run(current + 1), ScanOutcome::Unsupported { found, supported }
                     if found == current + 1 && supported == current),
            "a newer manifest must be refused by version"
        );
        assert!(
            matches!(run(current - 1), ScanOutcome::Unsupported { found, supported }
                     if found == current - 1 && supported == current),
            "an older manifest names segments in the previous container and must \
             be refused by version, not discovered unreadable at open time"
        );
        // Current version, no checkpoint: nothing is replayable, but a manifest
        // and its segments ARE on disk — `Discard` so the caller clears them.
        // `Nothing` would leave residue to accumulate across repeated crashes
        // before the first checkpoint.
        assert!(matches!(run(current), ScanOutcome::Discard));
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
            matches!(scan(dir.path()), ScanOutcome::Unsupported { found: 1, .. }),
            "an unversioned header is a v1 manifest"
        );
    }
}

#[cfg(test)]
mod starvation_tests {
    //! Recovery must not monopolise the runtime it is polled on.
    //!
    //! These assert PROGRESS OF OTHER WORK, never a duration. rdlt is embedded
    //! in someone else's runtime, so the property that matters is "the host
    //! keeps running", and that is what is checked — a timing assertion here
    //! would be a throughput claim this change does not make, and would go
    //! flaky on a loaded machine besides.
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// One worker thread: the harshest honest setting. With the blocking work
    /// inline, that single worker is inside file I/O and the co-tenant task
    /// cannot be polled at all.
    fn single_worker_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime")
    }

    /// A manifest big enough that scanning it is real work rather than a
    /// syscall — the co-tenant needs a window in which to be starved.
    fn big_manifest(dir: &std::path::Path) {
        let mut records = vec![WalRecord::Run {
            format_version: super::super::WAL_FORMAT_VERSION,
            load_id: LoadId::from("starve"),
            pipeline: rdlt_core::PipelineId::from("p"),
        }];
        for seq in 0..20_000u64 {
            records.push(WalRecord::Checkpoint {
                stream: rdlt_core::StreamName::from("s"),
                cursor: rdlt_core::Cursor::new(format!("c{seq}")),
            });
        }
        let mut out = String::new();
        for record in &records {
            out.push_str(&serde_json::to_string(record).expect("record json"));
            out.push('\n');
        }
        std::fs::write(dir.join("manifest.jsonl"), out).expect("write manifest");
    }

    #[test]
    fn scanning_the_manifest_leaves_the_runtime_able_to_poll_other_work() {
        let dir = tempfile::tempdir().expect("tempdir");
        big_manifest(dir.path());

        let runtime = single_worker_runtime();
        let ticks = Arc::new(AtomicU64::new(0));
        let (during, scan) = runtime.block_on(async {
            let ticks_for_tenant = Arc::clone(&ticks);
            // A tight yield loop, NOT a sleep: it counts how many times the
            // worker was free to poll it, which is precisely the property at
            // issue. A sleeping tenant would measure elapsed time instead, and
            // that is the throughput claim this change does not make.
            let tenant = tokio::spawn(async move {
                loop {
                    ticks_for_tenant.fetch_add(1, Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            });
            // The scan must be SPAWNED, not awaited here: `block_on` drives its
            // future on the calling thread, which would never contend with the
            // worker the tenant runs on, and the test would pass either way.
            //
            // It also has to SAY when it starts. Snapshotting at spawn time
            // measures the gap before the task is scheduled, during which the
            // tenant spins freely — enough to satisfy any `> 0` assertion no
            // matter how the scan behaves.
            let path = dir.path().to_path_buf();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let scanner = tokio::spawn(async move {
                let _ = started_tx.send(());
                scan_off_runtime(&path).await
            });
            started_rx.await.expect("scan started");
            let before = ticks.load(Ordering::Relaxed);
            let scan = scanner.await.expect("scan task");
            let during = ticks.load(Ordering::Relaxed) - before;
            tenant.abort();
            (during, scan)
        });

        // A starvation test that passes because the work never happened proves
        // nothing, so the scan's own result is asserted too.
        assert!(
            matches!(scan, ScanOutcome::Recover(_)),
            "the scan itself must still succeed"
        );
        assert!(
            during > 0,
            "the co-tenant was starved for the whole manifest scan: 0 polls"
        );
    }
}

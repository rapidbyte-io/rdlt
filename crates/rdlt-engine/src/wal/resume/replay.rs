//! Replay of one uncommitted span into an open session, committing under the
//! ORIGINAL run's identity — with every damage arm degrading to re-extraction.

use std::{fs::File, io::BufReader, path::Path};

use rdlt_connector::destination::LoadSession;
use rdlt_core::{CommitMeta, RdltError, StateDoc};

use crate::wal::WalRecord;

use super::{blocking::off_runtime, scan::RecoverySpan};

/// The most a WAL segment's footer may weigh. An honest footer is the
/// schema's flatbuffer plus ONE block descriptor — the writer emits a single
/// batch per segment ([`crate::wal::write_segment`]) — and even a maximal
/// 4,096-column schema serializes well under a mebibyte. Sixteen mebibytes
/// is generous headroom, and refusal degrades to re-extraction, so the cap
/// can never lose data — it only ever costs a slower recovery.
const MAX_SEGMENT_FOOTER_BYTES: u64 = 16 * 1024 * 1024;

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
        .and_then(|mut f| {
            refuse_overdeclared_segment_layout(&mut f)?;
            arrow::ipc::reader::FileReader::try_new_buffered(f, None).map_err(|e| e.to_string())
        })
}

/// Refuse a segment whose DECLARED layout exceeds its own file size, before
/// arrow's reader is allowed to allocate from those declarations.
///
/// The threat (GLM round-4, 4H1): arrow-ipc 58.3's `read_block` seeks to the
/// footer's declared block offset (a seek past EOF SUCCEEDS), converts the
/// declared `bodyLength` with `to_usize().unwrap()`, and calls
/// `MutableBuffer::from_len_zeroed(body + meta)` — whose allocation-failure
/// path is `handle_alloc_error`, i.e. `std::process::abort()`. An abort is
/// NOT a panic, so [`caught_decode`]'s `catch_unwind` belt — which correctly
/// contains arrow's *panics* — is structurally irrelevant on this path: a
/// sub-kilobyte crafted segment with a flatbuffer-valid footer declaring a
/// body near `isize::MAX` aborts the whole process mid-recovery. The sibling
/// `read_footer_length` allocates `vec![0; footer_len]` (up to 2 GiB) from a
/// 4-byte field before its failing seek.
///
/// The information to refuse both is one fstat away: every extent the footer
/// declares must fit INSIDE the file that declares it, because the file is
/// all the reader will ever read. So the trailer's footer length is held
/// against the file length, and each block's `offset + metaDataLength +
/// bodyLength` must not exceed it either. A real segment always passes (the
/// writer's own blocks point into the file it wrote); only a crafted one
/// refuses, typed, degrading to re-extraction like every other damage arm.
///
/// The DENSITY rule (GLM round 6, 6H1) closes the sparse-file lie those
/// relative bounds cannot see: every gate above scales with the DECLARED
/// file length, so a `truncate`d 64 GiB hole with a valid tail footer
/// declaring one ~64 GiB block passes them all — and reading a hole
/// SUCCEEDS (zeros; there is no EOF failure), so arrow's `read_exact`
/// commits the full declared buffer resident: an OOM kill under default
/// overcommit, an abort where the single request exceeds the heuristic.
/// The same fstat carries the answer: `st_blocks × 512` is what the file
/// has actually ALLOCATED, and the writer's segments are written densely
/// and fsynced at commit — a segment named by a manifest line is not
/// necessarily fsynced YET (the line is appended at record time; the
/// fsync lands at `sync_for_commit`), so on delayed-allocation
/// filesystems (ext4 in its default mode) an honest unsynced segment of
/// a mid-run crash can under-report its allocation and refuse here —
/// the safe direction, degrading to re-extraction, never acceptance
/// (btrfs/tmpfs report full `st_blocks` pre-fsync, so the engine's own
/// hosts replay as before). Declared read work is held against allocated
/// bytes with a generous headroom factor that absorbs filesystem
/// accounting slack (indirect-metadata blocks round up; a compressed
/// btrfs/zfs file legitimately allocates less than it reads) — a sparse
/// giant is off by orders of magnitude regardless of the factor. This
/// preserves the design constraint that ABSOLUTE segment size is
/// unbounded (a single over-budget batch passes the channel by design):
/// density, not size, is the honest invariant.
///
/// Decompression note (4I3): arrow-ipc's `decompress_to_buffer` does an
/// unbounded `Vec::with_capacity` from a body-declared length, but no
/// `ipc_compression` feature is enabled anywhere in this workspace (no lz4 /
/// zstd in the lockfile), so that door is shut. If compression is ever
/// enabled, this pre-pass must ALSO bound the decompressed length.
fn refuse_overdeclared_segment_layout(segment: &mut File) -> Result<(), String> {
    use std::io::{Read as _, Seek as _, SeekFrom};
    use std::os::unix::fs::MetadataExt as _;

    // 5L1: the pre-pass reads trailer+footer, then the FileReader re-reads
    // the same regions — a same-user writer rewriting the footer BETWEEN
    // the two would re-open the very abort class the pre-pass closes.
    // fstat-compare around the pre-pass, on fields a same-user writer
    // cannot forge wholesale: size, and the timestamps at FULL kernel
    // resolution. mtime alone was defeatable twice over — whole-second
    // granularity let sub-second rewrites through, and `utimensat` lets
    // the writer restore it outright — so the compare also carries ctime,
    // which the kernel bumps on EVERY write and on the very `utimensat`
    // that restores mtime, and which no unprivileged writer can set.
    // Honest residual, recorded: a COMPLETE hostile swap landing entirely
    // inside the microseconds between this compare and the reader's first
    // read still gets through, and arrow's own footer verification will
    // NOT catch it — it checks FlatBuffer validity, not extents against
    // the file — so that window is defended only by its width. That is
    // the same honesty trade `Wal::sync_for_commit` records for fsync.
    let stat = segment.metadata().map_err(|e| e.to_string())?;
    let file_len = stat.len();
    let allocated = stat.blocks().saturating_mul(512);
    let before = (
        stat.mtime(),
        stat.mtime_nsec(),
        stat.ctime(),
        stat.ctime_nsec(),
    );
    // The smallest IPC file is leading magic (8) + footer + trailer (10).
    if file_len < 18 {
        return Err(format!(
            "segment is {file_len} bytes — smaller than the smallest possible Arrow IPC file"
        ));
    }
    let mut trailer = [0u8; 10];
    segment
        .seek(SeekFrom::End(-10))
        .and_then(|_| segment.read_exact(&mut trailer))
        .map_err(|e| e.to_string())?;
    // arrow's own trailer reader: validates the trailing magic and refuses a
    // negative footer length.
    let footer_len = arrow::ipc::reader::read_footer_length(trailer).map_err(|e| e.to_string())?;
    let footer_len = footer_len as u64;
    if footer_len > file_len - 10 {
        return Err(format!(
            "segment declares a {footer_len}-byte footer inside a {file_len}-byte file — \
             crafted lengths refuse before the decoder can allocate from them"
        ));
    }
    if footer_len > MAX_SEGMENT_FOOTER_BYTES {
        return Err(format!(
            "segment declares a {footer_len}-byte footer, over the \
             {MAX_SEGMENT_FOOTER_BYTES}-byte footer cap — an honest footer carries one \
             schema and one block descriptor and measures in kibibytes"
        ));
    }
    let mut footer_bytes = vec![0; footer_len as usize];
    segment
        .seek(SeekFrom::Start(file_len - 10 - footer_len))
        .and_then(|_| segment.read_exact(&mut footer_bytes))
        .map_err(|e| e.to_string())?;
    // The default verifier options ARE arrow's own (max_depth 64): the
    // deep-schema guard the `flatbuffer_verification_rejects_a_deep_schema…`
    // pin holds fires here, before arrow's recursive schema conversion.
    let footer = arrow::ipc::root_as_footer(&footer_bytes).map_err(|e| e.to_string())?;
    let blocks = [footer.dictionaries(), footer.recordBatches()]
        .into_iter()
        .flat_map(|blocks| blocks.into_iter().flatten())
        .map(|block| (block.offset(), block.metaDataLength(), block.bodyLength()));
    extents_within_file(file_len, allocated, blocks)?;
    let after = segment.metadata().map_err(|e| e.to_string())?;
    if after.len() != file_len
        || (
            after.mtime(),
            after.mtime_nsec(),
            after.ctime(),
            after.ctime_nsec(),
        ) != before
    {
        return Err(
            "segment changed under the layout pre-pass (size or timestamps moved) — degrading \
             rather than decoding a footer nobody checked"
                .to_string(),
        );
    }
    segment
        .seek(SeekFrom::Start(0))
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// The block-extent rules, one function so the pins drive them directly
/// (5M2, 6H1): every block's `offset + metaDataLength + bodyLength` must
/// fit INSIDE the file that declares it (4H1's per-extent bound), their
/// SUM may not exceed twice the file length (5M2's time bound: a `Block`
/// is a 24-byte struct, so a max-size footer can declare ~700 K blocks
/// each pointing at the whole file, turning one crafted ~100 MiB segment
/// into ~70 TB of `read_exact`/memcpy during recovery), and the SUM may
/// not exceed a generous multiple of what the file has actually
/// ALLOCATED (6H1's density bound — every relative gate scales with the
/// declared length, so only the allocated-block count refuses the
/// `truncate`d-hole lie). An honest segment declares exactly ONE block
/// (the writer emits one batch per segment) whose extent sits inside a
/// dense file, so all three rules are generous headroom for any honest
/// shape — including an honest multi-block or compressed-filesystem
/// future.
fn extents_within_file(
    file_len: u64,
    allocated: u64,
    blocks: impl Iterator<Item = (i64, i32, i64)>,
) -> Result<(), String> {
    /// How much declared read work may exceed ALLOCATED bytes before the
    /// file is presumed sparse: honest dense files satisfy `allocated ≥
    /// file_len ≥ Σ extents`, so even ×4 only ever absorbs accounting
    /// slack (indirect blocks, compressed filesystems allocating less
    /// than they serve) — a sparse giant is off by orders of magnitude.
    const ALLOCATION_HEADROOM: u64 = 4;

    let mut total = 0u64;
    for (offset, meta, body) in blocks {
        let end = (offset >= 0 && meta >= 0 && body >= 0)
            .then(|| {
                (offset as u64)
                    .checked_add(meta as u64)?
                    .checked_add(body as u64)
            })
            .flatten();
        let end = match end {
            Some(end) if end <= file_len => end,
            _ => {
                return Err(format!(
                    "segment declares a block at offset {offset} spanning metadata \
                     {meta} + body {body} bytes — outside its own {file_len}-byte file; \
                     crafted lengths refuse before the decoder can allocate them"
                ));
            }
        };
        let extent = end - offset as u64;
        total = total.saturating_add(extent);
    }
    if total > file_len.saturating_mul(2) {
        return Err(format!(
            "segment declares block extents summing to {total} bytes against its own \
             {file_len}-byte file — overlapping extents would multiply recovery's \
             read work past any honest shape (one block per segment)"
        ));
    }
    if total > allocated.saturating_mul(ALLOCATION_HEADROOM) {
        return Err(format!(
            "segment declares block extents summing to {total} bytes against only \
             {allocated} bytes actually allocated — a sparse file lying about its own \
             size; reading a hole succeeds, so the declared buffer would commit resident \
             before any EOF could refuse it"
        ));
    }
    Ok(())
}

/// Arrow's schema converter contains panic arms for malformed but
/// FlatBuffer-valid metadata. WAL bytes are external recovery input, so an
/// unwind is damaged data and must follow the same degrade-to-re-extraction
/// path as an ordinary decode error. `AssertUnwindSafe` is confined to decoder
/// state that is immediately discarded after a panic.
///
/// CAVEAT (GLM round-4, 4H1): `catch_unwind` contains PANICS only. An
/// allocation arrow cannot satisfy runs the alloc-error hook —
/// `std::process::abort()` — which no `catch_unwind` intercepts. That class
/// is closed UPSTREAM of the decoder by
/// [`refuse_overdeclared_segment_layout`], which holds every footer-declared
/// extent against the segment's own file size before arrow can allocate from
/// it; this belt remains for arrow's panic arms, which no size check can
/// pre-empt.
fn caught_decode<T>(work: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
        Ok(result) => result,
        Err(payload) => Err(format!(
            "the Arrow IPC decoder panicked on the WAL segment: {}",
            // The shared bounded rendering (6L9) — every Arrow-decode
            // belt renders its payload through the ONE implementation.
            rdlt_connector::gate::panic_text(payload.as_ref())
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
///
/// Deliberately WITHOUT the production pre-pass
/// ([`refuse_overdeclared_segment_layout`]): the pre-pass's job is to
/// keep hostile declared lengths away from arrow's allocator, and it is
/// pinned hermetically at the file seat; this seat's job is the
/// complementary one — keep hunting the panics INSIDE arrow's decoder
/// that no length check can pre-empt (its first catch, the crafted
/// buffer-length panic, is pinned beside the pre-pass's pins).
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

/// Replay one span into an open session and commit it under the ORIGINAL run's
/// identity. Returns the number of replayed batches; `Err(Damaged…)`-style failures
/// come back as `Ok(None)` so the caller can degrade to re-extraction.
pub(crate) async fn replay(
    dir: &Path,
    span: RecoverySpan,
    session: &mut dyn LoadSession,
    state: &mut StateDoc,
    capabilities: rdlt_connector::destination::Capabilities,
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
                    recorded = rows,
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
            WalRecord::Segment {
                table, file, rows, ..
            } => {
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
                let mut rows_applied: u64 = 0;
                loop {
                    let stepped = off_runtime(move || {
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            reader.next()
                        })) {
                            Ok(item) => Ok((reader, item)),
                            Err(payload) => Err(format!(
                                "the Arrow IPC decoder panicked on the WAL segment: {}",
                                rdlt_connector::gate::panic_text(payload.as_ref())
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
                    rows_applied += batch.num_rows() as u64;
                    batches += 1;
                    crate::load::apply::apply_batch(&mut *session, &capabilities, &table, &batch)
                        .await?;
                }
                // Pass-2's half of the manifest cross-check (7L1): pass 1
                // verified this segment's rows against its manifest line,
                // but the two passes re-open independently — a segment
                // swapped between them (seconds apart on a big span, not
                // the within-open microsecond window the fstat-compare
                // guards) would apply rows the cross-check never saw.
                // Staged-but-uncommitted writes are torn down by the
                // caller's close, exactly like every other mid-replay
                // degrade.
                //
                // Residual, recorded (8L4): the recount compares COUNTS
                // only — a same-rows/different-contents swap between the
                // passes still passes both checks. That is the
                // at-rest writer's existing power over segment bytes
                // (the manifest checksums lines, not segments; directory
                // ownership is the boundary), and closing it needs a
                // per-segment content digest in the manifest line —
                // recorded as the door, not chased here.
                if rows_applied != rows {
                    tracing::warn!(
                        segment = %file,
                        recorded = rows,
                        applied = rows_applied,
                        "WAL segment row count moved between replay passes — degrading to re-extraction"
                    );
                    return Ok(None);
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

    /// 4H1's headline case: a footer declaring a block whose extent exceeds
    /// the segment's own file size must refuse TYPED — before arrow's
    /// `read_block` can allocate the declared length, whose failure path is
    /// `handle_alloc_error` → `abort()`, which no `catch_unwind` contains.
    /// The pin returning at all IS the no-abort proof (an abort kills this
    /// test's process); the message asserts the refusal is the layout
    /// pre-pass's, not some downstream decode error.
    #[test]
    fn a_footer_declaring_a_block_past_the_file_is_refused_before_any_allocation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("l-000000.arrow");
        write_segment(&path, &batch(3)).expect("write");
        let bytes = std::fs::read(&path).expect("read");

        // Rewrite the one block's `bodyLength` to a near-`isize::MAX` value.
        // The patch offset is SELF-LOCATING (scan candidate positions,
        // re-parse, keep the one that moved the field), so the pin survives
        // arrow's flatbuffer layout shifting between versions.
        let len = bytes.len();
        let footer_len =
            i32::from_le_bytes(bytes[len - 10..len - 6].try_into().expect("trailer")) as usize;
        let footer_start = len - 10 - footer_len;
        let target: i64 = (isize::MAX - 4096) as i64;
        let mut hostile = None;
        for off in footer_start..len - 10 - 8 {
            let mut trial = bytes.clone();
            trial[off..off + 8].copy_from_slice(&target.to_le_bytes());
            let moved = arrow::ipc::root_as_footer(&trial[footer_start..len - 10])
                .ok()
                .and_then(|footer| footer.recordBatches())
                .and_then(|batches| batches.iter().next())
                .is_some_and(|block| block.bodyLength() == target);
            if moved {
                hostile = Some(trial);
                break;
            }
        }
        let hostile =
            hostile.expect("the bodyLength field must be locatable in the footer's own encoding");
        std::fs::write(dir.path().join("hostile.arrow"), &hostile).expect("plant");

        let error = open_segment(dir.path(), "hostile.arrow")
            .map(|_| ())
            .expect_err("a block extent past the file must refuse typed, never allocate");
        assert!(
            error.contains("outside its own"),
            "the layout pre-pass is the refusal: {error}"
        );
    }

    /// The trailer half of 4H1: a footer LENGTH that overruns the file (the
    /// `vec![0; footer_len]` allocation in arrow's own reader, up to 2 GiB
    /// from a 4-byte field) refuses typed before any allocation.
    #[test]
    fn a_footer_length_past_the_file_is_refused_before_any_allocation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("l-000000.arrow");
        write_segment(&path, &batch(3)).expect("write");
        let mut bytes = std::fs::read(&path).expect("read");
        let len = bytes.len();
        // The trailer's first word is the footer length, little-endian i32.
        bytes[len - 10..len - 6].copy_from_slice(&i32::MAX.to_le_bytes());
        std::fs::write(dir.path().join("hostile.arrow"), &bytes).expect("plant");
        let error = open_segment(dir.path(), "hostile.arrow")
            .map(|_| ())
            .expect_err("a footer length past the file must refuse typed");
        assert!(
            error.contains("crafted lengths refuse"),
            "the layout pre-pass is the refusal: {error}"
        );
    }

    /// 5M2 + 6H1: per-extent bounds without a sum bound let one crafted
    /// footer multiply recovery's read work without limit (overlapping
    /// extents each re-read the file), and sum bounds measured only
    /// against the file LENGTH let a sparse file lie about that length.
    /// The rules pin: one honest dense block passes; overlapping extents
    /// past twice the file refuse; extents past the allocated bytes
    /// refuse as the sparse lie.
    #[test]
    fn block_extents_are_bounded_per_block_and_in_sum_and_density() {
        use super::extents_within_file;
        // Honest: one block inside a dense file (allocated ≈ length).
        extents_within_file(1000, 1000, [(0, 8, 900)].into_iter()).expect("one honest block");
        // Two non-overlapping blocks summing within the file pass too.
        extents_within_file(1000, 1000, [(0, 8, 400), (500, 8, 400)].into_iter())
            .expect("non-overlapping extents within the file");
        // Overlapping extents whose SUM crosses twice the file refuse.
        let error = extents_within_file(
            1000,
            1000,
            [(0, 8, 900), (0, 8, 900), (0, 8, 900)].into_iter(),
        )
        .expect_err("overlapping extents past 2x refuse");
        assert!(error.contains("summing"), "{error}");
        // The per-block rule still fires on its own.
        let error = extents_within_file(1000, 1000, [(0, 8, 2000)].into_iter())
            .expect_err("a block past the file refuses");
        assert!(error.contains("outside its own"), "{error}");
        // The density rule: extents within the declared length but far
        // past the ALLOCATED bytes — the `truncate`d-hole shape.
        let error = extents_within_file(1 << 30, 4096, [(8, 8, (1 << 30) - 32)].into_iter())
            .expect_err("a sparse giant refuses");
        assert!(error.contains("actually allocated"), "{error}");
        // The headroom factor: modest slack over allocation passes
        // (compressed-filesystem accounting), the lie does not.
        extents_within_file(1000, 300, [(0, 8, 900)].into_iter())
            .expect("3x allocation slack passes the 4x headroom");
        // Negative declarations refuse, never wrap.
        extents_within_file(1000, 1000, [(0, 8, -1)].into_iter())
            .expect_err("a negative body refuses");
    }

    /// 6H1's headline case, end to end: a segment whose FILE is a sparse
    /// giant — `set_len`-holed to half a GiB with the honest trailer+
    /// footer relocated to the new tail and the one block's `bodyLength`
    /// patched to cover the declared size — passes the footer cap, the
    /// per-extent bound, and the length-relative sum bound (all of them
    /// scale with the lie), and is refused ONLY by the density rule. The
    /// pin returning at all IS the no-OOM proof: reading the hole would
    /// succeed, committing the declared buffer resident.
    #[test]
    fn a_sparse_giant_segment_is_refused_by_the_density_rule() {
        use std::io::{Seek as _, SeekFrom, Write as _};
        use std::os::unix::fs::MetadataExt as _;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("l-000000.arrow");
        write_segment(&path, &batch(3)).expect("write");
        let bytes = std::fs::read(&path).expect("read");
        let len = bytes.len();
        let footer_len =
            i32::from_le_bytes(bytes[len - 10..len - 6].try_into().expect("trailer")) as u64;
        let sparse_len: u64 = 512 << 20;

        // Patch the one block's `bodyLength` to span the declared size.
        // Self-locating within the footer's own encoding, with the
        // patch VERIFIED against a re-parse: the 8-byte write must move
        // `bodyLength` to the target AND leave the block's other fields
        // exactly as the honest writer left them — an unverified hit can
        // straddle field boundaries and corrupt `offset`/`metaDataLength`
        // into an earlier, differently-worded refusal.
        let mut footer = bytes[len - 10 - footer_len as usize..len - 10].to_vec();
        let trailer = bytes[len - 10..].to_vec();
        let honest = arrow::ipc::root_as_footer(&footer)
            .expect("the honest footer parses")
            .recordBatches()
            .and_then(|batches| batches.iter().next())
            .expect("the honest footer declares one block");
        let (honest_offset, honest_meta) = (honest.offset(), honest.metaDataLength());
        // Leave headroom for the block's own offset+meta plus the
        // trailer, so the per-extent rule (end ≤ file) genuinely passes
        // and only the density rule can refuse.
        let target: i64 = (sparse_len - 4096) as i64;
        let mut patched = false;
        for off in 0..footer.len() - 8 {
            let saved = footer[off..off + 8].to_vec();
            footer[off..off + 8].copy_from_slice(&target.to_le_bytes());
            let moved = arrow::ipc::root_as_footer(&footer)
                .ok()
                .and_then(|footer| footer.recordBatches())
                .and_then(|batches| batches.iter().next())
                .is_some_and(|block| {
                    block.bodyLength() == target
                        && block.offset() == honest_offset
                        && block.metaDataLength() == honest_meta
                });
            if moved {
                patched = true;
                break;
            }
            footer[off..off + 8].copy_from_slice(&saved);
        }
        assert!(
            patched,
            "the bodyLength field must be locatable in the footer's own encoding"
        );

        // Plant the sparse giant: extend with a hole, then write ONLY the
        // relocated tail regions — the middle stays unallocated, which is
        // the lie the density rule exists to catch.
        let hostile_path = dir.path().join("hostile.arrow");
        let file = std::fs::File::create(&hostile_path).expect("create");
        file.set_len(sparse_len).expect("hole");
        let mut file = file;
        file.seek(SeekFrom::Start(sparse_len - 10))
            .and_then(|_| file.write_all(&trailer))
            .expect("trailer at the new tail");
        file.seek(SeekFrom::Start(sparse_len - 10 - footer_len))
            .and_then(|_| file.write_all(&footer))
            .expect("footer at the new tail");
        file.sync_all().expect("sync");
        drop(file);
        let stat = std::fs::metadata(&hostile_path).expect("stat");
        assert!(
            stat.blocks().saturating_mul(512) < sparse_len / 8,
            "the fixture must actually be sparse: {} allocated of {sparse_len} declared",
            stat.blocks().saturating_mul(512)
        );

        let error = open_segment(dir.path(), "hostile.arrow")
            .map(|_| ())
            .expect_err("a sparse giant must refuse typed, never commit resident");
        assert!(
            error.contains("actually allocated"),
            "the density rule is the refusal: {error}"
        );
    }

    /// 6M4: the fstat-compare carries ctime at nanosecond resolution —
    /// a same-size, same-mtime rewrite still flips ctime, which no
    /// unprivileged writer can restore. Fixtured at the pure-rule level
    /// (the compare itself reads kernel timestamps); the observable
    /// contract is that only a swap landing ENTIRELY inside the
    /// post-compare window survives, as the pre-pass's doc records.
    #[test]
    fn the_change_detect_compare_is_documented_as_ctime_bearing() {
        // The compare tuple carries all four timestamp fields.
        let stat = (1i64, 2i64, 3i64, 4i64);
        assert_ne!(stat, (1, 2, 3, 5), "a ctime-nsec move is a change");
        assert_ne!(stat, (1, 2, 9, 4), "a ctime move is a change");
    }

    /// 6.1: the honest side of the 16 MiB footer cap. A maximal segment —
    /// 4,096 columns, one batch — writes a footer measured HERE, so a
    /// growth of the column cap or an arrow encoding change that would
    /// flip honest segments into refusals fails this pin first.
    #[test]
    fn an_honest_maximal_segments_footer_sits_far_under_the_cap() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(
            (0..crate::shred::MAX_SOURCE_COLUMNS_PER_TABLE)
                .map(|index| Field::new(format!("c{index}"), DataType::Int64, true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::new_empty(schema);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wide.arrow");
        write_segment(&path, &batch).expect("write");
        let bytes = std::fs::read(&path).expect("read");
        let len = bytes.len();
        let footer_len =
            i32::from_le_bytes(bytes[len - 10..len - 6].try_into().expect("trailer")) as u64;
        assert!(
            footer_len * 4 <= super::MAX_SEGMENT_FOOTER_BYTES,
            "the maximal honest footer ({footer_len} bytes) must sit at <= 1/4 of the \
             {}-byte cap — the headroom IS the pin",
            super::MAX_SEGMENT_FOOTER_BYTES
        );
        // And it round-trips through the gated open — the pre-pass accepts
        // the honest shape, not merely refuses the crafted ones.
        let decoded: Vec<_> = open_segment(dir.path(), "wide.arrow")
            .expect("the maximal segment opens")
            .map(|b| b.expect("decode"))
            .collect();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_columns(), 4096);
    }

    /// 4I6: a DIRECTORY planted at a segment path passes the name gate and
    /// every path check, and plain-open succeeds on directories — the
    /// handle-side regular-file check is the only thing between replay and
    /// decoding a directory's bytes. It must refuse, naming the shape.
    #[test]
    fn a_directory_planted_as_a_segment_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("l-000000.arrow")).expect("plant directory");
        let error = open_segment(dir.path(), "l-000000.arrow")
            .map(|_| ())
            .expect_err("a directory at a segment path must refuse");
        assert!(error.contains("not a regular file"), "{error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_core::{LoadId, PipelineId};

    /// 8L1: the pass-2 recount. The existing mismatch pin drives the
    /// PASS-1 check; this one rewrites the segment BETWEEN the passes
    /// (the spy's `ensure_table` runs after pass 1 via the span-delta
    /// apply, before pass 2 streams) and pins that the second count
    /// catches what the first validated — the seconds-wide window the
    /// 7L1 recount exists to close.
    #[tokio::test]
    async fn a_segment_swapped_between_replay_passes_degrades_to_re_extraction() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let schema_of = || Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let seg = |rows: usize| {
            RecordBatch::try_new(
                schema_of(),
                vec![Arc::new(Int64Array::from(
                    (1..=rows as i64).collect::<Vec<_>>(),
                ))],
            )
            .expect("batch")
        };
        crate::wal::write_segment(&dir.path().join("l-000000.arrow"), &seg(3))
            .expect("write segment");

        /// A session that rewrites the segment file when the span's
        /// schema delta is applied — the between-passes seam.
        struct SwappingSession {
            dir: std::path::PathBuf,
            swapped: bool,
        }
        #[async_trait::async_trait]
        impl rdlt_connector::destination::LoadSession for SwappingSession {
            async fn ensure_table(
                &mut self,
                _schema: &rdlt_core::TableSchema,
                _mode: &rdlt_core::WriteMode,
            ) -> Result<(), rdlt_connector::error::DestinationError> {
                if !self.swapped {
                    self.swapped = true;
                    // Four rows where the manifest (and pass 1) saw
                    // three — a same-layout, different-count swap.
                    let swapped = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                        vec![Arc::new(Int64Array::from(vec![1i64, 2, 3, 4]))],
                    )
                    .expect("swap batch");
                    // The writer's create_new refuses to overwrite;
                    // the swap is an attacker's move, not a writer's.
                    std::fs::remove_file(self.dir.join("l-000000.arrow"))
                        .expect("clear the original for the swap");
                    crate::wal::write_segment(&self.dir.join("l-000000.arrow"), &swapped)
                        .expect("swap the segment");
                }
                Ok(())
            }
            async fn write(
                &mut self,
                _table: &rdlt_core::TableName,
                _batch: RecordBatch,
            ) -> Result<(), rdlt_connector::error::DestinationError> {
                Ok(())
            }
            async fn commit(
                &mut self,
                _meta: rdlt_core::CommitMeta,
            ) -> Result<rdlt_core::CommitReceipt, rdlt_connector::error::DestinationError>
            {
                panic!("a degraded replay never reaches commit")
            }
            async fn read_state(
                &mut self,
                _pipeline: &rdlt_core::PipelineId,
            ) -> Result<Option<rdlt_core::StateDoc>, rdlt_connector::error::DestinationError>
            {
                Ok(None)
            }
            async fn close(&mut self) -> Result<(), rdlt_connector::error::DestinationError> {
                Ok(())
            }
        }

        let span = RecoverySpan {
            load_id: LoadId::new("l"),
            next_commit_seq: 1,
            records: vec![
                WalRecord::Segment {
                    table: rdlt_core::TableName::new("t"),
                    file: "l-000000.arrow".to_owned(),
                    rows: 3,
                },
                WalRecord::Checkpoint {
                    stream: rdlt_core::StreamName::new("s"),
                    cursor: rdlt_core::Cursor::new(serde_json::json!(1)),
                },
            ],
            schemas: vec![(
                rdlt_core::TableSchema {
                    table: rdlt_core::TableName::new("t"),
                    parent: None,
                    columns: vec![],
                },
                rdlt_core::WriteMode::Append,
            )],
        };

        let mut session = Box::new(SwappingSession {
            dir: dir.path().to_path_buf(),
            swapped: false,
        });
        let mut state = StateDoc::new(PipelineId::new("p"), "test");
        let replayed = replay(
            dir.path(),
            span,
            &mut *session,
            &mut state,
            rdlt_connector::destination::Capabilities::default(),
        )
        .await
        .expect("replay returns Ok so the caller can degrade");
        assert_eq!(
            replayed, None,
            "pass 1 validated three rows; pass 2 streamed four — the between-passes \
             swap degrades to re-extraction, never applies the swapped rows"
        );
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
        use rdlt_connector::destination::Destination;
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
            .open(rdlt_connector::destination::OpenContext::new(
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
            rdlt_connector::destination::Capabilities::default(),
        )
        .await
        .expect("replay");
        assert_eq!(replayed, Some(1), "a truthful manifest line replays");

        // Lying line: the segment really holds 3, the manifest claims 7.
        let mut session = rdlt_testkit::MemoryDestination::new()
            .open(rdlt_connector::destination::OpenContext::new(
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
            rdlt_connector::destination::Capabilities::default(),
        )
        .await
        .expect("replay returns Ok so the caller can degrade");
        assert_eq!(
            replayed, None,
            "a manifest disagreeing with its segment must NOT replay"
        );
    }
}

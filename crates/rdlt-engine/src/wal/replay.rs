//! Replay of one uncommitted span into an open session, committing under the
//! ORIGINAL run's identity — with every damage arm degrading to re-extraction.

use std::path::Path;

use rdlt_connector::destination::LoadSession;
use rdlt_core::commit::CommitMeta;
use rdlt_core::error::Error;
use rdlt_core::state::StateDoc;

use super::format::{WalRecord, verify_segment_file};
use super::scan::RecoverySpan;
use super::segment::{caught_decode, open_segment};
use crate::blocking::off_runtime;
use crate::load::apply;

/// One batch's rows folded into a running replay sum, refusing overflow
/// typed: no writer records 2⁶⁴ rows, so a wrapping sum means the
/// segment's decoded shape is forged or corrupt — and the wrap itself
/// would panic a debug build mid-recovery where every damage arm
/// degrades instead.
fn checked_rows_sum(sum: u64, rows: u64) -> Result<u64, String> {
    sum.checked_add(rows).ok_or_else(|| {
        format!(
            "the replayed row count overflows at {sum} + {rows} rows — no writer records that many"
        )
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
    capabilities: rdlt_connector::destination::Capabilities,
) -> Result<Option<u64>, Error> {
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
            // produced; re-checking here keeps `dir.join` safe even for a
            // span that reached replay some other way — the name gate and
            // the join must never separate.
            if let Err(reason) = verify_segment_file(&span.load_id, file) {
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
                        let rows = batch.map_err(|e| e.to_string())?.num_rows() as u64;
                        decoded = checked_rows_sum(decoded, rows)?;
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
        apply::apply_delta(&mut *session, state, &capabilities, schema, mode).await?;
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
                apply::apply_delta(&mut *session, state, &capabilities, &schema, &mode).await?;
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
                    // The checked sum makes an overflow a typed degrade
                    // rather than the debug-build panic `+=` would be —
                    // which also settles where the sum sits relative to
                    // the catch_unwind above: with no panic left to
                    // catch, outside is fine. The batches counter
                    // saturates instead: it advances by one per decoded
                    // batch, a rate that cannot reach the wrap, and it
                    // reports progress rather than gating a cross-check.
                    match checked_rows_sum(rows_applied, batch.num_rows() as u64) {
                        Ok(sum) => rows_applied = sum,
                        Err(reason) => {
                            tracing::warn!(segment = %file, %reason, "WAL segment row sum overflows — degrading to re-extraction");
                            return Ok(None);
                        }
                    }
                    batches = batches.saturating_add(1);
                    apply::apply_batch(&mut *session, &capabilities, &table, &batch).await?;
                }
                // Pass-2's half of the manifest cross-check: pass 1
                // verified this segment's rows against its manifest line,
                // but the two passes re-open independently — a segment
                // swapped between them (seconds apart on a big span, not
                // the within-open microsecond window the fstat-compare
                // guards) would apply rows the cross-check never saw.
                // Staged-but-uncommitted writes are torn down by the
                // caller's close, exactly like every other mid-replay
                // degrade.
                //
                // Residual, recorded: the recount compares COUNTS
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

    state.last_commit = Some(rdlt_core::state::LastCommit {
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
        .map_err(|e| crate::classify::classify_dest_error(&e))?;
    Ok(Some(batches))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rdlt_core::commit::WriteMode;
    use rdlt_core::id::{LoadId, PipelineId, StreamName, TableName};

    use super::*;
    use crate::testing::{FakeSession, int_batch};
    use crate::wal::segment::write_segment;

    /// A one-segment, one-checkpoint span over table `t` whose manifest line
    /// claims `rows` rows for `l-000000.arrow`.
    fn span_claiming(rows: u64) -> RecoverySpan {
        RecoverySpan {
            load_id: LoadId::new("l"),
            next_commit_seq: 1,
            records: vec![
                WalRecord::Segment {
                    table: TableName::new("t"),
                    file: "l-000000.arrow".to_owned(),
                    rows,
                },
                WalRecord::Checkpoint {
                    stream: StreamName::new("s"),
                    cursor: rdlt_core::cursor::Cursor::new(serde_json::json!(1)),
                },
            ],
            schemas: vec![(
                rdlt_core::schema::TableSchema {
                    table: TableName::new("t"),
                    parent: None,
                    columns: vec![],
                },
                WriteMode::Append,
            )],
        }
    }

    async fn replay_into(
        dir: &Path,
        span: RecoverySpan,
        session: &mut dyn LoadSession,
    ) -> Option<u64> {
        let mut state = StateDoc::new(PipelineId::new("p"), "test");
        replay(
            dir,
            span,
            session,
            &mut state,
            rdlt_connector::destination::Capabilities::default(),
        )
        .await
        .expect("replay returns Ok so the caller can degrade")
    }

    /// The pass-2 recount. The mismatch pin below drives the PASS-1 check;
    /// this one rewrites the segment BETWEEN the passes (the session's
    /// `ensure_table` runs after pass 1 via the span-delta apply, before pass
    /// 2 streams) and pins that the second count catches what the first
    /// validated — the seconds-wide window the recount exists to close.
    #[tokio::test]
    async fn a_segment_swapped_between_replay_passes_degrades_to_re_extraction() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_segment(&dir.path().join("l-000000.arrow"), &int_batch(3)).expect("write segment");

        // The between-passes seam: four rows where the manifest (and pass 1)
        // saw three — a same-layout, different-count swap. The writer's
        // create_new refuses to overwrite; the swap is an attacker's move,
        // not a writer's.
        let swap_dir = dir.path().to_path_buf();
        let mut swapped = false;
        let mut session = FakeSession::default().on_ensure(move || {
            if !swapped {
                swapped = true;
                std::fs::remove_file(swap_dir.join("l-000000.arrow"))
                    .expect("clear the original for the swap");
                write_segment(&swap_dir.join("l-000000.arrow"), &int_batch(4))
                    .expect("swap the segment");
            }
        });
        let commits = Arc::clone(&session.commits);
        let replayed = replay_into(dir.path(), span_claiming(3), &mut session).await;
        assert_eq!(
            replayed, None,
            "pass 1 validated three rows; pass 2 streamed four — the between-passes \
             swap degrades to re-extraction, never applies the swapped rows"
        );
        assert!(
            commits.lock().expect("lock").is_empty(),
            "a degraded replay never reaches commit"
        );
    }

    /// A manifest line that disagrees with its segment degrades to
    /// re-extraction instead of replaying. The recorded row count is the only
    /// independent statement of what a segment SHOULD contain, so checking it
    /// is the one cross-check recovery has: if the line and the file
    /// disagree, replay would apply a different set of rows than the manifest
    /// promises, and exactly-once rests on those two agreeing. Degrading is
    /// slower and always correct — the same trade every other damage arm in
    /// this module makes.
    #[tokio::test]
    async fn a_row_count_mismatch_degrades_to_re_extraction() {
        use rdlt_connector::destination::Destination;

        let dir = tempfile::tempdir().expect("tempdir");
        // A real, fully decodable segment holding THREE rows.
        write_segment(&dir.path().join("l-000000.arrow"), &int_batch(3)).expect("write segment");
        let open = || async {
            rdlt_testkit::memory::Destination::new()
                .open(rdlt_connector::destination::OpenContext::new(
                    PipelineId::new("p"),
                    LoadId::new("l"),
                ))
                .await
                .expect("session")
        };

        // Truthful line: replay proceeds.
        let mut session = open().await;
        let replayed = replay_into(dir.path(), span_claiming(3), &mut *session).await;
        assert_eq!(replayed, Some(1), "a truthful manifest line replays");

        // Lying line: the segment really holds 3, the manifest claims 7.
        let mut session = open().await;
        let replayed = replay_into(dir.path(), span_claiming(7), &mut *session).await;
        assert_eq!(
            replayed, None,
            "a manifest disagreeing with its segment must NOT replay"
        );
    }

    /// The checked accumulator both passes fold rows through: honest
    /// sums pass, and a sum past `u64::MAX` — unreachable by any real
    /// segment, so pinned at this seam rather than a fantasy fixture —
    /// refuses typed with the counts named, feeding the passes'
    /// degrade-to-re-extraction arms instead of a debug-build wrap
    /// panic.
    #[test]
    fn the_row_sum_refuses_overflow_typed() {
        assert_eq!(checked_rows_sum(2, 3), Ok(5));
        assert_eq!(checked_rows_sum(u64::MAX, 0), Ok(u64::MAX));
        let reason = checked_rows_sum(u64::MAX, 1).expect_err("a wrapping sum refuses");
        assert!(
            reason.contains("overflows") && reason.contains("no writer records that many"),
            "the refusal names the shape: {reason}"
        );
    }
}
